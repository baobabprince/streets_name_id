import sys
import unittest
from types import ModuleType
from unittest.mock import Mock, patch

from laya_ai_resolver import LayaAIResolver


class TestLayaAIResolver(unittest.TestCase):
    def make_resolver(self, prediction=None, side_effect=None, chunk_size=15):
        agent = Mock()
        if side_effect is not None:
            agent.predict.side_effect = side_effect
        else:
            agent.predict.return_value = prediction
        laya_module = ModuleType("laya")
        laya_module.Agent = Mock(return_value=agent)
        with patch.dict(sys.modules, {"laya": laya_module}):
            resolver = LayaAIResolver(chunk_size=chunk_size)
        return resolver, agent, laya_module.Agent

    def test_resolve_city_matches_streets_against_city_lamas_list(self):
        resolver, agent, agent_factory = self.make_resolver(
            {
                "answers": {
                    "street_match": {
                        "choice": "street_2",
                        "probabilities": {"street_2": 0.88},
                    }
                }
            }
        )

        decisions = resolver.resolve_city(
            "חיפה",
            ["רחוב הרצל", "רחוב הרצל"],
            {
                "101": ["הנביאים"],
                "202": ["הרצל", "הרצליה"],
            },
        )

        self.assertEqual(
            agent_factory.call_args.args[0], "convaiinnovations/laya-multilingual"
        )
        self.assertEqual(agent.predict.call_count, 1)
        self.assertEqual(decisions["רחוב הרצל"]["lamas_id"], "202")
        self.assertEqual(decisions["רחוב הרצל"]["confidence"], 0.88)
        self.assertEqual(decisions["רחוב הרצל"]["method"], "local_laya")

        state, questions = agent.predict.call_args.args
        self.assertEqual(state["יישוב"], "חיפה")
        self.assertEqual(state["שם הרחוב ב-OSM"], "רחוב הרצל")
        self.assertIn("הרצליה", questions["street_match"]["criteria"]["street_2"])
        self.assertIn("no_match", questions["street_match"]["criteria"])

    def test_no_match_choice_returns_none(self):
        resolver, _, _ = self.make_resolver(
            {"answers": {"street_match": {"choice": "no_match", "confidence": 0.91}}}
        )

        result = resolver.resolve_street("חיפה", "שם לא קשור", {"101": ["הרצל"]})

        self.assertIsNone(result["lamas_id"])
        self.assertEqual(result["confidence"], 0.91)
        self.assertEqual(result["method"], "local_laya")

    def test_empty_inputs_do_not_call_model(self):
        resolver, agent, _ = self.make_resolver({})

        self.assertEqual(resolver.resolve_city("חיפה", [], {"101": ["הרצל"]}), {})
        self.assertEqual(resolver.resolve_city("חיפה", ["הרצל"], {}), {})
        agent.predict.assert_not_called()

    def test_resolve_city_batches_multiple_streets_with_predict_batch(self):
        resolver, agent, _ = self.make_resolver()
        agent.predict_batch = Mock(
            return_value=[
                {"answers": {"street_match": {"choice": "street_2", "probabilities": {"street_2": 0.9}}}},
                {"answers": {"street_match": {"choice": "no_match", "probabilities": {"no_match": 0.85}}}},
            ]
        )

        decisions = resolver.resolve_city(
            "חיפה",
            ["שד בן גוריון", "רחוב חסר"],
            {"101": ["הרצל"], "202": ["בן גוריון"]},
        )

        agent.predict_batch.assert_called_once()
        self.assertEqual(decisions["שד בן גוריון"]["lamas_id"], "202")
        self.assertIsNone(decisions["רחוב חסר"]["lamas_id"])

    def test_large_city_chunks_all_lamas_streets_and_runs_final_round(self):
        # 5 LAMAS streets with chunk_size=2 -> chunks [01,02], [03,04], [05],
        # winners from chunk 1 (02) and chunk 2 (03), then final round picks 03.
        responses = [
            {"answers": {"street_match": {"choice": "street_2", "probabilities": {"street_2": 0.7}}}},
            {"answers": {"street_match": {"choice": "street_1", "probabilities": {"street_1": 0.85}}}},
            {"answers": {"street_match": {"choice": "no_match", "probabilities": {"no_match": 0.95}}}},
            {"answers": {"street_match": {"choice": "street_2", "probabilities": {"street_2": 0.92}}}},
        ]
        resolver, agent, _ = self.make_resolver(side_effect=responses, chunk_size=2)
        lamas_streets = {
            "01": ["אלף"],
            "02": ["בית"],
            "03": ["גימל"],
            "04": ["דלת"],
            "05": ["הא"],
        }

        result = resolver.resolve_street("חיפה", "גימל החדש", lamas_streets)

        self.assertEqual(agent.predict.call_count, 4)
        self.assertEqual(result["lamas_id"], "03")
        self.assertEqual(result["confidence"], 0.92)


if __name__ == "__main__":
    unittest.main()
