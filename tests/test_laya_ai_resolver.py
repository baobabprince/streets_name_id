import sys
import unittest
from types import ModuleType
from unittest.mock import Mock, patch

from laya_ai_resolver import LayaAIResolver


class TestLayaAIResolver(unittest.TestCase):
    def make_resolver(self, prediction):
        agent = Mock()
        agent.predict.return_value = prediction
        laya_module = ModuleType("laya")
        laya_module.Agent = Mock(return_value=agent)
        with patch.dict(sys.modules, {"laya": laya_module}):
            resolver = LayaAIResolver()
        return resolver, agent, laya_module.Agent

    def test_selects_lamas_candidate_and_uses_its_probability(self):
        resolver, agent, agent_factory = self.make_resolver(
            {
                "answers": {
                    "street_match": {
                        "choice": "candidate_2",
                        "probabilities": {"candidate_2": 0.82},
                    }
                }
            }
        )

        result = resolver.resolve_street(
            "osm-1",
            "רחוב הרצל",
            "חיפה",
            [
                {"id": "101", "name": "הנביאים"},
                {"id": "202", "name": "הרצל", "names": ["הרצל", "הרצליה"]},
            ],
            ["בלפור"],
            city_lexicon="ID 202: הרצל, הרצליה",
        )

        self.assertEqual(agent_factory.call_args.args[0], "convaiinnovations/laya-multilingual")
        self.assertEqual(result["lamas_id"], "202")
        self.assertEqual(result["confidence"], 0.82)
        state, questions = agent.predict.call_args.args
        self.assertEqual(state["רחובות סמוכים ב-OSM"], ["בלפור"])
        self.assertNotIn("לקסיקון למ״ס בעיר", state)
        self.assertIn("הרצליה", questions["street_match"]["criteria"]["candidate_2"])
        self.assertIn("כינויים", questions["street_match"]["instructions"])
        self.assertIn("no_match", questions["street_match"]["criteria"])

    def test_no_match_choice_returns_no_lamas_id(self):
        resolver, _, _ = self.make_resolver(
            {"answers": {"street_match": {"choice": "no_match", "confidence": 0.9}}}
        )

        result = resolver.resolve_street(
            "osm-2",
            "שם לא קשור",
            "חיפה",
            [{"id": "101", "name": "הרצל"}],
            [],
        )

        self.assertIsNone(result["lamas_id"])
        self.assertEqual(result["confidence"], 0.9)
        self.assertEqual(result["method"], "local_laya")

    def test_empty_candidates_does_not_call_model(self):
        resolver, agent, _ = self.make_resolver({})

        result = resolver.resolve_street("osm-3", "הרצל", "חיפה", [], [])

        self.assertIsNone(result["lamas_id"])
        agent.predict.assert_not_called()


if __name__ == "__main__":
    unittest.main()
