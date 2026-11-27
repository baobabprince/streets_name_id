import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import geopandas as gpd
import pandas as pd
import requests
from shapely.geometry import LineString

import batch_process_settlements as batch
import pipeline
from rapidfuzz import fuzz as _rf_fuzz
from arabic_matching import (
    arabic_hebrew_skeleton,
    cross_script_comparison_skeleton,
    latin_to_hebrew_skeleton,
)
from openrouter_ai_resolver import (
    DEFAULT_OPENROUTER_MODEL,
    DecisionCache,
    HybridAIResolver,
    OpenRouterDecisionResolver,
    select_candidate_shortlist,
)
from pipeline import PipelineStatus, _finalize_mappings


def _mock_response(status_code=200, payload=None, text="", headers=None):
    resp = Mock(spec=requests.Response)
    resp.status_code = status_code
    resp.headers = headers or {}
    resp.text = text if text else (str(payload) if payload is not None else "")
    if payload is not None:
        resp.json.return_value = payload
    else:
        resp.json.side_effect = ValueError("No JSON body")
    return resp


class TestCandidateShortlist(unittest.TestCase):
    def test_small_city_returns_all_sorted_lamas_ids(self):
        lamas = {"20": ["הרצל"], "05": ["אלנבי"], "10": ["רוטשילד"]}
        shortlist = select_candidate_shortlist(["כלשהו"], lamas, shortlist_size=10)
        self.assertEqual(shortlist["כלשהו"], ["05", "10", "20"])

    def test_large_city_surfaces_hard_hebrew_arabic_digit_and_preferred_candidates(self):
        lamas = {f"d{i:03d}": [f"רחוב ירוק {i}"] for i in range(60)}
        # Flood with common token "יצחק" and prefix "כיכר" to ensure no starvation.
        for i in range(60, 95):
            lamas[f"y{i:03d}"] = [f"יצחק שמיר {i}", f"כיכר מדומה {i}"]
        lamas["6065"] = ["אשבל דב כיכר"]
        lamas["418"] = ["אלישר"]
        lamas["670"] = ["סלאח א דין"]
        lamas["108"] = ["הים", "רחוב הים"]
        lamas["2100"] = ["חטיבת גולני 21 ב"]
        lamas["pref99"] = ["שם אקראי לחלוטין"]

        queries = [
            "כיכר שרה ועמינדב אשבל",
            "אלישיר יצחק",
            "סאלח א דין אל איובי",
            "شارع البحر",
            "גולני 21",
            "רחוב לא דומה לכלום",
        ]
        shortlist = select_candidate_shortlist(
            queries,
            lamas,
            shortlist_size=25,
            preferred_ids={"רחוב לא דומה לכלום": "pref99"},
        )

        self.assertEqual(len(shortlist["כיכר שרה ועמינדב אשבל"]), 25)
        self.assertIn("6065", shortlist["כיכר שרה ועמינדב אשבל"])
        self.assertIn("418", shortlist["אלישיר יצחק"])
        self.assertIn("670", shortlist["סאלח א דין אל איובי"])
        self.assertIn("108", shortlist["شارع البحر"])
        self.assertIn("2100", shortlist["גולני 21"])
        self.assertEqual(len(shortlist["רחוב לא דומה לכלום"]), 25)
        self.assertIn("pref99", shortlist["רחוב לא דומה לכלום"])


class TestOpenRouterDecisionResolver(unittest.TestCase):
    def test_payload_format_translit_hint_and_response_parsing(self):
        captured = []

        def fake_post(url, headers=None, json=None, timeout=None):
            captured.append((url, headers, json))
            return _mock_response(
                200,
                {
                    "id": "dec-1",
                    "answers": {
                        "q_0": {
                            "type": "choice",
                            "choice": "s_2",
                            "probabilities": {"s_1": 0.04, "s_2": 0.93, "no_match": 0.03},
                            "confidence": 0.91,
                        },
                        "q_1": {
                            "type": "choice",
                            "choice": "no_match",
                            "probabilities": {"s_1": 0.10, "s_2": 0.08, "no_match": 0.82},
                            "confidence": 0.82,
                        },
                    },
                    "usage": {"input_tokens": 120, "output_tokens": 2, "cost": 0},
                },
            )

        session = Mock()
        session.post.side_effect = fake_post
        resolver = OpenRouterDecisionResolver(
            api_key="test-key",
            max_rpm=0,
            cache_path=None,
            session=session,
        )

        lamas = {"101": ["הנביאים"], "202": ["הים", "רחוב הים"]}
        decisions = resolver.resolve_city(
            "חיפה",
            ["شارع البحر", "רחוב מומצא"],
            lamas,
        )

        self.assertEqual(len(captured), 1)
        url, headers, payload = captured[0]
        self.assertIn("/api/alpha/decisions", url)
        self.assertEqual(headers["Authorization"], "Bearer test-key")
        self.assertEqual(payload["model"], DEFAULT_OPENROUTER_MODEL)
        self.assertIsInstance(payload["state"], str)
        self.assertEqual(set(payload["questions"].keys()), {"q_0", "q_1"})

        q0 = payload["questions"]["q_0"]
        self.assertEqual(q0["type"], "choice")
        self.assertIn("תעתיק עברי:", q0["instructions"])
        self.assertIn("no_match", q0["criteria"])
        self.assertIn("הים", q0["criteria"]["s_2"])

        q1 = payload["questions"]["q_1"]
        self.assertNotIn("תעתיק עברי:", q1["instructions"])

        self.assertEqual(decisions["شارع البحر"]["lamas_id"], "202")
        self.assertAlmostEqual(decisions["شارع البحر"]["confidence"], 0.93)
        self.assertEqual(
            decisions["شارع البحر"]["method"],
            f"openrouter:{DEFAULT_OPENROUTER_MODEL}",
        )
        self.assertIsNone(decisions["רחוב מומצא"]["lamas_id"])
        self.assertAlmostEqual(decisions["רחוב מומצא"]["confidence"], 0.82)

    def test_multi_city_batching_splits_requests_by_question_limit(self):
        calls = []

        def fake_post(url, headers=None, json=None, timeout=None):
            calls.append(json)
            answers = {
                qk: {
                    "type": "choice",
                    "choice": "s_1",
                    "probabilities": {"s_1": 0.9, "no_match": 0.1},
                }
                for qk in json["questions"]
            }
            return _mock_response(200, {"answers": answers, "usage": {"input_tokens": 50}})

        session = Mock()
        session.post.side_effect = fake_post
        resolver = OpenRouterDecisionResolver(
            api_key="test-key",
            questions_per_request=2,
            max_rpm=0,
            cache_path=None,
            session=session,
        )

        decisions = resolver.resolve_multi_city(
            [
                ("עיר א", ["רחוב 1", "רחוב 2"], {"1": ["אלף"]}),
                ("עיר ב", ["רחוב 3"], {"2": ["בית"]}),
            ]
        )

        # 3 total streets across 2 cities with limit=2 -> 2 HTTP calls (2 + 1).
        self.assertEqual(len(calls), 2)
        self.assertEqual(len(calls[0]["questions"]), 2)
        self.assertEqual(len(calls[1]["questions"]), 1)
        self.assertEqual(decisions["עיר א"]["רחוב 1"]["lamas_id"], "1")
        self.assertEqual(decisions["עיר א"]["רחוב 2"]["lamas_id"], "1")
        self.assertEqual(decisions["עיר ב"]["רחוב 3"]["lamas_id"], "2")

    def test_disk_cache_hits_skip_http_and_never_caches_refusals_or_errors(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_file = os.path.join(tmpdir, "or_cache.json")
            session = Mock()
            session.post.return_value = _mock_response(
                200,
                {
                    "answers": {
                        "q_0": {"type": "choice", "choice": "s_1", "probabilities": {"s_1": 0.95}},
                        "q_1": {"type": "refusal"},
                    },
                    "usage": {"input_tokens": 20},
                },
            )
            r1 = OpenRouterDecisionResolver(
                api_key="test-key",
                max_rpm=0,
                cache_path=cache_file,
                session=session,
            )
            res1 = r1.resolve_city(
                "חיפה",
                ["הרצל א", "רחוב שסורב"],
                {"10": ["הרצל"]},
            )
            self.assertEqual(res1["הרצל א"]["lamas_id"], "10")
            self.assertEqual(res1["רחוב שסורב"]["method"], "openrouter_refusal")
            self.assertEqual(session.post.call_count, 1)

            # Second resolver loading same cache: cached street skips HTTP,
            # refused street WAS NOT cached so it gets retried via HTTP.
            session2 = Mock()
            session2.post.return_value = _mock_response(
                200,
                {
                    "answers": {
                        "q_0": {"type": "choice", "choice": "no_match", "probabilities": {"no_match": 0.9}},
                    }
                },
            )
            r2 = OpenRouterDecisionResolver(
                api_key="test-key",
                max_rpm=0,
                cache_path=cache_file,
                session=session2,
            )
            res2 = r2.resolve_city(
                "חיפה",
                ["הרצל א", "רחוב שסורב"],
                {"10": ["הרצל"]},
            )
            self.assertEqual(r2.stats["cache_hits"], 1)
            self.assertEqual(session2.post.call_count, 1)
            sent_qs = session2.post.call_args.kwargs["json"]["questions"]
            self.assertEqual(len(sent_qs), 1)
            self.assertIn("רחוב שסורב", sent_qs["q_0"]["instructions"])
            self.assertIsNone(res2["רחוב שסורב"]["lamas_id"])


class TestHybridFallbackAndLogging(unittest.TestCase):
    def _make_local_mock(self, lamas_id="101", conf=0.88):
        local = Mock()
        local.is_available.return_value = True

        def fake_resolve(city_name, osm_streets, lamas_streets):
            return {
                s: {
                    "lamas_id": lamas_id,
                    "confidence": conf,
                    "reasoning": "Laya selected street_1",
                    "method": "local_laya",
                }
                for s in osm_streets
            }

        local.resolve_city.side_effect = fake_resolve
        return local

    def test_missing_api_key_logs_warning_and_uses_local_laya(self):
        local = self._make_local_mock()
        local_factory = Mock(return_value=local)
        primary = OpenRouterDecisionResolver(api_key="", max_rpm=0, cache_path=None)
        hybrid = HybridAIResolver(primary=primary, local_factory=local_factory)

        with self.assertLogs("openrouter_ai_resolver", level="WARNING") as cm:
            res = hybrid.resolve_city("חיפה", ["שד רוטשילד"], {"101": ["רוטשילד"]})

        self.assertTrue(any("OPENROUTER_API_KEY" in line or "fallback" in line for line in cm.output))
        local_factory.assert_called_once()
        local.resolve_city.assert_called_once()
        self.assertEqual(res["שד רוטשילד"]["lamas_id"], "101")
        self.assertEqual(res["שד רוטשילד"]["method"], "local_laya")
        self.assertIn("fallback:", res["שד רוטשילד"]["reasoning"])

    def test_http_500_retries_then_falls_back_with_warning(self):
        session = Mock()
        session.post.return_value = _mock_response(503, text="upstream overloaded")
        sleep_calls = []
        primary = OpenRouterDecisionResolver(
            api_key="test-key",
            max_rpm=0,
            max_retries=2,
            cache_path=None,
            session=session,
            sleep_fn=sleep_calls.append,
        )
        local = self._make_local_mock(lamas_id="202", conf=0.91)
        hybrid = HybridAIResolver(primary=primary, local_factory=Mock(return_value=local))

        with self.assertLogs("openrouter_ai_resolver", level="WARNING") as cm:
            res = hybrid.resolve_city("חיפה", ["הנביאים"], {"202": ["הנביאים"]})

        self.assertEqual(session.post.call_count, 3)  # 1 initial + 2 retries
        self.assertEqual(len(sleep_calls), 2)
        self.assertTrue(any("503" in line for line in cm.output))
        self.assertEqual(res["הנביאים"]["lamas_id"], "202")
        self.assertEqual(res["הנביאים"]["method"], "local_laya")
        self.assertEqual(hybrid.stats["fallback_events"], 1)

    def test_daily_quota_429_opens_circuit_breaker_and_skips_further_api_calls(self):
        session = Mock()
        session.post.return_value = _mock_response(
            429,
            payload={"error": {"message": "Rate limit exceeded: free-models-per-day."}},
            text="free-models-per-day",
        )
        primary = OpenRouterDecisionResolver(
            api_key="test-key",
            max_rpm=0,
            max_retries=2,
            cache_path=None,
            session=session,
        )
        local = self._make_local_mock(lamas_id="303")
        hybrid = HybridAIResolver(primary=primary, local_factory=Mock(return_value=local))

        with self.assertLogs("openrouter_ai_resolver", level="WARNING") as cm:
            r1 = hybrid.resolve_city("חיפה", ["א"], {"303": ["אלף"]})
            r2 = hybrid.resolve_city("עכו", ["ב"], {"303": ["אלף"]})

        # Daily limit must fail permanently on first call and NOT retry or hit API on second city.
        self.assertEqual(session.post.call_count, 1)
        self.assertFalse(primary.is_available())
        self.assertTrue(any("429" in line and "daily" in line for line in cm.output))
        self.assertEqual(r1["א"]["method"], "local_laya")
        self.assertEqual(r2["ב"]["method"], "local_laya")
        self.assertEqual(local.resolve_city.call_count, 2)

    def test_partial_refusal_routes_only_refused_street_to_local_laya(self):
        session = Mock()
        session.post.return_value = _mock_response(
            200,
            {
                "answers": {
                    "q_0": {"type": "choice", "choice": "s_1", "probabilities": {"s_1": 0.94}},
                    "q_1": {"type": "refusal"},
                },
                "usage": {"input_tokens": 50},
            },
        )
        primary = OpenRouterDecisionResolver(
            api_key="test-key",
            max_rpm=0,
            cache_path=None,
            session=session,
        )
        local = self._make_local_mock(lamas_id="202", conf=0.87)
        hybrid = HybridAIResolver(primary=primary, local_factory=Mock(return_value=local))

        with self.assertLogs("openrouter_ai_resolver", level="WARNING") as cm:
            res = hybrid.resolve_city(
                "חיפה",
                ["הרצל", "בלפור"],
                {"101": ["הרצל"], "202": ["בלפור"]},
            )

        self.assertTrue(any("fallback" in line for line in cm.output))
        self.assertEqual(res["הרצל"]["method"], f"openrouter:{DEFAULT_OPENROUTER_MODEL}")
        self.assertEqual(res["הרצל"]["lamas_id"], "101")
        # Local Laya was called ONLY for the refused street.
        local.resolve_city.assert_called_once_with(
            "חיפה", ["בלפור"], {"101": ["הרצל"], "202": ["בלפור"]}
        )
        self.assertEqual(res["בלפור"]["method"], "local_laya")
        self.assertEqual(res["בלפור"]["lamas_id"], "202")

    def test_both_openrouter_and_local_fail_gracefully_with_error_log(self):
        session = Mock()
        session.post.side_effect = requests.Timeout("timed out")
        primary = OpenRouterDecisionResolver(
            api_key="test-key",
            max_rpm=0,
            max_retries=0,
            cache_path=None,
            session=session,
        )
        hybrid = HybridAIResolver(
            primary=primary,
            local_factory=Mock(side_effect=RuntimeError("no local model")),
        )

        with self.assertLogs("openrouter_ai_resolver", level="ERROR") as cm:
            res = hybrid.resolve_city("חיפה", ["הרצל"], {"101": ["הרצל"]})

        self.assertTrue(any("failed" in line.lower() for line in cm.output))
        self.assertIsNone(res["הרצל"]["lamas_id"])
        self.assertEqual(res["הרצל"]["method"], "hybrid_ai_error")


class TestPipelineAndBatchAIIntegration(unittest.TestCase):
    def setUp(self):
        self.orig_laya = pipeline._LAYA_RESOLVER
        self.orig_ai = pipeline._AI_RESOLVER
        self.orig_ai_key = pipeline._AI_RESOLVER_KEY
        pipeline._LAYA_RESOLVER = None
        pipeline._AI_RESOLVER = None
        pipeline._AI_RESOLVER_KEY = None

    def tearDown(self):
        pipeline._LAYA_RESOLVER = self.orig_laya
        pipeline._AI_RESOLVER = self.orig_ai
        pipeline._AI_RESOLVER_KEY = self.orig_ai_key

    def test_cli_flags_and_forwarding(self):
        args = batch.build_cli_parser().parse_args(
            ["--use-ai", "--ai-backend", "openrouter", "--ai-model", "perplexity/pplx-decider-flash-v1.3:free"]
        )
        self.assertEqual(args.ai_backend, "openrouter")
        self.assertEqual(args.ai_model, "perplexity/pplx-decider-flash-v1.3:free")

        with patch.object(batch, "run_pipeline", return_value=PipelineStatus.SUCCESS) as run:
            batch.worker_wrapper(
                "חיפה",
                "חיפה, Israel",
                use_ai=True,
                ai_backend="openrouter",
                ai_model="custom/model",
            )
        self.assertEqual(run.call_args.kwargs["ai_backend"], "openrouter")
        self.assertEqual(run.call_args.kwargs["ai_model"], "custom/model")

    def test_pipeline_auto_with_key_uses_hybrid_and_passes_preferred_ids(self):
        osm_gdf = gpd.GeoDataFrame(
            {
                "osm_id": ["osm-exact", "osm-low"],
                "osm_name": ["הרצל", "דוד בן גוריון"],
                "city": ["יישוב בדיקה", "יישוב בדיקה"],
                "geometry": [
                    LineString([(34.80, 32.00), (34.801, 32.00)]),
                    LineString([(34.802, 32.00), (34.803, 32.00)]),
                ],
            },
            crs="EPSG:4326",
        )
        lamas_df = pd.DataFrame(
            {
                "LAMAS_id": ["101", "202"],
                "LAMAS_name": ["הרצל", "בן גוריון"],
                "city": ["יישוב בדיקה", "יישוב בדיקה"],
            }
        )
        session = Mock()
        session.post.return_value = _mock_response(
            200,
            {
                "answers": {
                    "q_0": {"type": "choice", "choice": "s_2", "probabilities": {"s_2": 0.94}},
                },
                "usage": {"input_tokens": 40},
            },
        )
        primary = OpenRouterDecisionResolver(
            api_key="test-key",
            max_rpm=0,
            cache_path=None,
            session=session,
        )
        hybrid = HybridAIResolver(primary=primary, local_factory=Mock())
        saved = {}

        with (
            patch.dict(os.environ, {"OPENROUTER_API_KEY": "test-key"}),
            patch.object(pipeline, "HybridAIResolver", return_value=hybrid) as hybrid_cls,
            patch.object(pipeline, "load_or_fetch_LAMAS", return_value=lamas_df),
            patch.object(pipeline, "load_or_fetch_osm", return_value=osm_gdf),
            patch.object(
                pipeline,
                "save_intermediate_df",
                side_effect=lambda df, name, place: saved.setdefault(name, df.copy()),
            ),
        ):
            status = pipeline.run_pipeline(
                "יישוב בדיקה",
                use_ai=True,
                skip_html=True,
                ai_backend="auto",
            )

        self.assertEqual(status, PipelineStatus.SUCCESS)
        hybrid_cls.assert_called_once()
        diagnostic = saved["diagnostic_report"].set_index("osm_id")
        self.assertEqual(diagnostic.loc["osm-low", "mapping_status"], "APPROVED_AI")
        self.assertEqual(diagnostic.loc["osm-low", "final_LAMAS_id"], "202")
        self.assertEqual(
            diagnostic.loc["osm-low", "ai_method"],
            f"openrouter:{DEFAULT_OPENROUTER_MODEL}",
        )

    def test_pipeline_auto_without_key_logs_warning_and_uses_laya(self):
        env_clean = {k: v for k, v in os.environ.items() if k != "OPENROUTER_API_KEY"}
        laya = Mock()
        laya.is_available.return_value = True
        with (
            patch.dict(os.environ, env_clean, clear=True),
            patch.object(pipeline, "_get_laya_resolver", return_value=laya) as get_laya,
            self.assertLogs("pipeline", level="WARNING") as cm,
        ):
            res = pipeline._get_ai_resolver(backend="auto")
        self.assertIs(res, laya)
        get_laya.assert_called_once()
        self.assertTrue(any("OPENROUTER_API_KEY" in line for line in cm.output))

    def test_lazy_local_factory_not_called_when_openrouter_succeeds(self):
        session = Mock()
        session.post.return_value = _mock_response(
            200,
            {"answers": {"q_0": {"type": "choice", "choice": "s_1", "probabilities": {"s_1": 0.91}}}},
        )
        primary = OpenRouterDecisionResolver(
            api_key="test-key", max_rpm=0, cache_path=None, session=session
        )
        local_factory = Mock()
        hybrid = HybridAIResolver(primary=primary, local_factory=local_factory)
        hybrid.resolve_city("חיפה", ["הרצל"], {"101": ["הרצל"]})
        local_factory.assert_not_called()


class TestCrossScriptAndSemanticApproval(unittest.TestCase):
    def test_latin_to_hebrew_skeleton_and_cross_script_comparison(self):
        pairs = [
            ("Gedud 68", "גדוד 68"),
            ("haRav Shach", "הרב שך"),
            ("Oren", "אורן"),
            ("ramat hagolan road", "רמת הגולן"),
            ("El Jawalida Rd", "אל גוואלידה"),
            ("al Malik", "אל מלק"),
            ("Alef Dalet Gordon", "אלף דלת גורדון"),
        ]
        for lat, heb in pairs:
            lat_sk = cross_script_comparison_skeleton(latin_to_hebrew_skeleton(lat))
            heb_sk = cross_script_comparison_skeleton(arabic_hebrew_skeleton(heb))
            self.assertGreaterEqual(
                _rf_fuzz.ratio(lat_sk, heb_sk),
                85.0,
                f"Expected {lat!r} ({lat_sk}) ~ {heb!r} ({heb_sk})",
            )

    def test_shortlist_surfaces_latin_hebrew_streets_in_large_city(self):
        lamas = {f"d{i:03d}": [f"רחוב אקראי {i}"] for i in range(100)}
        lamas["301"] = ["אורן"]
        lamas["302"] = ["הרב שך"]
        shortlist = select_candidate_shortlist(["Oren", "haRav Shach"], lamas, shortlist_size=25)
        self.assertIn("301", shortlist["Oren"])
        self.assertIn("302", shortlist["haRav Shach"])

    def test_openrouter_cross_script_and_semantic_gates(self):
        candidates = pd.DataFrame(
            {
                "osm_id": ["cs-1", "cs-2", "cs-3", "acr-1", "ar-sq", "laya-low", "or-dig", "or-mag1", "or-mag2", "or-mag3"],
                "normalized_name": [
                    "Gedud 68",
                    "Oren",
                    "סוק אל נאסרה",
                    "הבעשט",
                    "דואר אל נורס",
                    "הבעשט",
                    "Gedud 68",
                    "מבוא אלף",
                    "דרך בית",
                    "שביל גימל",
                ],
                "status": ["NEEDS_AI"] * 10,
                "best_score": [42.0, 0.0, 57.0, 67.5, 77.7, 67.5, 42.0, 60.0, 60.0, 60.0],
                "best_LAMAS_id": ["101", "999", "999", "999", "999", "999", "999", "999", "999", "999"],
                "best_LAMAS_name": ["גדוד 68", "משהו אחר", "רחוב אחר", "הגולן", "ד'ו אל נורין", "הגולן", "גדוד 68", "הבזלת", "הבזלת", "הבזלת"],
            }
        )
        # Include Arabic-script versions on two rows via normalized_name
        candidates.loc[candidates["osm_id"] == "cs-3", "normalized_name"] = "سوق الناصره"
        candidates.loc[candidates["osm_id"] == "ar-sq", "normalized_name"] = "دوار النورس"
        candidates.loc[candidates["osm_id"] == "or-dig", "normalized_name"] = "Gedud 68"

        ai_decisions = pd.DataFrame(
            {
                "osm_id": candidates["osm_id"].tolist(),
                "ai_suggested_LAMAS_id": ["101", "102", "103", "104", "105", "104", "106", "107", "107", "107"],
                "ai_confidence": [0.989, 0.937, 0.928, 0.956, 0.96, 0.98, 0.99, 0.99, 0.99, 0.99],
                "ai_reasoning": ["OpenRouter"] * 10,
                "ai_method": [
                    f"openrouter:{DEFAULT_OPENROUTER_MODEL}",
                    f"openrouter:{DEFAULT_OPENROUTER_MODEL}",
                    f"openrouter:{DEFAULT_OPENROUTER_MODEL}",
                    f"openrouter:{DEFAULT_OPENROUTER_MODEL}",
                    f"openrouter:{DEFAULT_OPENROUTER_MODEL}",
                    "local_laya",
                    f"openrouter:{DEFAULT_OPENROUTER_MODEL}",
                    f"openrouter:{DEFAULT_OPENROUTER_MODEL}",
                    f"openrouter:{DEFAULT_OPENROUTER_MODEL}",
                    f"openrouter:{DEFAULT_OPENROUTER_MODEL}",
                ],
            }
        )
        lamas_names = {
            "101": "גדוד 68",
            "102": "אורן",
            "103": "השוק",
            "104": "הבעל שם טוב",
            "105": "ד'ו אל נורין",
            "106": "גדוד 69",
            "107": "הבזלת",
        }
        merged, _ = _finalize_mappings(
            candidates,
            ai_decisions,
            lamas_name_by_id=lamas_names,
            city_name="עיר בדיקה",
        )
        by_id = merged.set_index("osm_id")
        self.assertEqual(by_id.loc["cs-1", "mapping_status"], "APPROVED_AI")
        self.assertEqual(by_id.loc["cs-2", "mapping_status"], "APPROVED_AI")
        self.assertEqual(by_id.loc["cs-3", "mapping_status"], "APPROVED_AI")
        self.assertEqual(by_id.loc["acr-1", "mapping_status"], "APPROVED_AI")
        self.assertEqual(by_id.loc["ar-sq", "mapping_status"], "UNMATCHED")
        self.assertIn("feature_type_mismatch", by_id.loc["ar-sq", "ai_reasoning"])
        self.assertEqual(by_id.loc["laya-low", "mapping_status"], "UNMATCHED")
        self.assertEqual(by_id.loc["or-dig", "mapping_status"], "UNMATCHED")
        self.assertIn("digit_mismatch", by_id.loc["or-dig", "ai_reasoning"])
        for mag in ("or-mag1", "or-mag2", "or-mag3"):
            self.assertEqual(by_id.loc[mag, "mapping_status"], "UNMATCHED")


if __name__ == "__main__":
    unittest.main()
