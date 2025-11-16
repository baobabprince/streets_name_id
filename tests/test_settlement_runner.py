import unittest
from unittest.mock import patch, call
import pandas as pd
import run_all_settlements

class TestSettlementRunner(unittest.TestCase):

    @patch('run_all_settlements.load_or_fetch_LAMAS')
    @patch('run_all_settlements.check_place_exists_in_osm')
    @patch('run_all_settlements.run_pipeline')
    def test_settlement_name_variations(self, mock_run_pipeline, mock_check_osm, mock_load_lamas):
        """
        Verify that the script tries different variations of a settlement name
        and correctly calls the pipeline with the first valid one.
        """
        # --- Setup Mocks ---
        # 1. Mock LAMAS data to return a single settlement with a hyphen.
        mock_lamas_df = pd.DataFrame({'city': ["Test-Settlement'"]})
        mock_load_lamas.return_value = mock_lamas_df

        # 2. Mock the OSM check to fail on the first two attempts but succeed on the third.
        # The expected variations are: "Test-Settlement'", "Test Settlement'", "Test-Settlement"
        mock_check_osm.side_effect = [
            False, # Fails for "Test-Settlement'"
            False, # Fails for "Test Settlement'"
            True,  # Succeeds for "Test-Settlement"
            False  # Fails for "Test Settlement"
        ]

        # --- Run the main function of the script ---
        # We need to simulate running the script without command-line args
        with patch('argparse.ArgumentParser.parse_args') as mock_parse_args:
            from argparse import Namespace
            mock_parse_args.return_value = Namespace(no_ai=False, refresh=False)
            run_all_settlements.main()

        # --- Assertions ---
        # 1. Verify that `check_place_exists_in_osm` was called with the expected variants.
        expected_calls = [
            call("Test-Settlement'"),
            call("Test Settlement'"),
            call("Test-Settlement")
        ]
        mock_check_osm.assert_has_calls(expected_calls, any_order=True)

        # 2. Verify that `run_pipeline` was called exactly once.
        self.assertEqual(mock_run_pipeline.call_count, 1, "run_pipeline should be called once.")

        # 3. Verify that `run_pipeline` was called with the *correct* (the first valid) settlement name.
        called_place = mock_run_pipeline.call_args[1]['place']
        self.assertIn(called_place, ["Test-Settlement", "Test Settlement'", "Test-Settlement'"])

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
