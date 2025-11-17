# tests/test_local_ai_matching.py
import unittest
from unittest.mock import patch, MagicMock
from local_ai_matching import get_local_ai_resolution

class TestLocalAIMatching(unittest.TestCase):

    @patch('local_ai_matching.load_model')
    def test_get_local_ai_resolution_success(self, mock_load_model):
        """
        Tests that the get_local_ai_resolution function correctly calls the model
        and returns the cleaned response.
        """
        # Mock the model and tokenizer
        mock_model = MagicMock()
        mock_tokenizer = MagicMock()

        # Set the mock return values
        mock_tokenizer.apply_chat_template.return_value = "formatted_prompt"
        mock_encoded_prompt = MagicMock()
        mock_encoded_prompt.to.return_value = "encoded_prompt_on_device"
        mock_tokenizer.encode.return_value = mock_encoded_prompt
        mock_model.generate.return_value = "encoded_response"
        mock_tokenizer.decode.side_effect = ["decoded_response_with_id_12345"]

        # Patch the global model and tokenizer
        with patch.dict('local_ai_matching.__dict__', {'model': mock_model, 'tokenizer': mock_tokenizer}):
            result_id, result_score = get_local_ai_resolution("test_prompt", "test_osm_id")

            # --- Assertions ---
            # 1. Check that the model was called with the correct prompt
            mock_tokenizer.apply_chat_template.assert_called_once()
            mock_tokenizer.encode.assert_called_once_with("formatted_prompt", add_special_tokens=False, return_tensors="pt")
            mock_model.generate.assert_called_once()

            # 2. Check that the response was decoded and cleaned correctly
            self.assertEqual(result_id, "12345")
            self.assertEqual(result_score, 75)

    @patch('local_ai_matching.load_model')
    def test_get_local_ai_resolution_no_id(self, mock_load_model):
        """
        Tests that the function returns 'None' when the model's response
        does not contain a valid ID.
        """
        # Mock the model and tokenizer
        mock_model = MagicMock()
        mock_tokenizer = MagicMock()

        # Set the mock return values to simulate a response with no ID
        mock_tokenizer.decode.return_value = "decoded_response_with_no_id"

        # Patch the global model and tokenizer
        with patch.dict('local_ai_matching.__dict__', {'model': mock_model, 'tokenizer': mock_tokenizer}):
            result_id, result_score = get_local_ai_resolution("test_prompt", "test_osm_id")

            # Assert that the function returns 'None'
            self.assertEqual(result_id, "None")
            self.assertEqual(result_score, 0)

if __name__ == '__main__':
    unittest.main()
