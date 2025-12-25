
import unittest
from unittest.mock import MagicMock, patch
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Mock transformers/torch before importing local_ai_resolver
sys.modules['transformers'] = MagicMock()
sys.modules['torch'] = MagicMock()

from local_ai_resolver import LocalAIResolver

class TestAIContext(unittest.TestCase):
    
    def setUp(self):
        # Mock the initialization to avoid loading models
        with patch.object(LocalAIResolver, '_initialize_model'):
            self.resolver = LocalAIResolver()
            self.resolver.initialized = True # Fake it

    def test_local_ai_prompt_content(self):
        """
        Test that the Local AI prompt contains key instructions, 
        specifically regarding partial matches and different people.
        """
        osm_name = "הרצל"
        city = "תל אביב"
        candidates = [
            {'id': 1, 'name': 'הרצל', 'score': 100},
            {'id': 2, 'name': 'הרצל רוזנבלום', 'score': 90}
        ]
        adjacent = ["ארלוזורוב", "ז'בוטינסקי"]
        
        prompt = self.resolver.prepare_prompt(osm_name, city, candidates, adjacent)
        
        # Check for basic context
        self.assertIn(f'עיר: {city}', prompt)
        self.assertIn(f'"{osm_name}"', prompt)
        self.assertIn("ארלוזורוב", prompt)
        
        # Check for candidates
        self.assertIn("הרצל רוזנבלום", prompt)
        
        # Check for CRITICAL new instruction about distinguishing different people
        # This is the "Red" test - expecting a failure until we implement the prompt update
        self.assertIn("הרצל רוזנבלום", prompt) # The candidate name
        self.assertIn("אישיות שונה", prompt, "Prompt should warn about matching to a different person")

    @patch('pipeline.requests.post')
    @patch('pipeline.API_KEY', 'fake_key')
    def test_pipeline_system_prompt(self, mock_post):
        """
        Test that the Gemini system prompt contains the critical warning.
        """
        from pipeline import get_ai_resolution
        import json
        
        # Mock response to avoid actual logic errors
        mock_response = MagicMock()
        mock_response.json.return_value = {'candidates': [{'content': {'parts': [{'text': '123'}]}}]}
        mock_post.return_value = mock_response
        
        get_ai_resolution("test prompt", "123")
        
        # Get the args passed to requests.post
        call_args = mock_post.call_args
        # data is the named argument 'data'
        data_str = call_args[1]['data']
        data_json = json.loads(data_str)
        
        system_instruction = data_json['systemInstruction']['parts'][0]['text']
        
        self.assertIn("היזהר מהתאמות חלקיות", system_instruction)
        self.assertIn("הרצל רוזנבלום", system_instruction)

if __name__ == '__main__':
    unittest.main()
