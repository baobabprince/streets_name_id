import unittest
from unittest.mock import Mock, patch

import pipeline


class TestPipelineResolverCache(unittest.TestCase):
    def setUp(self):
        self.original_resolver = pipeline._LAYA_RESOLVER
        pipeline._LAYA_RESOLVER = None

    def tearDown(self):
        pipeline._LAYA_RESOLVER = self.original_resolver

    def test_reuses_initialized_laya_resolver(self):
        resolver = Mock()
        resolver.is_available.return_value = True
        with patch("pipeline.LayaAIResolver", return_value=resolver) as resolver_class:
            first = pipeline._get_laya_resolver()
            second = pipeline._get_laya_resolver()

        self.assertIs(first, resolver)
        self.assertIs(second, resolver)
        resolver_class.assert_called_once_with()

    def test_does_not_cache_unavailable_resolver(self):
        resolver = Mock()
        resolver.is_available.return_value = False
        with patch("pipeline.LayaAIResolver", return_value=resolver) as resolver_class:
            self.assertIsNone(pipeline._get_laya_resolver())
            self.assertIsNone(pipeline._get_laya_resolver())

        self.assertEqual(resolver_class.call_count, 2)


if __name__ == "__main__":
    unittest.main()
