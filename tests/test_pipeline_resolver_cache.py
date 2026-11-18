import unittest
from unittest.mock import Mock, patch

import pipeline


class TestPipelineResolverCache(unittest.TestCase):
    def setUp(self):
        self.original_cache = pipeline._LOCAL_RESOLVER_CACHE.copy()
        pipeline._LOCAL_RESOLVER_CACHE.clear()

    def tearDown(self):
        pipeline._LOCAL_RESOLVER_CACHE.clear()
        pipeline._LOCAL_RESOLVER_CACHE.update(self.original_cache)

    def test_reuses_initialized_resolver_for_same_model(self):
        resolver = Mock()
        resolver.is_available.return_value = True
        with patch("pipeline.LayaAIResolver", return_value=resolver) as resolver_class:
            first = pipeline._get_local_resolver("laya")
            second = pipeline._get_local_resolver("laya")

        self.assertIs(first, resolver)
        self.assertIs(second, resolver)
        resolver_class.assert_called_once_with()

    def test_does_not_cache_unavailable_resolver(self):
        resolver = Mock()
        resolver.is_available.return_value = False
        with patch("pipeline.LayaAIResolver", return_value=resolver) as resolver_class:
            self.assertIsNone(pipeline._get_local_resolver("laya"))
            self.assertIsNone(pipeline._get_local_resolver("laya"))

        self.assertEqual(resolver_class.call_count, 2)


if __name__ == "__main__":
    unittest.main()
