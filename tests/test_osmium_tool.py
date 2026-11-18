import os
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import osm_pbf_loader


class TestOsmiumToolSetup(unittest.TestCase):
    def test_pbf_decompression_error_refreshes_and_retries_once(self):
        command = ["/usr/bin/osmium", "tags-filter", "region.osm.pbf"]
        success = subprocess.CompletedProcess(command, 0)
        with (
            patch.object(
                osm_pbf_loader.subprocess,
                "run",
                side_effect=[
                    subprocess.CalledProcessError(
                        1, command, stderr="failed to uncompress data: buffer error"
                    ),
                    success,
                ],
            ) as run,
            patch.object(osm_pbf_loader, "download_osm_pbf") as download,
        ):
            result = osm_pbf_loader._run_osmium_with_pbf_retry(
                command, "region.osm.pbf"
            )

        self.assertIs(result, success)
        self.assertEqual(run.call_count, 2)
        download.assert_called_once_with(
            target_path="region.osm.pbf", force=True
        )

    def test_other_osmium_errors_do_not_redownload_pbf(self):
        command = ["/usr/bin/osmium", "tags-filter", "region.osm.pbf"]
        failure = subprocess.CalledProcessError(
            1, command, stderr="permission denied writing output"
        )
        with (
            patch.object(osm_pbf_loader.subprocess, "run", side_effect=failure),
            patch.object(osm_pbf_loader, "download_osm_pbf") as download,
        ):
            with self.assertRaises(subprocess.CalledProcessError):
                osm_pbf_loader._run_osmium_with_pbf_retry(command, "region.osm.pbf")

        download.assert_not_called()

    def test_forced_download_discards_stale_partial_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            pbf_path = os.path.join(temp_dir, "region.osm.pbf")
            temp_path = pbf_path + ".tmp"
            with open(pbf_path, "wb") as target:
                target.write(b"old")
            with open(temp_path, "wb") as partial:
                partial.write(b"truncated")

            def write_fresh_download(_url, target_path):
                with open(target_path, "wb") as fresh:
                    fresh.write(b"fresh")

            with (
                patch.object(osm_pbf_loader.shutil, "which", return_value=None),
                patch.object(
                    osm_pbf_loader.urllib.request,
                    "urlretrieve",
                    side_effect=write_fresh_download,
                ),
            ):
                osm_pbf_loader.download_osm_pbf(
                    url="https://example.test/region.osm.pbf",
                    target_path=pbf_path,
                    force=True,
                )

            with open(pbf_path, "rb") as target:
                self.assertEqual(target.read(), b"fresh")
            self.assertFalse(os.path.exists(temp_path))

    def test_platform_install_instructions(self):
        with patch.dict(
            os.environ,
            {"TERMUX_VERSION": "0.118", "PREFIX": "/data/data/com.termux/files/usr"},
        ):
            self.assertEqual(
                osm_pbf_loader._osmium_install_instructions(),
                "pkg install osmium-tool",
            )

        with (
            patch.dict(os.environ, {}, clear=True),
            patch.object(osm_pbf_loader.platform, "system", return_value="Windows"),
        ):
            self.assertEqual(
                osm_pbf_loader._osmium_install_instructions(),
                "conda install -c conda-forge osmium-tool",
            )

    def test_osmium_executable_can_be_overridden(self):
        configured_path = os.path.join("custom", "bin", "osmium")
        with (
            patch.dict(os.environ, {"OSMIUM_BIN": configured_path}),
            patch.object(
                osm_pbf_loader.shutil, "which", return_value=configured_path
            ) as which,
        ):
            self.assertEqual(
                osm_pbf_loader._find_osmium_executable(), configured_path
            )

        which.assert_called_once_with(configured_path)

    def test_missing_tool_fails_before_downloading_pbf(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            pbf_path = os.path.join(temp_dir, "region.osm.pbf")
            streets_path = os.path.join(temp_dir, "streets.pkl")
            boundaries_path = os.path.join(temp_dir, "boundaries.pkl")
            with (
                patch.object(osm_pbf_loader, "_find_osmium_executable", return_value=None),
                patch.object(
                    osm_pbf_loader,
                    "_osmium_install_instructions",
                    return_value="install osmium-tool",
                ),
                patch.object(osm_pbf_loader, "download_osm_pbf") as download,
            ):
                with self.assertRaisesRegex(RuntimeError, "install osmium-tool"):
                    osm_pbf_loader.extract_pbf_to_cache(
                        pbf_path, streets_path, boundaries_path
                    )

            download.assert_not_called()

    def test_existing_caches_do_not_require_osmium(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            streets_path = os.path.join(temp_dir, "streets.pkl")
            boundaries_path = os.path.join(temp_dir, "boundaries.pkl")
            for path in (streets_path, boundaries_path):
                with open(path, "wb"):
                    pass

            with patch.object(osm_pbf_loader, "_find_osmium_executable") as find_osmium:
                result = osm_pbf_loader.extract_pbf_to_cache(
                    os.path.join(temp_dir, "missing.osm.pbf"),
                    streets_path,
                    boundaries_path,
                )

            self.assertEqual(result, (streets_path, boundaries_path))
            find_osmium.assert_not_called()


if __name__ == "__main__":
    unittest.main()
