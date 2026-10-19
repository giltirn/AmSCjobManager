"""Offline checks for the local username command."""

import subprocess
import unittest
from unittest.mock import patch

from amscjobmanager.local_api import localUsername


class LocalUsernameTests(unittest.TestCase):
    def test_runs_whoami_and_strips_output(self):
        with patch("amscjobmanager.local_api.subprocess.run", return_value=subprocess.CompletedProcess(["whoami"], 0, "local-user\n", "")) as run:
            self.assertEqual(localUsername(), "local-user")
            run.assert_called_once_with(["whoami"], text=True, capture_output=True, check=True)

    def test_command_failure_is_not_silenced(self):
        with patch("amscjobmanager.local_api.subprocess.run", side_effect=subprocess.CalledProcessError(1, ["whoami"])), self.assertRaises(subprocess.CalledProcessError):
            localUsername()


if __name__ == "__main__":
    unittest.main()
