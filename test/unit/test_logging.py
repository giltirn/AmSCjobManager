"""Verify library diagnostics honor their configured output hooks."""

import io
import unittest
from unittest.mock import Mock, patch

from amscjobmanager import logging as manager_logging
from amscjobmanager.utils import cmdlinePrint


class LoggingTests(unittest.TestCase):
    def test_manager_and_api_logs_use_their_own_hooks(self):
        """Distinct callbacks receive the original positional and keyword arguments.

        Replace both hooks, emit manager/API messages with print-style options,
        and verify exact forwarding. In particular, end/flush must be passed
        as keyword arguments, rather than being expanded as positional strings.

        Args:
            self (LoggingTests): unittest-created test instance; no
                caller-supplied parameters.

        Returns:
            None: Checks report failures through unittest assertions.
        """
        manager = Mock()
        api = Mock()
        with patch.object(manager_logging, 'wfman_log_func', manager), \
             patch.object(manager_logging, 'api_log_func', api):
            manager_logging.wfmanLog('workflow', 1, sep=':', end='!', flush=True)
            manager_logging.wfapiLog('transfer', 2, sep='/', end='.', flush=True)
        manager.assert_called_once_with('workflow', 1, sep=':', end='!', flush=True)
        api.assert_called_once_with('transfer', 2, sep='/', end='.', flush=True)

    def test_default_output_hook_honors_print_options(self):
        """The default sink writes to a supplied stream with the requested formatting.

        Use an in-memory stream and explicit file/sep/end options to check the
        final cmdlinePrint adapter, independently of the hook dispatch layer.

        Args:
            self (LoggingTests): unittest-created test instance; no
                caller-supplied parameters.

        Returns:
            None: Checks report failures through unittest assertions.
        """
        output = io.StringIO()
        cmdlinePrint('one', 'two', file=output, sep=':', end='!', flush=True)
        self.assertEqual(output.getvalue(), 'one:two!')


if __name__ == '__main__':
    unittest.main()
