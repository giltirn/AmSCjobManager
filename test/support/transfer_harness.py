"""Deterministic remote transfer fixtures shared by workflow recovery tests."""

from dataclasses import dataclass

from amscjobmanager.actions_base import TransferActionBase
from amscjobmanager.globus_transfer_action import GlobusDataTransfers
from support.action_manager_helpers import DirectAsyncActions


class TransferHarness:
    """Small, deterministic model of the remote transfer service."""

    def __init__(self):
        """Initialize empty remote-ID/status maps and zeroed operation/failure counters; return None."""
        self.by_name = {}
        self.status_by_api_key = {}
        self.submission_count = 0
        self.failed_submission_count = 0
        self.failed_status_lookup_count = 0
        self.failed_remote_lookup_count = 0
        self.hidden_remote_lookup_count = 0
        self.failed_cancellation_count = 0
        self.cancellation_count = 0

    def submit(self, name):
        """Accept name (str | None), record a simulated transfer, and return its ID (str).

        Consume one configured failed_submission_count and raise instead when positive.
        """
        if self.failed_submission_count:
            self.failed_submission_count -= 1
            raise RuntimeError("simulated remote submission failure")
        self.submission_count += 1
        api_key = f"transfer-{self.submission_count}"
        self.by_name[name] = api_key
        self.status_by_api_key[api_key] = "ACTIVE"
        return api_key

    def find(self, name):
        """Find name (str | None); return its transfer ID (str) or None if absent/hidden.

        Consume configured lookup-failure or hidden-result counters before reading the map.
        """
        if self.failed_remote_lookup_count:
            self.failed_remote_lookup_count -= 1
            raise RuntimeError("simulated remote action lookup failure")
        if self.hidden_remote_lookup_count:
            self.hidden_remote_lookup_count -= 1
            return None
        return self.by_name.get(name)

    def status(self, api_key):
        """Read status (str) for api_key (str), consuming a configured lookup failure if set."""
        if self.failed_status_lookup_count:
            self.failed_status_lookup_count -= 1
            raise RuntimeError("simulated remote status lookup failure")
        return self.status_by_api_key[api_key]

    def cancel(self, api_key):
        """Mark api_key (str) FAILED and count cancellation; return None, or consume a failure."""
        if self.failed_cancellation_count:
            self.failed_cancellation_count -= 1
            raise RuntimeError("simulated remote cancellation failure")
        self.cancellation_count += 1
        self.status_by_api_key[api_key] = "FAILED"


@dataclass
class HarnessGlobusTransfer(TransferActionBase):
    """Transfer action whose remote side effect is owned by the test harness."""

    harness = None

    def initiateAction(self, workflow_id, name=None):
        """Submit name (str | None) to the harness; return its transfer ID (str).

        workflow_id (int) is the owning workflow database ID, unused by this fixture.
        """
        return self.harness.submit(name)

    def getInfo(self):
        """Return an empty metadata dict; take no arguments and perform no remote work."""
        return {}


@dataclass
class RaisingHarnessGlobusTransfer(HarnessGlobusTransfer):
    """Action-layer failure used to verify ActionManager exception handling."""

    def initiateAction(self, workflow_id, name=None):
        """Raise a submission failure without accepting work; workflow_id (int) and name are unused."""
        raise RuntimeError("simulated action-layer submission failure")


@dataclass
class AcceptedThenRaisingHarnessGlobusTransfer(HarnessGlobusTransfer):
    """Simulate a response failure after the remote service accepts work."""

    def initiateAction(self, workflow_id, name=None):
        """Accept name (str | None), then raise instead of returning its provider ID.

        workflow_id (int) identifies the owning workflow and is unused by the harness.
        """
        self.harness.submit(name)
        raise RuntimeError("simulated response failure after acceptance")


class HarnessGlobusDataTransfers(DirectAsyncActions, GlobusDataTransfers):
    """The real transfer manager with only its remote operations overridden."""

    def __init__(self, connection, harness, scheduling_retry_delay=0, **kwargs):
        """Create deterministic transfer supervision with an injected remote harness.

        connection (LockedConnection) owns SQLite storage; harness (TransferHarness)
        supplies remote operations. scheduling_retry_delay (float) is seconds
        between retries; kwargs are forwarded manager settings. Return None.
        """
        self.harness = harness
        super().__init__(
            connection, scheduling_retry_delay=scheduling_retry_delay, **kwargs
        )

    def _queryStatusInternal(self, machine, api_key):
        """Return harness status (str) for api_key (str); machine (str) is unused."""
        return self.harness.status(api_key)

    def _findRemoteActionByName(self, machine, name):
        """Find name (str) in the harness; return ID (str | None). machine (str) is unused."""
        return self.harness.find(name)

    def _cancelActionInternal(self, machine, api_key):
        """Cancel api_key (str) in the harness; machine (str) is unused. Return None."""
        self.harness.cancel(api_key)


