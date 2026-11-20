"""Compute-job supervision and persistent machine-specific node-hour budgets."""

import math
import sqlite3
import time
from typing import Literal

from pydantic import BaseModel, Field, StrictStr

from .action_manager import ActionStatus, _unser
from .async_action_manager import AsyncActionManager
from .api_general import cancelJob, getJobState, findJobByName
from .logging import wfmanLog

from .manager_functions import (
    ConfiguredMachineArguments, EmptyFunctionResult, FunctionArguments, FunctionDefinition,
)


class WalletFilter(FunctionArguments):
    """Optional machine (str | None) filters wallet names case-insensitively; None lists all."""

    machine: StrictStr | None = Field(default=None, description='Optional case-insensitive machine filter; omit for all recorded wallets. Missing wallets impose no limit.')


class WalletRechargeArguments(ConfiguredMachineArguments):
    """Required machine (str) and node_hours (finite positive number) specify an additive grant."""

    node_hours: float = Field(gt=0, allow_inf_nan=False, description='Positive node-hour grant; additive, do not blindly retry after a lost response.')


class ComputeWalletSummary(BaseModel):
    """One recorded machine budget, expressed in node-hours.

    enabled (bool) controls future reservations; available_node_hours (float)
    excludes reserved funds. reserved_node_hours (float) sums unsettled full
    reservations. updated_at (int) is Unix seconds for the last wallet mutation.
    """

    enabled: bool = Field(description='Whether new compute admissions require funds; missing wallets are unrestricted.')
    available_node_hours: float = Field(description='Unreserved machine balance, in node-hours.')
    reserved_node_hours: float = Field(description='Full unsettled compute reservations, in node-hours.')
    updated_at: int = Field(description='Last wallet mutation as Unix time in whole seconds.')


class ComputeWalletListing(BaseModel):
    """Wallets keyed by case-insensitive machine name; missing means unrestricted.

    result (dict[str, ComputeWalletSummary]) is empty when no wallets match.
    """

    result: dict[str, ComputeWalletSummary]


# Use ActionStatus names on the wire; its enum values are internal integers.
ComputeActionStatusName = Literal[tuple(status.name for status in ActionStatus)]


class ComputeActionFilter(FunctionArguments):
    """Optional AND-combined filters for records owned by the compute manager.

    action_id/workflow_id (positive int | None) are database IDs, not list/stage
    positions. machine (str | None) matches case-insensitively; api_status
    (str | None) matches exactly, preserving provider casing or manager reasons.
    statuses (list[ActionStatus name] | None) uses OR; None means all and [] none.
    No filter causes provider polling. Missing/unmatched records yield an empty list.
    """

    action_id: int | None = Field(default=None, gt=0, strict=True, description='Optional positive action database ID, not a stage/list index; unmatched IDs return an empty list.')
    workflow_id: int | None = Field(default=None, gt=0, strict=True, description='Optional positive owning workflow database ID; independent of action IDs.')
    machine: StrictStr | None = Field(default=None, description='Optional case-insensitive machine name; unknown machines return an empty list.')
    statuses: list[ComputeActionStatusName] | None = Field(default=None, description='Recorded action-status names, combined using OR; omit for all, [] for none. Includes SCHEDULING and PROVIDER_HELD. Combined with other filters using AND.')
    api_status: StrictStr | None = Field(default=None, description='Exact, case-sensitive provider status or manager reason, for example active or INSUFFICIENT_WALLET_BALANCE. Omit for all, including unknown/null statuses.')


class ComputeActionSummary(BaseModel):
    """Recorded compute identity, request, and execution state, without executable/script contents.

    action_id/workflow_id (int) are independent positive database IDs. machine,
    action_type, action_status (str) identify target, definition class, and stored
    ActionStatus name. api_key/api_status (str | None) are a provider job ID (not
    a credential) and provider status or manager reason; None means not recorded.
    nodes (int), time (str, requested walltime seconds), account/queue (str) are
    common request fields; empty account/queue may be valid for local execution.
    last_update/last_submission_attempt (int Unix seconds; latter nullable) are
    manager timestamps. submission_attempts (int) counts attempted API submissions;
    last_submission_error (str | None) is its most recent recorded diagnostic.
    """

    action_id: int = Field(description='Positive global action database ID; not a workflow ID or stage/list position.')
    workflow_id: int = Field(description='Positive database ID of the owning workflow.')
    machine: str = Field(description='Target machine as recorded on the action.')
    action_type: str = Field(description='Stored compute definition class name, for example ExecuteJobComputeAction.')
    action_status: ComputeActionStatusName = Field(description='Recorded ActionStatus name; SCHEDULING includes wallet waits, ACTIVE includes provider-queued work.')
    api_key: str | None = Field(description='Recorded provider job identifier, not an authentication token; null before an ID is recovered or accepted.')
    api_status: str | None = Field(description='Recorded provider status with original casing, or manager reason such as INSUFFICIENT_WALLET_BALANCE; null if unknown.')
    nodes: int = Field(description='Requested number of compute nodes.')
    time: str = Field(description='Requested walltime in seconds, retained as the supplied string.')
    account: str = Field(description='Requested project/account; may be empty for local execution.')
    queue: str = Field(description='Requested scheduler queue; may be empty for local execution.')
    last_update: int = Field(description='Recorded manager timestamp as whole Unix seconds; not a provider execution-start time.')
    submission_attempts: int = Field(description='Number of attempted API submissions. Wallet waits do not consume attempts.')
    last_submission_attempt: int | None = Field(description='Last recorded submission-attempt time as whole Unix seconds, or null before any attempt.')
    last_submission_error: str | None = Field(description='Most recent recorded submission/status diagnostic, or null if none is retained.')


class ComputeActionListing(BaseModel):
    """result (list[ComputeActionSummary]) contains matching records ordered by action_id.

    Includes SCHEDULING, running/provider-queued/held, canceling and terminal
    manager records. Future workflow stages that have not reached this manager
    are absent. An empty result means no matches. No API calls or state changes occur.
    """

    result: list[ComputeActionSummary]


class ComputeActions(AsyncActionManager):
    """Manage compute jobs and optionally reserve their cost against machine wallets."""

    terminal_api_statuses = {"completed", "failed", "canceled"}

    def __init__(self, connection, **kwargs):
        """Initialize compute execution and wallet tables without enabling any wallet.

        connection (LockedConnection) is the shared database owner. kwargs are
        AsyncActionManager supervision settings. Return None. Existing balances,
        reservations and settlements survive reopening; no configuration grants
        funds. Missing wallets impose no budget restriction.
        """
        # Provider-held compute jobs wait in the queue and do not occupy nodes.
        # TODO: Live-test Slurm dependency jobs to verify held/queued reporting.
        smap = {"new": ActionStatus.ACTIVE, "queued": ActionStatus.ACTIVE,
                "active": ActionStatus.ACTIVE, "held": ActionStatus.PROVIDER_HELD,
                "completed": ActionStatus.COMPLETED, "failed": ActionStatus.FAILED,
                "canceled": ActionStatus.CANCELED}
        super().__init__(connection, "computes", smap, **kwargs)
        with self.conn as conn:
            conn.execute('''CREATE TABLE IF NOT EXISTS compute_wallets (
                machine TEXT PRIMARY KEY,
                enabled INTEGER NOT NULL CHECK(enabled IN (0, 1)),
                available_node_hours REAL NOT NULL CHECK(available_node_hours >= 0),
                updated_at INTEGER NOT NULL
            )''')
            conn.execute('''CREATE TABLE IF NOT EXISTS compute_wallet_actions (
                action_id INTEGER PRIMARY KEY REFERENCES computes(action_id),
                machine TEXT NOT NULL,
                nodes INTEGER NOT NULL CHECK(nodes > 0),
                requested_node_hours REAL NOT NULL CHECK(requested_node_hours > 0),
                reserved_node_hours REAL NOT NULL CHECK(reserved_node_hours >= 0),
                used_node_hours REAL NOT NULL DEFAULT 0 CHECK(used_node_hours >= 0),
                observed_at INTEGER,
                provider_status TEXT,
                settled INTEGER NOT NULL DEFAULT 0 CHECK(settled IN (0, 1))
            )''')

    def getMCPFunctions(self) -> tuple[FunctionDefinition, ...]:
        """Describe read-only compute action and wallet inspection.

        Take no arguments; return tuple[FunctionDefinition, ...] without querying state.
        """
        def _list_wallets(manager: ComputeActions, args: WalletFilter) -> ComputeWalletListing:
            """Read manager's wallets selected by validated args.machine; return the listing model."""
            return ComputeWalletListing(result=manager.getWallets(args.machine))

        def _list_actions(manager: ComputeActions, args: ComputeActionFilter) -> ComputeActionListing:
            """Query manager-owned compute records using validated args; return their summaries."""
            return manager.listActiveAndPastComputeActions(args)

        return (
            FunctionDefinition(
                'list_compute_wallets',
                'List persistent compute wallet balances for all machines or one selected machine. Missing wallets impose no limit.',
                _list_wallets, read_only=True, idempotent=True,
            ),
            FunctionDefinition(
                'list_active_and_past_compute_actions',
                'List recorded compute actions, including scheduling/wallet waits, provider-queued or running work, and terminal history. Excludes future unstarted workflow stages; reads cached state without polling.',
                _list_actions, read_only=True, idempotent=True,
            ),
        )

    def listActiveAndPastComputeActions(self, filters: ComputeActionFilter | None = None) -> ComputeActionListing:
        """Read matching compute-manager records without polling or progressing work.

        filters (ComputeActionFilter | None) supplies optional ID, machine,
        action-status and API-status filters; None selects all records. Return
        ComputeActionListing with result ordered by action_id, as documented by
        ComputeActionSummary. Unknown IDs/machines or unmatched API-status values yield no matches.
        Read this manager's table only; unstarted future stages are excluded.
        Call under WorkflowManager's lock while it runs. No state or provider changes occur.
        """
        if filters is None:
            filters = ComputeActionFilter()
        if filters.statuses == []:
            return ComputeActionListing(result=[])

        # Filter the owned execution table before deserializing any definitions.
        conditions, parameters = [], []
        for column, value in (('action_id', filters.action_id), ('workflow_id', filters.workflow_id),
                              ('api_status', filters.api_status)):
            if value is not None:
                conditions.append(f'{column} = ?')
                parameters.append(value)
        if filters.machine is not None:
            conditions.append('LOWER(machine) = ?')
            parameters.append(filters.machine.lower())
        if filters.statuses is not None:
            conditions.append('action_status IN (' + ','.join('?' for _ in filters.statuses) + ')')
            parameters.extend(filters.statuses)
        query = 'SELECT * FROM computes'
        if conditions:
            query += ' WHERE ' + ' AND '.join(conditions)
        query += ' ORDER BY action_id'
        with self.conn as conn:
            rows = conn.execute(query, parameters).fetchall()

        # Expose common compute metadata and diagnostics, rather than serialized
        # definitions or executable, shell-script, and argument contents.
        summaries = []
        for row in rows:
            action = _unser(row['details'])
            summaries.append(ComputeActionSummary(
                action_id=row['action_id'], workflow_id=row['workflow_id'], machine=row['machine'],
                action_type=type(action).__name__, action_status=row['action_status'],
                api_key=row['api_key'], api_status=row['api_status'], nodes=action.nodes,
                time=action.time, account=action.account, queue=action.queue,
                last_update=row['last_update'], submission_attempts=row['submission_attempts'],
                last_submission_attempt=row['last_submission_attempt'],
                last_submission_error=row['last_submission_error'],
            ))
        return ComputeActionListing(result=summaries)

    def getAdminFunctions(self) -> tuple[FunctionDefinition, ...]:
        """Describe password-protected wallet administration commands.

        Take no arguments; return definitions whose local callables accept a
        ComputeActions instance and validated input, without capturing self.
        """
        def _enable_wallet(manager: ComputeActions, args: ConfiguredMachineArguments) -> EmptyFunctionResult:
            """Enable manager's wallet for args.machine; return an empty acknowledgment model."""
            manager.setWalletEnabled(args.machine, True)
            return EmptyFunctionResult()

        def _disable_wallet(manager: ComputeActions, args: ConfiguredMachineArguments) -> EmptyFunctionResult:
            """Disable manager's wallet for args.machine; return an empty acknowledgment model."""
            manager.setWalletEnabled(args.machine, False)
            return EmptyFunctionResult()

        def _recharge_wallet(manager: ComputeActions, args: WalletRechargeArguments) -> EmptyFunctionResult:
            """Grant args.node_hours to manager's args.machine wallet; return an empty result model."""
            manager.rechargeWallet(args.machine, args.node_hours)
            return EmptyFunctionResult()

        return (
            FunctionDefinition('enable_wallet', 'Enable compute budgeting for a machine; a new wallet starts at zero.',
                               _enable_wallet, idempotent=True, success_message='Compute wallet enabled.'),
            FunctionDefinition('disable_wallet', 'Disable compute budgeting for new submissions while retaining existing reservations.',
                               _disable_wallet, idempotent=True, success_message='Compute wallet disabled.'),
            FunctionDefinition('recharge_wallet', 'Add node-hours to a persistent compute wallet without changing whether it is enabled.',
                               _recharge_wallet, success_message='Compute wallet recharged.'),
        )

    def setWalletEnabled(self, machine: str, enabled: bool) -> None:
        """Enable or disable submission budgeting for one machine, without changing funds.

        machine (nonempty str) is case-insensitive. enabled (bool) controls future
        reservations. A new wallet starts at zero. Return None. Disabling does
        not release existing reservations; their unused funds still settle.
        Privileged server callers must authenticate before invoking this method.
        """
        # Validate the public request before modifying durable wallet state.
        if not isinstance(machine, str) or not machine.strip():
            raise ValueError('Wallet machine must be a nonempty string')
        if not isinstance(enabled, bool):
            raise ValueError('Wallet enabled must be a bool')
        machine = machine.lower()

        # Preserve any existing balance while changing the admission policy.
        # ON CONFLICT here means that if the new row's key (machine) already exists, just update the enabled and updated_at columns
        with self.conn as conn:
            conn.execute('''INSERT INTO compute_wallets VALUES (?, ?, 0, ?)
                ON CONFLICT(machine) DO UPDATE SET enabled = excluded.enabled,
                updated_at = excluded.updated_at''', (machine, enabled, int(time.time())))

    def rechargeWallet(self, machine: str, node_hours: float) -> None:
        """Add a positive node-hour grant to one persistent machine wallet.

        machine (nonempty str) is case-insensitive. node_hours (finite positive
        int | float) is stored directly as floating-point node-hours, without
        rounding to whole seconds. Return None. A missing wallet is
        created disabled. Recharging is additive, not idempotent: callers must
        not blindly retry after an uncertain response. No running jobs change.
        Privileged server callers must authenticate before invoking this method.
        """
        # Validate the public request before modifying durable wallet state.
        if not isinstance(machine, str) or not machine.strip():
            raise ValueError('Wallet machine must be a nonempty string')
        if (isinstance(node_hours, bool) or not isinstance(node_hours, (int, float))
                or not math.isfinite(node_hours) or node_hours <= 0):
            raise ValueError('Node-hours must be a finite positive number')
        node_hours = float(node_hours)
        machine = machine.lower()

        # Leave headroom for every outstanding reservation to be refunded.
        with self.conn as conn:
            row = conn.execute('''SELECT available_node_hours + COALESCE((SELECT SUM(reserved_node_hours)
                FROM compute_wallet_actions WHERE machine = ? AND settled = 0), 0.0)
                FROM compute_wallets WHERE machine = ?''', (machine, machine)).fetchone()
            if row is not None and not math.isfinite(row[0] + node_hours):
                raise ValueError('Wallet balance must remain finite')
            conn.execute('''INSERT INTO compute_wallets VALUES (?, 0, ?, ?)
                ON CONFLICT(machine) DO UPDATE SET
                available_node_hours = available_node_hours + excluded.available_node_hours,
                updated_at = excluded.updated_at''', (machine, node_hours, int(time.time())))

    def getWallets(self, machine: str | None = None) -> dict:
        """Return recorded wallet balances for all machines or one selected target.

        machine (str | None) filters case-insensitively; None lists all wallets.
        Return dict[str, dict] keyed by machine, empty for an unrecorded target.
        Each value contains enabled (bool), available_node_hours (float, funds
        after reservations), reserved_node_hours (float, unsettled reservations),
        and updated_at (int Unix seconds). Missing wallets mean no enforcement.
        No provider call, mutation or credential is required.
        """
        if machine is not None and (not isinstance(machine, str) or not machine.strip()):
            raise ValueError('Wallet machine must be a nonempty string or None')

        # Aggregate reservations without exposing mutable owner state.
        with self.conn as conn:
            query = '''SELECT w.*, COALESCE((SELECT SUM(reserved_node_hours) FROM
                compute_wallet_actions a WHERE a.machine = w.machine AND a.settled = 0), 0.0)
                AS reserved FROM compute_wallets w'''
            rows = conn.execute(query + (' WHERE w.machine = ?' if machine is not None else '') + ' ORDER BY w.machine',
                                () if machine is None else (machine.lower(),)).fetchall()
            return {row['machine']: {'enabled': bool(row['enabled']),
                    'available_node_hours': row['available_node_hours'],
                    'reserved_node_hours': row['reserved'],
                    'updated_at': row['updated_at']} for row in rows}

    def _authorizeSubmission(self, action_id, action) -> bool:
        """Authorize submission by reserving its cost, or record a visible wallet wait.

        action_id (int) is the compute database ID; action (ComputeActionBase)
        supplies machine, positive nodes, and walltime seconds (str). Return
        True if budgeting is disabled, funds are already reserved, or the
        current wallet can cover the request; otherwise return False.
        Admission and balance debit share one transaction. Unfunded actions stay
        SCHEDULING without a submission attempt. A disabled or missing wallet
        permits submission without creating an accounting record. Enabling a
        wallet applies to subsequent attempts that have no existing reservation.
        """
        # Compute the maximum possible reservation from the common resource fields.
        machine = action.machine.lower()
        seconds = int(action.time)
        cost = seconds * action.nodes / 3600 #IRI times are in seconds
        if cost <= 0 or not math.isfinite(cost):
            raise ValueError('Requested node-hours must be finite and positive')
        with self.conn as conn:
            # Reuse a reservation after a crash or uncertain acceptance;
            # never debit twice for the same action.
            existing = conn.execute('SELECT 1 FROM compute_wallet_actions WHERE action_id = ?', (action_id,)).fetchone()

            #Sanity check that the action is in SCHEDULING state
            state = conn.execute('SELECT action_status FROM computes WHERE action_id = ?', (action_id,)).fetchone()
            if state[0] != ActionStatus.SCHEDULING.name:
                return False

            wallet = conn.execute('SELECT enabled, available_node_hours FROM compute_wallets WHERE machine = ?', (machine,)).fetchone()
            if existing is None and wallet is not None and wallet['enabled']:
                # Apply the current wallet policy to an unreserved attempt.
                if wallet['available_node_hours'] < cost:
                    conn.execute("UPDATE computes SET api_status = 'INSUFFICIENT_WALLET_BALANCE' WHERE action_id = ?", (action_id,))
                    return False

                conn.execute('UPDATE compute_wallets SET available_node_hours = available_node_hours - ?, updated_at = ? WHERE machine = ?',
                             (cost, int(time.time()), machine))
                conn.execute('''INSERT INTO compute_wallet_actions
                    (action_id, machine, nodes, requested_node_hours, reserved_node_hours)
                    VALUES (?, ?, ?, ?, ?)''', (action_id, machine, action.nodes, cost, cost))
                self._maybeSimulateCrash('wallet_reservation_recorded')

            # If the attempt was previously rejected it would be marked with INSUFFICIENT_WALLET_BALANCE. It has now been accepted (user has recharged wallet or disabled it)
            # so we can clear this flag
            conn.execute("UPDATE computes SET api_status = NULL WHERE action_id = ? AND api_status = 'INSUFFICIENT_WALLET_BALANCE'", (action_id,))
        self._maybeSimulateCrash('wallet_reserved')
        return True

    def _recordStatusObservation(self, conn: sqlite3.Connection, action_id, api_status, *,
                                 finished=False, remote_work_submitted=False) -> None:
        """Estimate occupied node-hours and settle unused funds on remote termination.

        conn (sqlite3.Connection) is the caller's locked transaction; action_id
        (int) identifies the compute action. api_status (str | None) is the raw
        provider state. finished (bool) means completion/failure or acknowledged
        cancellation. remote_work_submitted (bool, default False) means a remote
        job was accepted, as established by its provider ID; False refunds the
        full reservation when finished is True. Return None. Settlement is atomic with the caller's
        action-state write, including after crashes. Previously settled actions
        and jobs submitted without a reservation are not charged here.
        """
        # Repeated recovery observations cannot charge or refund settled work.
        record = conn.execute('SELECT * FROM compute_wallet_actions WHERE action_id = ?', (action_id,)).fetchone()
        if record is None or record['settled']:
            return
        now = int(time.time())
        used = record['used_node_hours']
        previous_time = record['observed_at']
        # Without a previous observation, use the recorded submission-attempt
        # time. This can also be None if a crash occurred after reserving funds
        # but before recording an attempt, followed by cancellation on recovery.
        if previous_time is None:
            submission = conn.execute('SELECT last_submission_attempt FROM computes WHERE action_id = ?', (action_id,)).fetchone()
            previous_time = submission[0]

        # Charge uncertain transition intervals conservatively, capped at the
        # reservation. Repeated new/queued/held observations cost nothing.
        # Downtime after a running observation remains chargeable on recovery.
        if previous_time is not None and (record['provider_status'] == 'active'
                                         or api_status == 'active' or finished):
            used = min(record['reserved_node_hours'], used + max(0, now - previous_time) * record['nodes'] / 3600)

        # Publish usage and any refund within the caller's status transaction.
        if not remote_work_submitted:
            used = 0.0
        if finished:
            refund = record['reserved_node_hours'] - used
            conn.execute('UPDATE compute_wallets SET available_node_hours = available_node_hours + ?, updated_at = ? WHERE machine = ?',
                         (refund, now, record['machine']))
        conn.execute('''UPDATE compute_wallet_actions SET used_node_hours = ?, observed_at = ?,
            provider_status = ?, settled = ? WHERE action_id = ?''',
                     (used, now, api_status, int(finished), action_id))
        if finished:
            self._maybeSimulateCrash('wallet_settlement_recorded')
            if record['reserved_node_hours']:
                wfmanLog(f'Compute wallet settled for action {action_id}: estimated {used:g} node-hours; refunded {record["reserved_node_hours"] - used:g}')

    def _queryStatusInternal(self, machine, api_key):
        """Return provider job state (str) for machine (str) and api_key (str)."""
        return getJobState(machine, api_key)

    def _findRemoteActionByName(self, machine, name):
        """Return provider job ID (str | None) for machine/name (str), None if absent."""
        return findJobByName(machine, name)

    def _cancelActionInternal(self, machine, api_key):
        """Cancel machine's provider job; machine/api_key are str. Return None."""
        cancelJob(machine, api_key)
