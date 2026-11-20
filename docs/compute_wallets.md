# Compute node-hour wallets

Wallets optionally limit the compute resources that workflows can reserve on
individual machines. Balances persist in the manager's SQLite database and are
owned by its compute action manager. They apply to both executable and batch
script compute actions, including the local target. Transfers and API stages
are not charged.

A machine without a wallet has no budget restriction. Wallets cannot be
initialized from the configuration file. Enabling a new wallet starts it at zero;
recharge it explicitly to authorize work.

## Administration and inspection

Start the server with its local admin socket, as described in the
[server guide](server.md#local-administration). On the server host:

```bash
python utilities/manager_admin.py ./admin.sock enable_wallet frontier
python utilities/manager_admin.py ./admin.sock recharge_wallet frontier 100
python utilities/manager_admin.py ./admin.sock disable_wallet frontier
```

Command names, inputs, and help come from the running server. Inspect them
without a password using `python utilities/manager_admin.py ./admin.sock -h`
or append a command name for detailed help. Wallet definitions are owned by
`ComputeActions`; its administrative operations are not MCP tools.

Every command prompts privately for the server's admin password. These mutations
are absent from MCP. Recharge adds the specified node-hours; **do not blindly
repeat a recharge after a lost response**, since it may already have succeeded.
Enable and disable are safe to repeat.

Read balances through MCP or its generated CLI command:

```bash
python utilities/manager_client.py list_compute_wallets
python utilities/manager_client.py list_compute_wallets --machine frontier
```

Each machine record contains:

- `enabled`: whether new compute admissions require funds.
- `available_node_hours`: funds available after outstanding reservations.
- `reserved_node_hours`: the full reservations awaiting settlement.
- `updated_at`: Unix seconds when the wallet last changed.

Machine names are case-insensitive. An empty result means no matching wallets,
not a zero balance. Disabling bypasses budgeting for new admissions; it does not
release existing reservations. Enabling applies to work not yet accepted by the
compute provider, including stages of already submitted workflows and submission
retries. Disabled wallets create no zero-cost accounting records or exemptions.
Jobs already accepted without a reservation are not charged retroactively.

For direct Python use, retrieve the compute manager through `JobData.getManager`:

```python
from amscjobmanager.actions_base import ActionClass

with manager as jobs:
    computes = jobs.getManager(ActionClass.COMPUTE)
    computes.setWalletEnabled("frontier", True)
    computes.rechargeWallet("frontier", 100)
    wallets = computes.getWallets("frontier")
```

The direct Python interface trusts its caller. Password enforcement belongs to
the server's separate admin route.

## Reservations, waits, and refunds

Before a submission attempt, the manager reserves
`nodes * requested_walltime_seconds / 3600` node-hours from the machine wallet.
Balances, requests, reservations, and estimated usage are stored as SQLite
`REAL` values in node-hours, with constraints rejecting negative amounts.
Recharge accepts any finite positive node-hour amount, without rounding to whole
seconds.
`nodes` is a positive, keyword-only attribute inherited from `ComputeActionBase`.

If funds are insufficient, the action stays `SCHEDULING` and reports
`api_status="INSUFFICIENT_WALLET_BALANCE"`. The overall workflow remains `ACTIVE`.
No submission attempt is consumed and no remote job is submitted. After a
recharge or disabling the wallet, the worker rechecks on its normal scheduling
interval, clears the reason, and proceeds. Once known, the ordinary provider
status replaces it. The reason is visible through `headActionStatus` with
`return_api_status=True`, MCP `list_workflows`, and the CLI.

Reservations are recorded before contacting the provider and reused across
retries and restarts. An uncertain submission retains its reservation while the
manager searches for the remote job by name. If the manager establishes that no
job was accepted and stops trying, or cancels before submission, it refunds the
whole reservation.

When remote work completes, fails, or is canceled, the manager estimates usage
and refunds the unused balance once. It records settlement in the same
transaction as the terminal action state, so recovery cannot refund twice.
Successful cancellation acknowledgment ends estimated execution; the manager
does not add a separate terminal-state polling phase. Failed cancellation or
status calls retain the reservation and are retried normally.

## Estimation limits

These are protective estimates, not scheduler billing records. Repeated provider
`new`, `queued`, or `held` observations do not consume node-hours. Provider
`active` means execution. Intervals that cross into or out of execution are
charged conservatively, including uncertainty before the first observation and
between polls. A job that starts and finishes between polls may therefore be
charged for the entire intervening interval. Downtime after an execution
observation remains chargeable on recovery. Charges are capped at the reserved
request; actual usage can differ because exact scheduler timestamps are not
available.

An unexpected local worker failure does not prove that remote work stopped, so
it does not release its reservation. Stopping the manager also leaves remote jobs
and reservations intact for recovery. The wallet controls submissions made by
this manager, not direct scheduler submissions or arbitrary commands launched
outside its compute-action interface.
