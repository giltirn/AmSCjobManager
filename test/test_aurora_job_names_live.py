"""Compare colon and underscore job names against Aurora's live IRI API.

Run manually with an Aurora-enabled manager configuration and your allocation:

    python test/test_aurora_job_names_live.py manager-config.json --account PROJECT

The test expects ``amscjm:1`` to be rejected specifically for an invalid job
name, then expects ``amscjm_1`` to be accepted. Both submissions otherwise use
identical parameters: one node, one rank, a five-minute limit, and /bin/date.
Known accepted jobs are canceled immediately; execution is not required.
Artifacts remain in a unique child of Aurora's first configured sandbox.
No production naming or retry logic is changed by this diagnostic.
"""

import argparse
from pathlib import PurePosixPath
import uuid


def main(argv=None) -> int:
    """Submit the two names using CLI settings and return zero if both expectations hold.

    argv optionally supplies command-line arguments. Load credentials and
    sandboxes through the manager configuration, print submission errors in
    full, and cancel every job whose submission returns an ID. Unexpected
    rejections, acceptance of the colon name, and cancellation errors fail
    the test. A submission with no returned ID cannot be canceled here.
    """
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("config", help="Manager configuration JSON with Aurora credentials and a sandbox")
    parser.add_argument("--account", required=True, help="Aurora project/allocation to charge")
    parser.add_argument("--queue", default="debug", help="Aurora queue (default: debug)")
    args = parser.parse_args(argv)

    from amscjobmanager import iri_api as api
    from amscjobmanager.manager_config import readManagerConfigFile

    # Use the normal configuration and sandbox checks, with an isolated run directory.
    config = readManagerConfigFile(args.config)
    sandboxes = api.setupWorkflowAgentFromConfig(config)
    if "aurora" not in api.getKnownMachines() or not sandboxes.get("aurora"):
        parser.error("Configuration must include Aurora and at least one Aurora sandbox directory")
    run_dir = str(PurePosixPath(sandboxes["aurora"][0]) / f"amsc-job-names-{uuid.uuid4().hex}")
    api.remoteMkdir("aurora", run_dir)
    print(f"Remote artifacts: aurora:{run_dir}", flush=True)

    # Change only the name, checking the actual server error rather than treating
    # authentication, allocation, or other unrelated failures as a passing result.
    colon_rejected = False
    for name in ("amscjm:1", "amscjm_1"):
        job_id = None
        print(f"Submitting name={name!r}", flush=True)
        try:
            try:
                job_id = api.executeJob(
                    "aurora", executable="/bin/date", arguments=[],
                    nodes=1, ranks_per_node=1, gpus_per_rank=0,
                    time="300", queue=args.queue, account=args.account,
                    job_run_dir=run_dir, name=name,
                )
            except Exception as exc:
                print(f"Rejected {name!r}: {type(exc).__name__}: {exc}", flush=True)
                if name != "amscjm:1" or "invalid job name" not in str(exc).lower() or name not in str(exc):
                    raise
                colon_rejected = True
            else:
                if not job_id:
                    raise AssertionError(f"Submission of {name!r} returned no job ID")
                print(f"Accepted {name!r}: job ID {job_id}", flush=True)
        finally:
            # Cancel accepted probes even when a later check fails, without waiting
            # for the queue to start them. Cancellation failures remain visible.
            if job_id is not None:
                api.cancelJob("aurora", job_id)
                print(f"Cancellation requested for {job_id}", flush=True)

    if not colon_rejected:
        raise AssertionError("Aurora accepted 'amscjm:1'; the reported name rejection was not reproduced")
    print("PASS: Aurora rejected 'amscjm:1' and accepted 'amscjm_1'", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
