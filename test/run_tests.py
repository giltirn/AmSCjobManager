"""Run offline test modules in isolated processes; live scripts are never imported."""

import argparse
import os
from pathlib import Path
import subprocess
import sys


def create_parser() -> argparse.ArgumentParser:
    """Build the offline test CLI; take no inputs and return ArgumentParser.

    The optional suite selects unit, integration, or both. Filename patterns,
    listing, and verbosity narrow or describe the run without adding live tests.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('suite', nargs='?', choices=('unit', 'integration', 'all'), default='unit',
                        help='Offline test group (default: unit); all runs unit and integration tests')
    parser.add_argument('--pattern', default='test_*.py', help='Test filename glob (default: test_*.py)')
    parser.add_argument('--list', action='store_true', help='List matching modules without importing or running them')
    parser.add_argument('-v', '--verbose', action='store_true', help='Show individual unittest names')
    return parser


def main(argv: list[str] | None = None) -> int:
    """Select offline modules and run each in a fresh Python process.

    argv (list[str] | None) supplies CLI arguments without the program name;
    None reads sys.argv. Return 0 for successful runs/listing, 1 if any module
    fails; argparse rejects invalid inputs with exit status 2. Child processes
    run from the repository root with test/ on PYTHONPATH. Their default API
    backend is IRI; SPOOF modules select their own backend before library imports.
    No live scripts are selected or imported. Output and errors remain visible.
    """
    parser = create_parser()
    args = parser.parse_args(argv)
    test_root = Path(__file__).resolve().parent
    repository = test_root.parent

    # Select only the two automated trees; listing must have no import effects.
    groups = ('unit', 'integration') if args.suite == 'all' else (args.suite,)
    paths = [path for group in groups for path in sorted((test_root / group).rglob('test_*.py'))
             if path.match(args.pattern)]
    if not paths:
        parser.error(f'No {args.suite} test modules match {args.pattern!r}')
    if args.list:
        for path in paths:
            print(path.relative_to(repository))
        return 0

    # Per-module isolation prevents backend selection and global test fixtures
    # from leaking into unrelated suites, including subprocess recovery tests.
    environment = os.environ.copy()
    environment['PYTHONPATH'] = str(test_root) + (
        os.pathsep + environment['PYTHONPATH'] if environment.get('PYTHONPATH') else '')
    environment['AMSC_JOB_MANAGER_API_IMPL'] = 'IRI'
    failures = []
    for index, path in enumerate(paths, start=1):
        module = '.'.join(path.relative_to(test_root).with_suffix('').parts)
        command = [sys.executable, '-m', 'unittest', module]
        if args.verbose:
            command.append('-v')
        print(f'[{index}/{len(paths)}] {path.relative_to(repository)}', flush=True)
        completed = subprocess.run(command, cwd=repository, env=environment)
        if completed.returncode:
            failures.append(module)

    # Preserve every module failure while letting independent coverage finish.
    if failures:
        print(f'FAILED: {len(failures)}/{len(paths)} modules: ' + ', '.join(failures), file=sys.stderr)
        return 1
    print(f'PASSED: {len(paths)} offline test modules', flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
