#!/usr/bin/env python3
"""
Test runner for DeltaStream SDK tests.

Usage:
    python tests/sdk/run_tests.py              # Run all tests
    python tests/sdk/run_tests.py --unit       # Run unit tests only
    python tests/sdk/run_tests.py --client     # Run client tests only
    python tests/sdk/run_tests.py --models     # Run model tests only
    python tests/sdk/run_tests.py --resources  # Run resource tests only
"""

import sys
import subprocess
import argparse
from pathlib import Path


def run_tests(test_filter=None, verbose=False, coverage=False):
    """Run the SDK tests with optional filtering."""

    # Base pytest command
    cmd = ["python", "-m", "pytest"]

    # Add test directory
    cmd.append("tests/sdk/")

    # Add verbosity
    if verbose:
        cmd.append("-v")
    else:
        cmd.append("--tb=short")

    # Add coverage if requested
    if coverage:
        cmd.extend(["--cov=src/deltastream/sdk", "--cov-report=term-missing"])

    # Add test filter
    if test_filter:
        if test_filter == "unit":
            cmd.append("-m")
            cmd.append("unit")
        elif test_filter == "client":
            cmd.append("tests/sdk/test_client.py")
        elif test_filter == "models":
            cmd.append("tests/sdk/test_models.py")
        elif test_filter == "resources":
            cmd.append("tests/sdk/test_resources.py")
        elif test_filter == "exceptions":
            cmd.append("tests/sdk/test_exceptions.py")

    print(f"Running command: {' '.join(cmd)}")

    # Run the tests
    try:
        result = subprocess.run(cmd, cwd=Path(__file__).parent.parent.parent)
        return result.returncode
    except KeyboardInterrupt:
        print("\nTests interrupted by user")
        return 1
    except Exception as e:
        print(f"Error running tests: {e}")
        return 1


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Run DeltaStream SDK tests")

    parser.add_argument(
        "--unit",
        action="store_const",
        const="unit",
        dest="filter",
        help="Run unit tests only",
    )
    parser.add_argument(
        "--client",
        action="store_const",
        const="client",
        dest="filter",
        help="Run client tests only",
    )
    parser.add_argument(
        "--models",
        action="store_const",
        const="models",
        dest="filter",
        help="Run model tests only",
    )
    parser.add_argument(
        "--resources",
        action="store_const",
        const="resources",
        dest="filter",
        help="Run resource tests only",
    )
    parser.add_argument(
        "--exceptions",
        action="store_const",
        const="exceptions",
        dest="filter",
        help="Run exception tests only",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument(
        "--cov",
        "--coverage",
        action="store_true",
        dest="coverage",
        help="Run with coverage reporting",
    )

    args = parser.parse_args()

    # Run the tests
    exit_code = run_tests(
        test_filter=args.filter, verbose=args.verbose, coverage=args.coverage
    )

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
