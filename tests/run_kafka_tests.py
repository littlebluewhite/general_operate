"""
Kafka module test runner script.

This script provides convenient ways to run the complete Kafka test suite
with different configurations and options.

Usage:
    # Run all tests
    uv run python tests/run_kafka_tests.py

    # Run specific test categories
    uv run python tests/run_kafka_tests.py --category unit
    uv run python tests/run_kafka_tests.py --category integration
    uv run python tests/run_kafka_tests.py --category performance
    uv run python tests/run_kafka_tests.py --category security

    # Run with coverage
    uv run python tests/run_kafka_tests.py --coverage

    # Run with verbose output
    uv run python tests/run_kafka_tests.py --verbose

    # Run specific test files
    uv run python tests/run_kafka_tests.py --file test_kafka_client.py
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Optional


class KafkaTestRunner:
    """Test runner for Kafka module tests."""
    
    def __init__(self):
        self.test_dir = Path(__file__).parent
        self.project_root = self.test_dir.parent
        
        # Test categories
        self.test_categories = {
            "unit": [
                "test_kafka_client.py",
                "test_dlq_handler.py", 
                "test_manager_config.py",
                "test_service_event_manager.py",
            ],
            "integration": [
                "test_integration_kafka.py",
            ],
            "performance": [
                "test_performance_kafka.py",
            ],
            "security": [
                "test_security_kafka.py",
            ],
        }
        
        # All test files
        self.all_test_files = []
        for files in self.test_categories.values():
            self.all_test_files.extend(files)

    def run_tests(
        self,
        category: Optional[str] = None,
        test_file: Optional[str] = None,
        coverage: bool = False,
        verbose: bool = False,
        markers: Optional[List[str]] = None,
        parallel: bool = False,
    ) -> int:
        """Run tests with specified options."""
        
        # Build pytest command
        cmd = ["uv", "run", "python", "-m", "pytest"]
        
        # Determine which tests to run
        if test_file:
            if not test_file.startswith("test_"):
                test_file = f"test_{test_file}"
            if not test_file.endswith(".py"):
                test_file = f"{test_file}.py"
            test_paths = [str(self.test_dir / test_file)]
        elif category:
            if category not in self.test_categories:
                print(f"Error: Unknown category '{category}'")
                print(f"Available categories: {', '.join(self.test_categories.keys())}")
                return 1
            test_paths = [str(self.test_dir / f) for f in self.test_categories[category]]
        else:
            # Run all tests
            test_paths = [str(self.test_dir / f) for f in self.all_test_files]
        
        cmd.extend(test_paths)
        
        # Add coverage if requested
        if coverage:
            cmd.extend([
                "--cov=general_operate.kafka",
                "--cov-report=term-missing",
                "--cov-report=html:htmlcov",
                "--cov-report=xml:coverage.xml",
            ])
        
        # Add verbosity
        if verbose:
            cmd.append("-v")
        else:
            cmd.append("-q")
        
        # Add markers
        if markers:
            for marker in markers:
                cmd.extend(["-m", marker])
        
        # Add parallel execution
        if parallel:
            cmd.extend(["-n", "auto"])
        
        # Add other useful options
        cmd.extend([
            "--tb=short",  # Shorter traceback format
            "--strict-markers",  # Strict marker validation
            "--strict-config",  # Strict config validation
        ])
        
        print(f"Running command: {' '.join(cmd)}")
        print(f"Working directory: {self.project_root}")
        print("-" * 60)
        
        # Run the tests
        try:
            result = subprocess.run(
                cmd,
                cwd=self.project_root,
                check=False,
            )
            return result.returncode
        except KeyboardInterrupt:
            print("\nTests interrupted by user")
            return 130
        except Exception as e:
            print(f"Error running tests: {e}")
            return 1

    def list_tests(self, category: Optional[str] = None):
        """List available tests."""
        if category:
            if category not in self.test_categories:
                print(f"Error: Unknown category '{category}'")
                return
            
            print(f"Tests in category '{category}':")
            for test_file in self.test_categories[category]:
                print(f"  - {test_file}")
        else:
            print("Available test categories:")
            for cat, files in self.test_categories.items():
                print(f"\n{cat.upper()}:")
                for test_file in files:
                    print(f"  - {test_file}")

    def check_test_environment(self):
        """Check if the test environment is properly set up."""
        issues = []
        
        # Check if uv is available
        try:
            subprocess.run(["uv", "--version"], check=True, capture_output=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            issues.append("uv command not found. Please install uv.")
        
        # Check if pytest is available in the environment
        try:
            result = subprocess.run(
                ["uv", "run", "python", "-c", "import pytest; print(pytest.__version__)"],
                check=True,
                capture_output=True,
                text=True,
                cwd=self.project_root,
            )
            print(f"pytest version: {result.stdout.strip()}")
        except subprocess.CalledProcessError:
            issues.append("pytest not available in uv environment")
        
        # Check if test files exist
        missing_files = []
        for test_file in self.all_test_files:
            if not (self.test_dir / test_file).exists():
                missing_files.append(test_file)
        
        if missing_files:
            issues.append(f"Missing test files: {', '.join(missing_files)}")
        
        # Check if Kafka module is importable
        try:
            subprocess.run(
                ["uv", "run", "python", "-c", "import general_operate.kafka"],
                check=True,
                capture_output=True,
                cwd=self.project_root,
            )
        except subprocess.CalledProcessError:
            issues.append("Cannot import general_operate.kafka module")
        
        if issues:
            print("Environment issues found:")
            for issue in issues:
                print(f"  - {issue}")
            return False
        else:
            print("Test environment looks good!")
            return True

    def generate_test_report(self):
        """Generate a comprehensive test report."""
        report_file = self.project_root / "kafka_test_report.html"
        
        cmd = [
            "uv", "run", "python", "-m", "pytest",
            *[str(self.test_dir / f) for f in self.all_test_files],
            "--html", str(report_file),
            "--self-contained-html",
            "--cov=general_operate.kafka",
            "--cov-report=html:htmlcov",
            "-v",
        ]
        
        print(f"Generating comprehensive test report...")
        result = subprocess.run(cmd, cwd=self.project_root)
        
        if result.returncode == 0:
            print(f"Test report generated: {report_file}")
            print(f"Coverage report generated: {self.project_root}/htmlcov/index.html")
        
        return result.returncode


def main():
    """Main entry point for the test runner."""
    parser = argparse.ArgumentParser(
        description="Run Kafka module tests",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    
    parser.add_argument(
        "--category", "-c",
        choices=["unit", "integration", "performance", "security"],
        help="Run tests from a specific category",
    )
    
    parser.add_argument(
        "--file", "-f",
        help="Run a specific test file",
    )
    
    parser.add_argument(
        "--coverage",
        action="store_true",
        help="Run tests with coverage reporting",
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose output",
    )
    
    parser.add_argument(
        "--markers", "-m",
        nargs="+",
        help="Run tests with specific pytest markers",
    )
    
    parser.add_argument(
        "--parallel", "-p",
        action="store_true",
        help="Run tests in parallel (requires pytest-xdist)",
    )
    
    parser.add_argument(
        "--list", "-l",
        action="store_true",
        help="List available tests",
    )
    
    parser.add_argument(
        "--check-env",
        action="store_true",
        help="Check test environment setup",
    )
    
    parser.add_argument(
        "--report",
        action="store_true",
        help="Generate comprehensive test report",
    )
    
    args = parser.parse_args()
    
    runner = KafkaTestRunner()
    
    if args.list:
        runner.list_tests(args.category)
        return 0
    
    if args.check_env:
        success = runner.check_test_environment()
        return 0 if success else 1
    
    if args.report:
        return runner.generate_test_report()
    
    # Run tests
    return runner.run_tests(
        category=args.category,
        test_file=args.file,
        coverage=args.coverage,
        verbose=args.verbose,
        markers=args.markers,
        parallel=args.parallel,
    )


if __name__ == "__main__":
    sys.exit(main())