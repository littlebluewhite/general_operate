"""
Test runner for modernized Kafka client tests.

This script runs all the tests for the modernized Kafka security configuration
and provides comprehensive reporting on the test results.
"""

import sys
import subprocess
import time
from pathlib import Path
from typing import List, Dict, Any


def run_test_suite(test_file: str, verbose: bool = True) -> Dict[str, Any]:
    """Run a specific test suite and return results"""
    print(f"\n{'='*60}")
    print(f"Running: {test_file}")
    print(f"{'='*60}")
    
    cmd = ["uv", "run", "python", "-m", "pytest", test_file]
    if verbose:
        cmd.extend(["-v", "-s"])
    cmd.extend(["--tb=short", "--no-header"])
    
    start_time = time.time()
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(Path(__file__).parent.parent)
        )
        end_time = time.time()
        
        return {
            "file": test_file,
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "duration": end_time - start_time,
            "success": result.returncode == 0
        }
    except Exception as e:
        end_time = time.time()
        return {
            "file": test_file,
            "returncode": -1,
            "stdout": "",
            "stderr": str(e),
            "duration": end_time - start_time,
            "success": False
        }


def print_test_summary(results: List[Dict[str, Any]]):
    """Print a summary of all test results"""
    print(f"\n{'='*80}")
    print("TEST SUMMARY")
    print(f"{'='*80}")
    
    total_tests = len(results)
    passed_tests = sum(1 for r in results if r["success"])
    failed_tests = total_tests - passed_tests
    total_duration = sum(r["duration"] for r in results)
    
    print(f"Total test suites: {total_tests}")
    print(f"Passed: {passed_tests}")
    print(f"Failed: {failed_tests}")
    print(f"Total duration: {total_duration:.2f} seconds")
    print()
    
    for result in results:
        status = "✅ PASSED" if result["success"] else "❌ FAILED"
        duration = result["duration"]
        print(f"{status} {result['file']} ({duration:.2f}s)")
    
    if failed_tests > 0:
        print(f"\n{'='*80}")
        print("FAILURE DETAILS")
        print(f"{'='*80}")
        
        for result in results:
            if not result["success"]:
                print(f"\n--- FAILED: {result['file']} ---")
                if result["stdout"]:
                    print("STDOUT:")
                    print(result["stdout"])
                if result["stderr"]:
                    print("STDERR:")
                    print(result["stderr"])
                print()


def run_specific_test_categories():
    """Run specific categories of tests with targeted focus"""
    print("Running modernized Kafka security configuration tests...")
    print("This will test the new SecurityConfigBuilder and KafkaConnectionBuilder classes.")
    
    # Define test categories
    test_categories = {
        "Core Security Configuration": [
            "tests/test_modern_kafka_security.py::TestSecurityConfigBuilder",
            "tests/test_modern_kafka_security.py::TestKafkaConnectionBuilder",
        ],
        "Integration Tests": [
            "tests/test_kafka_modernization_integration.py::TestModernizedKafkaAsyncProducer",
            "tests/test_kafka_modernization_integration.py::TestModernizedKafkaAsyncConsumer",
            "tests/test_kafka_modernization_integration.py::TestModernizedKafkaAsyncAdmin",
        ],
        "Backward Compatibility": [
            "tests/test_modern_kafka_security.py::TestBackwardCompatibility",
            "tests/test_kafka_modernization_integration.py::TestConfigurationMigrationScenarios",
        ],
        "Edge Cases and Error Handling": [
            "tests/test_kafka_security_edge_cases.py::TestSecurityConfigBuilderEdgeCases",
            "tests/test_kafka_security_edge_cases.py::TestKafkaConnectionBuilderEdgeCases",
            "tests/test_kafka_security_edge_cases.py::TestErrorHandling",
        ],
        "Performance and Stress": [
            "tests/test_kafka_modern_performance.py::TestConfigurationPerformance",
            "tests/test_kafka_modern_performance.py::TestStressScenarios",
        ],
    }
    
    all_results = []
    
    for category, test_patterns in test_categories.items():
        print(f"\n{'🔍 ' + category}")
        print(f"{'='*60}")
        
        for test_pattern in test_patterns:
            result = run_test_suite(test_pattern, verbose=True)
            all_results.append(result)
            
            if result["success"]:
                print(f"✅ {test_pattern.split('::')[-1]} - PASSED")
            else:
                print(f"❌ {test_pattern.split('::')[-1]} - FAILED")
                # Print brief error info
                if result["stderr"]:
                    print(f"   Error: {result['stderr'][:200]}...")
    
    return all_results


def main():
    """Main test runner function"""
    print("Modernized Kafka Client Test Suite")
    print("="*50)
    
    # Test files to run
    test_files = [
        "tests/test_modern_kafka_security.py",
        "tests/test_kafka_modernization_integration.py", 
        "tests/test_kafka_security_edge_cases.py",
        "tests/test_kafka_modern_performance.py",
    ]
    
    print("Running comprehensive test suite for modernized Kafka security configuration...")
    print(f"Test files: {len(test_files)}")
    print()
    
    # Check if we should run specific categories
    if len(sys.argv) > 1 and sys.argv[1] == "--categories":
        results = run_specific_test_categories()
    else:
        # Run all test files
        results = []
        for test_file in test_files:
            result = run_test_suite(test_file)
            results.append(result)
            
            # Print immediate feedback
            status = "✅ PASSED" if result["success"] else "❌ FAILED"
            print(f"{status} {test_file} ({result['duration']:.2f}s)")
            
            # Print any failures immediately
            if not result["success"]:
                print("STDOUT:", result["stdout"][-500:] if result["stdout"] else "None")
                print("STDERR:", result["stderr"][-500:] if result["stderr"] else "None")
    
    # Print comprehensive summary
    print_test_summary(results)
    
    # Exit with appropriate code
    all_passed = all(r["success"] for r in results)
    if all_passed:
        print("\n🎉 All tests passed! The modernized Kafka security configuration is working correctly.")
        sys.exit(0)
    else:
        print("\n💥 Some tests failed. Please review the failures above.")
        sys.exit(1)


if __name__ == "__main__":
    main()