#!/usr/bin/env python3
"""
Simple test runner for Snowflake Agent API testing
"""

import subprocess
import sys
from pathlib import Path
import argparse


def run_command(cmd, description):
    """Run a command and handle the output"""
    print(f"\n{'='*60}")
    print(f"🚀 {description}")
    print(f"{'='*60}")

    try:
        result = subprocess.run(
            cmd,
            shell=True,
            cwd=Path(__file__).parent,
            text=True,
            capture_output=False,  # Show output in real time
        )

        if result.returncode == 0:
            print(f"✅ {description} completed successfully")
        else:
            print(f"❌ {description} failed with exit code {result.returncode}")

        return result.returncode == 0

    except Exception as e:
        print(f"❌ Error running {description}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Run Snowflake Agent API tests")
    parser.add_argument(
        "--basic-only", action="store_true", help="Run only basic connectivity tests"
    )
    parser.add_argument(
        "--api-only", action="store_true", help="Run only API response tests"
    )
    parser.add_argument("--questions", help="Custom questions JSON file")

    args = parser.parse_args()

    print("🧪 Snowflake Agent API Testing Suite")
    print("This will test your actual ACME Intelligence agents")

    success_count = 0
    total_tests = 0

    # Test 1: Basic connectivity
    if not args.api_only:
        total_tests += 1
        if run_command("python simple_agent_test.py", "Basic Connectivity Test"):
            success_count += 1

    # Test 2: API Response Analysis
    if not args.basic_only:
        total_tests += 1
        cmd = "python api_response_analyzer.py"
        if args.questions:
            cmd += f" --questions {args.questions}"

        if run_command(cmd, "API Response Analysis"):
            success_count += 1

    # Summary
    print(f"\n{'='*60}")
    print(f"📊 TEST SUMMARY")
    print(f"{'='*60}")
    print(f"✅ Passed: {success_count}/{total_tests} tests")

    if success_count == total_tests:
        print("🎉 All tests completed successfully!")
        print("\n💡 Next steps:")
        print("   1. Review api_analysis_results.json")
        print("   2. Update examples.md with your findings")
        print("   3. Refactor the Slack bot based on real API behavior")
    else:
        print("⚠️ Some tests failed. Check the output above for details.")
        print("\n🔧 Troubleshooting:")
        print("   1. Verify your Snowflake connection configuration")
        print("   2. Check if agents exist in SNOWFLAKE_INTELLIGENCE.AGENTS")
        print("   3. Ensure you have proper permissions")

    # Show available files
    print(f"\n📁 Generated files:")
    for file_path in Path(".").glob("*.json"):
        print(f"   - {file_path}")

    return 0 if success_count == total_tests else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n⏹️ Testing interrupted by user")
        sys.exit(1)
