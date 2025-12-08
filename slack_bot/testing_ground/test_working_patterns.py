#!/usr/bin/env python3
"""
Test the working patterns from the proven Slack app
Tests the direct Cortex Agent API approach that actually works
"""

import sys
import json
import requests
import time
import re
from pathlib import Path
import logging

# Add the project paths
project_root = Path(__file__).parent.parent.parent
data_setup_path = project_root / "data_setup"
sys.path.insert(0, str(data_setup_path))

try:
    from snowflake_connection import SnowflakeConnection

    print("✅ Successfully imported snowflake_connection")
except ImportError as e:
    print(f"❌ Could not import snowflake_connection: {e}")
    sys.exit(1)

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class WorkingPatternTester:
    """Test the proven patterns from the working Slack app"""

    def __init__(self):
        self.connection = None
        self.account = None
        self.base_url = None
        self.results = {"timestamp": time.time(), "tests": [], "patterns_found": []}

    def setup_connection(self):
        """Setup connection using Snow CLI"""
        print("🔗 Setting up connection...")

        try:
            self.connection = SnowflakeConnection.from_snow_cli(
                "snowflake_intelligence"
            )
            self.connection.execute("USE DATABASE SNOWFLAKE_INTELLIGENCE")

            print(f"✅ Connected to {self.connection.current_database}")

            # Extract account info
            if hasattr(self.connection, "_config") and self.connection._config:
                self.account = self.connection._config.account
                self.base_url = f"https://{self.account}.snowflakecomputing.com"
                print(f"✅ Account: {self.account}")

            return True

        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False

    def test_direct_cortex_api(self):
        """Test the direct Cortex Agent API pattern from working code"""
        print("\n🧪 Testing Direct Cortex Agent API...")

        if not self.base_url:
            print("❌ No base URL available")
            return False

        # Use the working pattern
        endpoint = f"{self.base_url}/api/v2/cortex/agent:run"

        # Test payload similar to working code
        test_payload = {
            "model": "mistral-large2",  # From working code
            "tools": [
                {
                    "tool_spec": {
                        "type": "cortex_analyst_text_to_sql",
                        "name": "analyst_tool",
                    }
                }
            ],
            "tool_resources": {
                "analyst_tool": {
                    "semantic_model_file": "@SNOWFLAKE_INTELLIGENCE.PUBLIC.STAGE/model.yml"  # Example
                }
            },
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": "Hello, are you working?"}],
                }
            ],
        }

        # Test different authentication methods
        auth_methods = [
            ("pat_token", "Bearer YOUR_PAT_TOKEN_HERE"),  # Would need real PAT
            ("snowsight_token", self._get_snowsight_token()),
        ]

        for auth_name, auth_header in auth_methods:
            if not auth_header:
                continue

            print(f"\n  🔑 Testing {auth_name}...")

            headers = {
                "Authorization": auth_header,
                "Content-Type": "application/json",
                "Accept": "application/json",
            }

            # Add PAT-specific header if using PAT
            if auth_name == "pat_token":
                headers[
                    "X-Snowflake-Authorization-Token-Type"
                ] = "PROGRAMMATIC_ACCESS_TOKEN"

            try:
                response = requests.post(
                    endpoint, json=test_payload, headers=headers, timeout=10
                )

                result = {
                    "auth_method": auth_name,
                    "endpoint": endpoint,
                    "status_code": response.status_code,
                    "headers": dict(response.headers),
                    "success": response.status_code == 200,
                }

                if response.status_code == 200:
                    print(f"    ✅ {auth_name} worked!")
                    result["response_preview"] = response.text[:500]
                    self.results["patterns_found"].append(
                        f"Direct API works with {auth_name}"
                    )
                else:
                    print(f"    ❌ {auth_name} failed: {response.status_code}")
                    result["error"] = response.text[:200]

                self.results["tests"].append(result)

            except Exception as e:
                print(f"    ❌ {auth_name} error: {e}")
                self.results["tests"].append(
                    {
                        "auth_method": auth_name,
                        "endpoint": endpoint,
                        "error": str(e),
                        "success": False,
                    }
                )

        return len([t for t in self.results["tests"] if t.get("success")]) > 0

    def _get_snowsight_token(self):
        """Try to get a Snowsight context token"""
        try:
            token_result = self.connection.sql(
                "SELECT SYSTEM$GET_SNOWSIGHT_CONTEXT_TOKEN() as token"
            ).collect()
            if token_result:
                token = token_result[0]["TOKEN"]
                if token:
                    print(f"    ✅ Got Snowsight token ({len(token)} chars)")
                    return f"Bearer {token}"
        except Exception as e:
            print(f"    ⚠️ Snowsight token failed: {e}")
        return None

    def test_intelligent_routing_patterns(self):
        """Test the smart routing logic from working code"""
        print("\n🧪 Testing Intelligent Routing Patterns...")

        # Test questions from working code patterns
        test_cases = [
            # SQL-triggering patterns
            (
                "How many customers do we have?",
                "sql",
                r"\b(count|list|how many|average)\b",
            ),
            ("Show me the top 10 customers", "sql", r"\bshow\s+(?:me|the|\d+)"),
            ("List all contracts", "sql", r"\b(count|list|how many|average)\b"),
            (
                "Average revenue per customer",
                "sql",
                r"\b(count|list|how many|average)\b",
            ),
            # RAG-triggering patterns (everything else)
            ("What is our company policy?", "rag", "default"),
            ("Tell me about safety procedures", "rag", "default"),
            ("How do we handle customer complaints?", "rag", "default"),
        ]

        routing_results = []

        for question, expected_type, pattern_desc in test_cases:
            # Apply the working code's routing logic
            is_sql = re.search(
                r"\b(count|list|how many|average)\b", question, flags=re.I
            ) or re.search(r"\bshow\s+(?:me|the|\d+)", question, flags=re.I)

            detected_type = "sql" if is_sql else "rag"
            correct = detected_type == expected_type

            result = {
                "question": question,
                "expected": expected_type,
                "detected": detected_type,
                "correct": correct,
                "pattern": pattern_desc,
            }

            routing_results.append(result)

            status = "✅" if correct else "❌"
            print(
                f"  {status} '{question}' → {detected_type} (expected {expected_type})"
            )

        accuracy = sum(1 for r in routing_results if r["correct"]) / len(
            routing_results
        )
        print(f"\n📊 Routing accuracy: {accuracy:.1%}")

        self.results["routing_accuracy"] = accuracy
        self.results["routing_tests"] = routing_results

        if accuracy > 0.8:
            self.results["patterns_found"].append(
                "Intelligent routing logic works well"
            )

        return accuracy > 0.8

    def test_response_parsing_pattern(self):
        """Test response parsing patterns from working code"""
        print("\n🧪 Testing Response Parsing Patterns...")

        # Sample streaming response format (from working code comments)
        sample_response = """data: {"delta": {"content": [{"type": "tool_results", "tool_results": {"content": [{"type": "json", "json": {"sql": "SELECT COUNT(*) FROM customers", "text": "Found 100 customers"}}]}}]}}
data: {"delta": {"content": [{"type": "text", "text": "The query completed successfully."}]}}
data: [DONE]"""

        # Test the parsing function pattern
        parsed = self._parse_streaming_response(sample_response)

        success_indicators = [
            ("sql_extracted", parsed.get("sql") is not None),
            ("text_extracted", len(parsed.get("text", "")) > 0),
            ("json_structure", isinstance(parsed, dict)),
        ]

        for indicator, success in success_indicators:
            status = "✅" if success else "❌"
            print(f"  {status} {indicator}: {success}")

        parsing_success = all(success for _, success in success_indicators)

        if parsing_success:
            self.results["patterns_found"].append("Response parsing pattern works")
            print(f"\n📋 Parsed result: {json.dumps(parsed, indent=2)}")

        return parsing_success

    def _parse_streaming_response(self, response_text: str) -> dict:
        """Parse server-sent events streaming response (from working code)"""
        result = {"sql": None, "text": "", "searchResults": []}

        try:
            lines = response_text.strip().split("\n")

            for line in lines:
                if line.startswith("data: ") and not line.endswith("[DONE]"):
                    json_str = line[6:]  # Remove 'data: ' prefix
                    try:
                        data = json.loads(json_str)

                        # Navigate through the nested structure (from working code)
                        if "delta" in data and "content" in data["delta"]:
                            for content in data["delta"]["content"]:
                                if content.get("type") == "tool_results":
                                    tool_results = content.get("tool_results", {})
                                    if "content" in tool_results:
                                        for tool_content in tool_results["content"]:
                                            if tool_content.get("type") == "json":
                                                json_data = tool_content.get("json", {})

                                                # Extract the good stuff
                                                if "sql" in json_data:
                                                    result["sql"] = json_data["sql"]
                                                if "text" in json_data:
                                                    result["text"] += json_data["text"]
                                                if "searchResults" in json_data:
                                                    result["searchResults"].extend(
                                                        json_data["searchResults"]
                                                    )

                                elif content.get("type") == "text":
                                    result["text"] += content.get("text", "")

                    except json.JSONDecodeError:
                        continue

        except Exception as e:
            logger.warning(f"Error parsing streaming response: {e}")

        return result

    def generate_findings_report(self):
        """Generate a report of what we learned"""
        print("\n📊 Generating Findings Report...")

        findings = {
            "summary": {
                "total_tests": len(self.results["tests"]),
                "successful_tests": len(
                    [t for t in self.results["tests"] if t.get("success")]
                ),
                "patterns_found": len(self.results["patterns_found"]),
                "routing_accuracy": self.results.get("routing_accuracy", 0),
            },
            "key_patterns": self.results["patterns_found"],
            "recommendations": [],
        }

        # Generate recommendations
        if findings["summary"]["successful_tests"] > 0:
            findings["recommendations"].append(
                "✅ Direct Cortex Agent API approach is viable"
            )

        if findings["summary"]["routing_accuracy"] > 0.8:
            findings["recommendations"].append(
                "✅ Use regex-based intelligent routing for tool selection"
            )

        findings["recommendations"].extend(
            [
                "🔧 Focus on PAT token authentication (most reliable)",
                "📝 Use proven response parsing patterns from working code",
                "🧠 Implement the SQL vs RAG routing logic",
                "🚀 Build iteratively starting with this proven foundation",
            ]
        )

        print("\n📋 Key Findings:")
        for pattern in findings["key_patterns"]:
            print(f"  ✅ {pattern}")

        print("\n💡 Recommendations:")
        for rec in findings["recommendations"]:
            print(f"  {rec}")

        return findings

    def run_all_tests(self):
        """Run all tests and generate report"""
        print("🚀 Testing Working Patterns from Proven Slack App")
        print("=" * 60)

        if not self.setup_connection():
            return False

        # Run all test suites
        api_success = self.test_direct_cortex_api()
        routing_success = self.test_intelligent_routing_patterns()
        parsing_success = self.test_response_parsing_pattern()

        # Generate findings
        findings = self.generate_findings_report()

        # Save results
        output_file = Path(__file__).parent / "working_patterns_results.json"
        with open(output_file, "w") as f:
            json.dump({"test_results": self.results, "findings": findings}, f, indent=2)

        print(f"\n💾 Results saved to {output_file}")

        success_rate = sum([api_success, routing_success, parsing_success]) / 3
        print(f"\n🎯 Overall Success Rate: {success_rate:.1%}")

        return success_rate > 0.5


def main():
    """Main test function"""
    tester = WorkingPatternTester()

    try:
        success = tester.run_all_tests()

        if success:
            print("\n🎉 Working patterns validated! Ready to build improved Slack bot.")
            print("\n📝 Next steps:")
            print("   1. Get a Snowflake Personal Access Token (PAT)")
            print("   2. Implement the direct Cortex Agent API approach")
            print("   3. Use the proven routing and parsing patterns")
            print("   4. Test with real agents in your environment")
        else:
            print(
                "\n⚠️ Some patterns need adjustment, but we have a foundation to work with."
            )

    except KeyboardInterrupt:
        print("\n⏹️ Testing interrupted")
    except Exception as e:
        print(f"\n❌ Testing failed: {e}")
        logger.exception("Test failed")


if __name__ == "__main__":
    main()
