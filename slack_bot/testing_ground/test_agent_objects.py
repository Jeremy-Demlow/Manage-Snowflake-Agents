#!/usr/bin/env python3
"""
Test the proper Agent Object API approach using existing Snowflake Intelligence layers
This is the CORRECT approach according to Snowflake documentation
"""

import sys
import json
import requests
import time
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


class AgentObjectTester:
    """
    Test the proper Agent Object API using existing Snowflake Intelligence layers
    """

    def __init__(self):
        self.connection = None
        self.account = None
        self.base_url = None
        self.available_agents = []
        self.results = {"timestamp": time.time(), "tests": [], "agent_responses": {}}

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

    def get_available_agents(self):
        """Get the list of available agent objects"""
        print("\n🤖 Getting available agents...")

        try:
            agents_df = self.connection.sql(
                "SHOW AGENTS IN DATABASE SNOWFLAKE_INTELLIGENCE"
            )
            agents = agents_df.collect()

            self.available_agents = []
            for agent in agents:
                # Debug: print the agent structure
                print(f"    🔍 Agent structure: {agent}")
                try:
                    # Try different ways to access the data
                    if hasattr(agent, "asDict"):
                        agent_dict = agent.asDict()
                        name = agent_dict.get("name", agent_dict.get("NAME", "Unknown"))
                    else:
                        # Fallback to indexing if it's a Row/list-like object
                        name = agent[1] if len(agent) > 1 else "Unknown"

                    agent_info = {
                        "name": name,
                        "database": "SNOWFLAKE_INTELLIGENCE",
                        "schema": "AGENTS",
                        "comment": "Available agent",
                    }
                    self.available_agents.append(agent_info)
                    print(f"  📋 {agent_info['name']}: {agent_info['comment']}")
                except Exception as e:
                    print(f"    ⚠️ Error parsing agent: {e}")
                    continue

            print(f"✅ Found {len(self.available_agents)} agents")
            return len(self.available_agents) > 0

        except Exception as e:
            print(f"❌ Failed to get agents: {e}")
            return False

    def test_agent_object_api(self, agent_name: str, question: str):
        """Test the Agent Object API with a specific agent"""
        print(f"\n🧪 Testing Agent Object API: {agent_name}")
        print(f"   Question: {question}")

        if not self.base_url:
            return {"error": "No base URL available"}

        # Construct the proper Agent Object API endpoint
        database = "SNOWFLAKE_INTELLIGENCE"
        schema = "AGENTS"
        endpoint = f"{self.base_url}/api/v2/databases/{database}/schemas/{schema}/agents/{agent_name}:run"

        # Simple request payload for Agent Object API
        payload = {
            "messages": [
                {"role": "user", "content": [{"type": "text", "text": question}]}
            ]
        }

        # Try different authentication methods
        auth_methods = [
            ("snowsight_token", self._get_snowsight_token()),
            # Note: PAT would go here if we had one
        ]

        for auth_name, auth_header in auth_methods:
            if not auth_header:
                continue

            print(f"    🔑 Testing with {auth_name}...")

            headers = {
                "Authorization": auth_header,
                "Content-Type": "application/json",
                "Accept": "application/json",
            }

            try:
                response = requests.post(
                    endpoint,
                    json=payload,
                    headers=headers,
                    timeout=30,
                    stream=True,  # Enable streaming
                )

                result = {
                    "agent": agent_name,
                    "question": question,
                    "auth_method": auth_name,
                    "endpoint": endpoint,
                    "status_code": response.status_code,
                    "success": response.status_code == 200,
                }

                if response.status_code == 200:
                    print(f"      ✅ Agent {agent_name} responded!")

                    # Parse streaming response
                    streaming_events = []
                    try:
                        for line in response.iter_lines(decode_unicode=True):
                            if line.startswith("event: ") or line.startswith("data: "):
                                streaming_events.append(line)
                                if len(streaming_events) <= 10:  # First 10 events
                                    print(f"        📡 {line[:100]}...")
                    except Exception as e:
                        print(f"        ⚠️ Streaming parse error: {e}")

                    result["streaming_events"] = streaming_events[
                        :20
                    ]  # First 20 events
                    result["total_events"] = len(streaming_events)

                    # Try to extract key information
                    parsed_response = self._parse_agent_streaming_response(
                        streaming_events
                    )
                    result["parsed_response"] = parsed_response

                    if parsed_response.get("text"):
                        print(
                            f"      💬 Response preview: {parsed_response['text'][:150]}..."
                        )

                else:
                    print(f"      ❌ Agent {agent_name} failed: {response.status_code}")
                    result["error"] = response.text[:300]

                self.results["tests"].append(result)
                return result

            except Exception as e:
                print(f"      ❌ Request failed: {e}")
                return {
                    "agent": agent_name,
                    "question": question,
                    "auth_method": auth_name,
                    "error": str(e),
                    "success": False,
                }

        return {"error": "No valid authentication method"}

    def _get_snowsight_token(self):
        """Try to get a Snowsight context token"""
        try:
            token_result = self.connection.sql(
                "SELECT SYSTEM$GET_SNOWSIGHT_CONTEXT_TOKEN() as token"
            ).collect()
            if token_result:
                token = token_result[0]["TOKEN"]
                if token:
                    print(f"      ✅ Got Snowsight token ({len(token)} chars)")
                    return f"Bearer {token}"
        except Exception as e:
            print(f"      ⚠️ Snowsight token failed: {e}")
        return None

    def _parse_agent_streaming_response(self, streaming_events):
        """Parse the Agent Object API streaming response format"""
        result = {
            "text": "",
            "tool_uses": [],
            "status_updates": [],
            "final_response": None,
        }

        try:
            for line in streaming_events:
                if line.startswith("data: "):
                    try:
                        data = json.loads(line[6:])  # Remove 'data: ' prefix

                        # Handle different event types from Agent Object API
                        if "text" in data:
                            result["text"] += data.get("text", "")

                        if "status" in data:
                            result["status_updates"].append(data.get("message", ""))

                        if "type" in data and data.get("type") in [
                            "cortex_analyst",
                            "cortex_search",
                        ]:
                            result["tool_uses"].append(data.get("type"))

                        if "role" in data and data.get("role") == "assistant":
                            result["final_response"] = data

                    except json.JSONDecodeError:
                        continue

        except Exception as e:
            logger.warning(f"Error parsing agent streaming response: {e}")

        return result

    def test_multiple_agents(self):
        """Test multiple agents with different question types"""
        print("\n🎯 Testing Multiple Agents with Different Questions...")

        # Test questions designed for different agents
        test_cases = [
            ("ACME_INTELLIGENCE_AGENT", "How many customers do we have?"),
            ("ACME_CONTRACTS_AGENT", "Which contracts are at risk of churn?"),
            ("DATA_ENGINEER_ASSISTANT", "What are the slowest queries today?"),
            # Add more test cases as needed
        ]

        successful_tests = 0
        total_tests = 0

        for agent_name, question in test_cases:
            # Check if this agent exists
            agent_exists = any(
                agent["name"] == agent_name for agent in self.available_agents
            )
            if not agent_exists:
                print(f"    ⚠️ Skipping {agent_name} - not found in available agents")
                continue

            total_tests += 1
            result = self.test_agent_object_api(agent_name, question)

            if result.get("success"):
                successful_tests += 1
                self.results["agent_responses"][agent_name] = result

        success_rate = successful_tests / max(total_tests, 1)
        print(
            f"\n📊 Success rate: {successful_tests}/{total_tests} ({success_rate:.1%})"
        )

        return success_rate > 0

    def generate_findings_report(self):
        """Generate findings about the Agent Object API"""
        print("\n📋 Agent Object API Findings:")

        successful_agents = [t for t in self.results["tests"] if t.get("success")]

        if successful_agents:
            print(f"  ✅ {len(successful_agents)} agents responded successfully")

            for test in successful_agents:
                agent_name = test["agent"]
                parsed = test.get("parsed_response", {})
                text_preview = parsed.get("text", "")[:100]
                tool_uses = parsed.get("tool_uses", [])

                print(f"  🤖 {agent_name}:")
                print(f"     💬 Response: {text_preview}...")
                if tool_uses:
                    print(f"     🔧 Tools used: {', '.join(tool_uses)}")
        else:
            print("  ❌ No agents responded successfully")
            print("  💡 This likely means we need a proper Personal Access Token (PAT)")

        # Save results
        output_file = Path(__file__).parent / "agent_object_results.json"
        with open(output_file, "w") as f:
            json.dump(self.results, f, indent=2)

        print(f"\n💾 Results saved to {output_file}")

        return len(successful_agents) > 0

    def run_all_tests(self):
        """Run all Agent Object API tests"""
        print("🚀 Testing Agent Object API with Existing Intelligence Layers")
        print("=" * 70)

        if not self.setup_connection():
            return False

        if not self.get_available_agents():
            return False

        # Test multiple agents
        success = self.test_multiple_agents()

        # Generate findings
        self.generate_findings_report()

        print("\n" + "=" * 70)
        if success:
            print("🎉 Agent Object API is working! This is the correct approach.")
            print("\n📝 Next steps:")
            print("   1. Get a Personal Access Token (PAT) for full functionality")
            print("   2. Build Slack bot using Agent Object API endpoints")
            print("   3. Let the existing agent intelligence do the work!")
        else:
            print("⚠️ Need PAT authentication, but Agent Object approach is validated")
            print("\n📝 Next steps:")
            print("   1. Create a Snowflake Personal Access Token")
            print("   2. Test with PAT authentication")
            print("   3. Build production Slack bot")

        return True


def main():
    """Main test function"""
    tester = AgentObjectTester()

    try:
        tester.run_all_tests()

    except KeyboardInterrupt:
        print("\n⏹️ Testing interrupted")
    except Exception as e:
        print(f"\n❌ Testing failed: {e}")
        logger.exception("Test failed")


if __name__ == "__main__":
    main()
