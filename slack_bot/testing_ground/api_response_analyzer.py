#!/usr/bin/env python3
"""
Snowflake Agent API Response Analyzer
Captures and analyzes actual API responses to understand the structure
"""

import sys
import json
import requests
import time
from pathlib import Path
import argparse
from datetime import datetime
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


class AgentAPIAnalyzer:
    """Analyzes Snowflake Agent API responses"""

    def __init__(self):
        self.connection = None
        self.base_url = None
        self.session = requests.Session()
        self.results = {
            "timestamp": datetime.now().isoformat(),
            "tests": [],
            "errors": [],
            "learnings": [],
        }

    def setup_connection(self):
        """Setup Snowflake connection and API access"""
        print("🔗 Setting up Snowflake connection...")

        try:
            self.connection = SnowflakeConnection.from_snow_cli(
                "snowflake_intelligence"
            )

            # Switch to agents schema
            self.connection.execute("USE DATABASE SNOWFLAKE_INTELLIGENCE")
            # Don't need to USE SCHEMA since we'll query at database level

            print(
                f"✅ Connected to {self.connection.current_database}.{self.connection.current_schema}"
            )

            # Try to determine base URL from connection
            if hasattr(self.connection, "_config") and self.connection._config:
                account = self.connection._config.account
                self.base_url = f"https://{account}.snowflakecomputing.com"
            else:
                print(
                    "⚠️ Could not determine base URL - will try to infer from account"
                )

            return True

        except Exception as e:
            print(f"❌ Connection setup failed: {e}")
            self.results["errors"].append(
                {
                    "type": "connection_error",
                    "message": str(e),
                    "timestamp": datetime.now().isoformat(),
                }
            )
            return False

    def get_available_agents(self):
        """Get list of available agents"""
        try:
            agents_df = self.connection.sql(
                "SHOW AGENTS IN DATABASE SNOWFLAKE_INTELLIGENCE"
            )
            agents = agents_df.collect()
            agent_names = [agent["name"] for agent in agents]

            print(f"✅ Found {len(agent_names)} agents: {', '.join(agent_names)}")
            return agent_names

        except Exception as e:
            print(f"❌ Failed to get agents: {e}")
            return []

    def try_api_authentication(self):
        """Try different authentication methods for the API"""
        print("\n🔑 Testing API authentication methods...")

        auth_methods = []

        # Method 1: Try to get a Snowsight context token
        try:
            token_result = self.connection.sql(
                "SELECT SYSTEM$GET_SNOWSIGHT_CONTEXT_TOKEN() as token"
            ).collect()
            if token_result:
                token = token_result[0]["TOKEN"]
                if token:
                    auth_methods.append(("snowsight_token", f"Bearer {token}"))
                    print("✅ Got Snowsight context token")
        except Exception as e:
            print(f"⚠️ Snowsight token failed: {e}")

        # Method 2: Try to get current session token (may not work)
        try:
            session_result = self.connection.sql(
                "SELECT CURRENT_SESSION() as session_id"
            ).collect()
            if session_result:
                session_id = session_result[0]["SESSION_ID"]
                print(f"ℹ️ Current session ID: {session_id}")
        except Exception as e:
            print(f"⚠️ Session info failed: {e}")

        # Method 3: Try username/password (not recommended for production)
        if hasattr(self.connection, "_config") and self.connection._config:
            if hasattr(self.connection._config, "user") and hasattr(
                self.connection._config, "password"
            ):
                # We won't actually use this, just note it's available
                auth_methods.append(("basic_auth", "username:password"))
                print("ℹ️ Username/password authentication available")

        return auth_methods

    def test_api_endpoint(self, agent_name, question, auth_header):
        """Test actual API endpoint call"""
        if not self.base_url:
            return {"error": "No base URL available"}

        database = self.connection.current_database
        schema = self.connection.current_schema
        endpoint = (
            f"/api/v2/databases/{database}/schemas/{schema}/agents/{agent_name}:run"
        )
        url = f"{self.base_url}{endpoint}"

        payload = {
            "messages": [
                {"role": "user", "content": [{"type": "text", "text": question}]}
            ]
        }

        headers = {
            "Authorization": auth_header,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        try:
            print(f"🌐 Testing API call to {agent_name}...")
            print(f"   URL: {url}")
            print(f"   Question: {question}")

            response = self.session.post(
                url, json=payload, headers=headers, timeout=10, stream=True
            )

            result = {
                "agent": agent_name,
                "question": question,
                "status_code": response.status_code,
                "headers": dict(response.headers),
                "url": url,
            }

            if response.status_code == 200:
                print("✅ API call successful!")

                # Try to read response
                response_text = response.text[:1000]  # First 1000 chars
                result["response_preview"] = response_text

                # Try to parse as Server-Sent Events
                lines = response_text.split("\n")[:10]  # First 10 lines
                result["response_lines"] = lines

            else:
                print(f"❌ API call failed with status {response.status_code}")
                result["error"] = response.text[:500]

            return result

        except requests.exceptions.RequestException as e:
            print(f"❌ Request failed: {e}")
            return {
                "agent": agent_name,
                "question": question,
                "error": str(e),
                "error_type": type(e).__name__,
            }

    def analyze_responses(self, questions_file=None):
        """Analyze API responses for different question types"""
        print("\n🔬 Analyzing API responses...")

        # Load test questions
        if questions_file and Path(questions_file).exists():
            with open(questions_file, "r") as f:
                all_questions = json.load(f)
        else:
            # Use simple test questions
            all_questions = {
                "simple_test": ["Hello, are you working?", "What can you help me with?"]
            }

        # Get agents
        agents = self.get_available_agents()
        if not agents:
            print("❌ No agents available for testing")
            return

        # Try authentication
        auth_methods = self.try_api_authentication()
        if not auth_methods:
            print("❌ No authentication methods available")
            return

        # Test with first agent and first auth method
        test_agent = agents[0]
        auth_method, auth_header = auth_methods[0]

        print(f"\n🧪 Testing with agent: {test_agent}")
        print(f"🔑 Using auth method: {auth_method}")

        # Test each question category
        for category, questions in all_questions.items():
            print(f"\n📂 Testing {category} questions...")

            for i, question in enumerate(
                questions[:2]
            ):  # Test first 2 questions per category
                print(f"\n   Question {i+1}: {question}")

                result = self.test_api_endpoint(test_agent, question, auth_header)
                result["category"] = category
                result["auth_method"] = auth_method
                result["timestamp"] = datetime.now().isoformat()

                self.results["tests"].append(result)

                # Small delay between requests
                time.sleep(1)

    def generate_learning_report(self):
        """Generate a report of what we learned"""
        print("\n📊 Generating learning report...")

        # Analyze results
        successful_tests = [
            t for t in self.results["tests"] if t.get("status_code") == 200
        ]
        failed_tests = [t for t in self.results["tests"] if t.get("status_code") != 200]

        learnings = []

        learnings.append(f"Tested {len(self.results['tests'])} API calls")
        learnings.append(f"Successful: {len(successful_tests)}")
        learnings.append(f"Failed: {len(failed_tests)}")

        if successful_tests:
            learnings.append("✅ API endpoint is accessible")

            # Analyze response structure
            sample_response = successful_tests[0]
            if "response_preview" in sample_response:
                learnings.append("✅ Got response data")

                response_text = sample_response["response_preview"]
                if "data:" in response_text:
                    learnings.append("✅ Response uses Server-Sent Events format")

                if '"event"' in response_text:
                    learnings.append("✅ Response contains event-based JSON")

        if failed_tests:
            # Analyze common errors
            error_codes = [
                t.get("status_code") for t in failed_tests if t.get("status_code")
            ]
            if error_codes:
                common_error = max(set(error_codes), key=error_codes.count)
                learnings.append(f"❌ Most common error: HTTP {common_error}")

        self.results["learnings"] = learnings

        # Print learning summary
        print("\n📚 Learning Summary:")
        for learning in learnings:
            print(f"   {learning}")

    def save_results(self, output_file="api_analysis_results.json"):
        """Save analysis results to file"""
        output_path = Path(__file__).parent / output_file

        with open(output_path, "w") as f:
            json.dump(self.results, f, indent=2)

        print(f"💾 Results saved to {output_path}")

    def run_analysis(self, questions_file=None):
        """Run complete analysis"""
        print("🚀 Starting Snowflake Agent API Analysis")
        print("=" * 60)

        if not self.setup_connection():
            return False

        self.analyze_responses(questions_file)
        self.generate_learning_report()
        self.save_results()

        print("\n" + "=" * 60)
        print("✅ Analysis complete!")

        return True


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="Analyze Snowflake Agent API responses"
    )
    parser.add_argument("--questions", help="JSON file with test questions")
    parser.add_argument(
        "--output", default="api_analysis_results.json", help="Output file for results"
    )

    args = parser.parse_args()

    analyzer = AgentAPIAnalyzer()

    try:
        success = analyzer.run_analysis(args.questions)
        if success:
            print("\n💡 Next steps:")
            print("   1. Review the results in api_analysis_results.json")
            print("   2. Check examples.md for documented findings")
            print("   3. Update the Slack bot based on real API behavior")

    except KeyboardInterrupt:
        print("\n⏹️ Analysis interrupted by user")
    except Exception as e:
        print(f"\n❌ Analysis failed: {e}")
        logger.exception("Analysis failed")


if __name__ == "__main__":
    main()
