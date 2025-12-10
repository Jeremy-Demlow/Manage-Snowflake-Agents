"""
Agent Caller UDF Handler
Calls Cortex Agent via REST API using PAT authentication stored as a secret.

Deployed via snowflake.yml as: SKI_RESORT_DB.MODELS.ASK_RESORT_EXECUTIVE
"""

import _snowflake
import requests
import json


def run_agent(user_query: str) -> str:
    """
    Call the Resort Executive Agent via REST API.

    Uses PAT token from Snowflake secret for authentication.
    Parses SSE streaming response to extract final answer.

    Args:
        user_query: The question to ask the agent

    Returns:
        Agent's text response or error message
    """
    # Get PAT from secret
    try:
        token = _snowflake.get_generic_secret_string("agent_token")
    except Exception as e:
        return f"Error reading secret: {str(e)}"

    # Agent API endpoint - update account/agent name as needed
    url = "https://trb65519.snowflakecomputing.com/api/v2/databases/SKI_RESORT_DB/schemas/AGENTS/agents/RESORT_EXECUTIVE_DEV:run"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "text/event-stream",
    }

    payload = {
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": user_query}]}
        ]
    }

    try:
        response = requests.post(
            url, headers=headers, json=payload, stream=True, timeout=300
        )

        if response.status_code != 200:
            return f"API Error {response.status_code}: {response.text[:500]}"

        # Parse SSE stream
        final_answer = []
        current_event = None

        for line in response.iter_lines():
            if not line:
                continue
            decoded = line.decode("utf-8")

            if decoded.startswith("event: "):
                current_event = decoded[7:].strip()

            if decoded.startswith("data: "):
                data_str = decoded[6:]
                if data_str == "[DONE]":
                    break
                try:
                    data = json.loads(data_str)
                    if current_event == "response.text.delta" and "text" in data:
                        final_answer.append(data["text"])
                except json.JSONDecodeError:
                    continue

        return "".join(final_answer) if final_answer else "Agent returned no content."

    except requests.exceptions.Timeout:
        return "Error: Request timed out after 300 seconds"
    except requests.exceptions.RequestException as e:
        return f"Connection error: {str(e)}"
    except Exception as e:
        return f"Unexpected error: {str(e)}"
