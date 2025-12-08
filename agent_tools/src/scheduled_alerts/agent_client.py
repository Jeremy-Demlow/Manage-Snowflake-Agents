"""
Cortex Agent REST API Client

A production-ready client for calling Snowflake Cortex Agents via REST API.
Supports multiple authentication methods and runtime environments.

Authentication methods:
- SessionTokenProvider: From Snowpark session (local development)
- ContainerTokenProvider: From SPCS/Container Runtime token file
- KeyPairTokenProvider: Using RSA key-pair (external schedulers)
- PATTokenProvider: Using Personal Access Token (simplest for external use)

Usage:
    from agent_client import AgentClient

    # From Snowpark session
    client = AgentClient.from_session(session, agent_name="MY_AGENT")
    response = client.ask("What is total revenue?")

    # Using PAT (Personal Access Token)
    client = AgentClient.from_pat(
        agent_name="MY_AGENT",
        database="MY_DB",
        schema="MY_SCHEMA",
        account="my_account",
        pat="my_personal_access_token"
    )
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional

import requests


@dataclass
class AgentConfig:
    """Configuration for Cortex Agent connection."""

    database: str
    schema: str
    agent_name: str
    host: str
    timeout: int = 300

    @property
    def endpoint(self) -> str:
        """REST API endpoint for agent."""
        return f"https://{self.host}/api/v2/databases/{self.database}/schemas/{self.schema}/agents/{self.agent_name}:run"


@dataclass
class AgentResponse:
    """Structured response from agent."""

    text: str
    raw_events: List[Dict[str, Any]] = field(default_factory=list)
    tool_calls: List[Dict[str, Any]] = field(default_factory=list)
    thinking: str = ""
    duration_ms: int = 0

    @property
    def has_content(self) -> bool:
        return bool(self.text and len(self.text) >= 10)


class TokenProvider(ABC):
    """Abstract base class for token providers."""

    @abstractmethod
    def get_token(self) -> str:
        """Return a valid authentication token."""
        pass

    @abstractmethod
    def get_host(self) -> str:
        """Return the Snowflake host for REST API calls."""
        pass

    @abstractmethod
    def get_auth_header(self) -> str:
        """Return the Authorization header value."""
        pass


class SessionTokenProvider(TokenProvider):
    """Token provider using Snowpark session."""

    def __init__(self, session):
        self._session = session
        self._token: Optional[str] = None
        self._host: Optional[str] = None
        self._extract_credentials()

    def _extract_credentials(self):
        """Extract token and host from session."""
        # Get token from session internals (works for local Snowpark sessions)
        conn = self._session._conn._conn
        if hasattr(conn, "_rest") and hasattr(conn._rest, "_token"):
            self._token = conn._rest._token
        elif hasattr(conn, "rest") and hasattr(conn.rest, "_token"):
            self._token = conn.rest._token
        else:
            raise RuntimeError(
                "Could not extract token from session. Ensure you're using a local Snowpark session."
            )

        # Get host from session
        result = self._session.sql(
            "SELECT CURRENT_ACCOUNT(), CURRENT_REGION()"
        ).collect()
        account = result[0][0].lower()
        region = result[0][1]

        # Parse region to construct host (e.g., "PUBLIC.AWS_US_WEST_2" -> "us-west-2")
        region_parts = region.split(".")
        if len(region_parts) >= 2:
            cloud_region = region_parts[-1].lower().replace("_", "-")
            if cloud_region.startswith("aws-"):
                cloud_region = cloud_region[4:]
            self._host = f"{account}.{cloud_region}.snowflakecomputing.com"
        else:
            self._host = f"{account}.snowflakecomputing.com"

    def get_token(self) -> str:
        return self._token

    def get_host(self) -> str:
        return self._host

    def get_auth_header(self) -> str:
        return f'Snowflake Token="{self._token}"'


class ContainerTokenProvider(TokenProvider):
    """Token provider for SPCS/Container Runtime environments."""

    TOKEN_PATH = "/snowflake/session/token"

    def __init__(self):
        if not os.path.exists(self.TOKEN_PATH):
            raise RuntimeError(
                f"Token file not found at {self.TOKEN_PATH}. "
                "This provider only works in SPCS/Container Runtime environments."
            )
        self._host = self._resolve_host()

    def _resolve_host(self) -> str:
        host = os.getenv("SNOWFLAKE_HOST")
        if host:
            return host

        account = os.getenv("SNOWFLAKE_ACCOUNT", "").lower()
        if account:
            return f"{account}.snowflakecomputing.com"

        raise RuntimeError(
            "Could not determine host. Set SNOWFLAKE_HOST environment variable."
        )

    def get_token(self) -> str:
        with open(self.TOKEN_PATH, "r") as f:
            return f.read().strip()

    def get_host(self) -> str:
        return self._host

    def get_auth_header(self) -> str:
        return f'Snowflake Token="{self.get_token()}"'


class PATTokenProvider(TokenProvider):
    """
    Token provider using Personal Access Token (PAT).

    PATs are the simplest way to authenticate from external systems.
    Generate a PAT in Snowsight under User Menu > My Profile > Personal Access Tokens.
    """

    def __init__(self, account: str, pat: str):
        self._account = account.lower()
        self._pat = pat

    def get_token(self) -> str:
        return self._pat

    def get_host(self) -> str:
        # Handle account locators with region
        if "." in self._account:
            return f"{self._account}.snowflakecomputing.com"
        return f"{self._account}.snowflakecomputing.com"

    def get_auth_header(self) -> str:
        return f"Bearer {self._pat}"


class KeyPairTokenProvider(TokenProvider):
    """
    Token provider using key-pair authentication.

    Generates JWT tokens using RSA private key. Ideal for Airflow and other
    external schedulers that need long-running authentication.
    """

    def __init__(self, account: str, user: str, private_key_path: str):
        self._account = account.lower()
        self._user = user
        self._private_key_path = private_key_path
        self._token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None

    def _generate_jwt(self) -> str:
        """Generate JWT token using private key."""
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization
        import jwt

        # Load private key
        with open(self._private_key_path, "rb") as f:
            private_key = serialization.load_pem_private_key(
                f.read(), password=None, backend=default_backend()
            )

        # Get public key fingerprint
        public_key = private_key.public_key()
        public_key_bytes = public_key.public_bytes(
            serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo
        )
        sha256_hash = hashlib.sha256(public_key_bytes).digest()
        public_key_fp = "SHA256:" + base64.b64encode(sha256_hash).decode("utf-8")

        # Generate JWT
        now = datetime.now(timezone.utc)
        lifetime = 3600  # 1 hour

        qualified_username = f"{self._account.upper()}.{self._user.upper()}"

        payload = {
            "iss": f"{qualified_username}.{public_key_fp}",
            "sub": qualified_username,
            "iat": now,
            "exp": now.timestamp() + lifetime,
        }

        token = jwt.encode(payload, private_key, algorithm="RS256")
        self._token = token
        self._token_expiry = datetime.fromtimestamp(
            now.timestamp() + lifetime, tz=timezone.utc
        )

        return token

    def get_token(self) -> str:
        if self._token and self._token_expiry:
            if datetime.now(timezone.utc) < self._token_expiry:
                return self._token
        return self._generate_jwt()

    def get_host(self) -> str:
        return f"{self._account}.snowflakecomputing.com"

    def get_auth_header(self) -> str:
        return f'Snowflake Token="{self.get_token()}"'


class AgentClient:
    """
    Production-ready Cortex Agent REST API client.

    Supports streaming responses and proper error handling.
    """

    def __init__(self, config: AgentConfig, token_provider: TokenProvider):
        self.config = config
        self.token_provider = token_provider

    @classmethod
    def from_session(
        cls,
        session,
        agent_name: str,
        database: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> "AgentClient":
        """
        Create client from Snowpark session.

        Args:
            session: Snowpark Session object
            agent_name: Name of the Cortex Agent
            database: Database containing agent (defaults to session's current database)
            schema: Schema containing agent (defaults to session's current schema)
        """
        provider = SessionTokenProvider(session)

        if not database:
            database = session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
        if not schema:
            schema = session.sql("SELECT CURRENT_SCHEMA()").collect()[0][0]

        config = AgentConfig(
            database=database,
            schema=schema,
            agent_name=agent_name,
            host=provider.get_host(),
        )

        return cls(config, provider)

    @classmethod
    def from_container(
        cls, agent_name: str, database: str, schema: str
    ) -> "AgentClient":
        """
        Create client for SPCS/Container Runtime environment.

        Note: As of Dec 2024, the container token may not be valid for
        Agent REST API calls. This will work when Snowflake enables it.
        """
        provider = ContainerTokenProvider()

        config = AgentConfig(
            database=database,
            schema=schema,
            agent_name=agent_name,
            host=provider.get_host(),
        )

        return cls(config, provider)

    @classmethod
    def from_pat(
        cls, agent_name: str, database: str, schema: str, account: str, pat: str
    ) -> "AgentClient":
        """
        Create client using Personal Access Token (PAT).

        PATs are the simplest authentication method for external systems.
        Generate in Snowsight: User Menu > My Profile > Personal Access Tokens

        Args:
            agent_name: Name of the Cortex Agent
            database: Database containing agent
            schema: Schema containing agent
            account: Snowflake account identifier (e.g., "xy12345" or "xy12345.us-west-2")
            pat: Personal Access Token string
        """
        provider = PATTokenProvider(account, pat)

        config = AgentConfig(
            database=database,
            schema=schema,
            agent_name=agent_name,
            host=provider.get_host(),
        )

        return cls(config, provider)

    @classmethod
    def from_key_pair(
        cls,
        agent_name: str,
        database: str,
        schema: str,
        account: str,
        user: str,
        private_key_path: str,
    ) -> "AgentClient":
        """
        Create client using key-pair authentication.

        Ideal for external schedulers like Airflow that need programmatic auth.

        Args:
            agent_name: Name of the Cortex Agent
            database: Database containing agent
            schema: Schema containing agent
            account: Snowflake account identifier
            user: Snowflake username
            private_key_path: Path to RSA private key file (PEM format)
        """
        provider = KeyPairTokenProvider(account, user, private_key_path)

        config = AgentConfig(
            database=database,
            schema=schema,
            agent_name=agent_name,
            host=provider.get_host(),
        )

        return cls(config, provider)

    def _build_headers(self) -> Dict[str, str]:
        """Build request headers with authentication."""
        return {
            "Authorization": self.token_provider.get_auth_header(),
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
        }

    def _build_body(self, question: str) -> Dict[str, Any]:
        """Build request body."""
        return {
            "messages": [
                {"role": "user", "content": [{"type": "text", "text": question}]}
            ]
        }

    def _parse_sse_stream(self, response: requests.Response) -> AgentResponse:
        """
        Parse Server-Sent Events stream from agent response.

        Extracts response.text events, filtering out thinking events.
        """
        events: List[Dict[str, Any]] = []
        tool_calls: List[Dict[str, Any]] = []
        text_parts: List[str] = []
        thinking_parts: List[str] = []
        current_event_type: Optional[str] = None

        for line in response.iter_lines(decode_unicode=True):
            if not line:
                continue

            if line.startswith("event:"):
                current_event_type = line[6:].strip()
                continue

            if not line.startswith("data:"):
                continue

            data = line[5:].strip()
            if data == "[DONE]":
                break

            try:
                event = json.loads(data)
                events.append(event)

                if current_event_type == "response.text.delta":
                    if "text" in event:
                        text_parts.append(event["text"])

                elif current_event_type == "response.text":
                    if "text" in event:
                        text_parts.append(event["text"])

                elif current_event_type == "response.thinking.delta":
                    if "text" in event:
                        thinking_parts.append(event["text"])

                elif current_event_type == "response.tool_use":
                    tool_calls.append(event)

                elif current_event_type == "response":
                    # Final response - use as authoritative
                    if "content" in event:
                        for item in event["content"]:
                            if item.get("type") == "text":
                                text_parts = [item.get("text", "")]

            except json.JSONDecodeError:
                continue

        return AgentResponse(
            text="".join(text_parts).strip(),
            raw_events=events,
            tool_calls=tool_calls,
            thinking="".join(thinking_parts),
        )

    def ask(self, question: str) -> str:
        """
        Ask the agent a question and return the text response.

        Args:
            question: The question to ask

        Returns:
            The agent's text response

        Raises:
            RuntimeError: If the API call fails
        """
        response = self.ask_detailed(question)
        return response.text

    def ask_detailed(self, question: str) -> AgentResponse:
        """
        Ask the agent a question and return detailed response.

        Args:
            question: The question to ask

        Returns:
            AgentResponse with text, events, tool calls, and thinking

        Raises:
            RuntimeError: If the API call fails
        """
        start_time = datetime.now()

        response = requests.post(
            self.config.endpoint,
            headers=self._build_headers(),
            json=self._build_body(question),
            stream=True,
            timeout=self.config.timeout,
        )

        if response.status_code == 401:
            raise RuntimeError(
                "Authentication failed (401). This may be due to:\n"
                "- Expired or invalid token/PAT\n"
                "- SPCS token not yet supported for Agent API (known gap as of Dec 2024)\n"
                "- Insufficient permissions on the agent"
            )

        if response.status_code != 200:
            raise RuntimeError(
                f"Agent API error: {response.status_code} - {response.text[:500]}"
            )

        result = self._parse_sse_stream(response)
        result.duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)

        return result

    def stream(self, question: str) -> Iterator[str]:
        """
        Stream the agent's response token by token.

        Yields text chunks as they arrive.

        Args:
            question: The question to ask

        Yields:
            Text chunks from the response
        """
        response = requests.post(
            self.config.endpoint,
            headers=self._build_headers(),
            json=self._build_body(question),
            stream=True,
            timeout=self.config.timeout,
        )

        if response.status_code != 200:
            raise RuntimeError(
                f"Agent API error: {response.status_code} - {response.text[:500]}"
            )

        current_event_type = None

        for line in response.iter_lines(decode_unicode=True):
            if not line:
                continue

            if line.startswith("event:"):
                current_event_type = line[6:].strip()
                continue

            if not line.startswith("data:"):
                continue

            data = line[5:].strip()
            if data == "[DONE]":
                break

            try:
                event = json.loads(data)

                if current_event_type == "response.text.delta":
                    if "text" in event:
                        yield event["text"]

            except json.JSONDecodeError:
                continue
