"""
Thread Context Manager

Manages conversation context for multi-turn conversations with TTL-based cleanup.
Prevents memory leaks from long-running bot instances.

Usage:
    from context import ConversationContext

    ctx = ConversationContext(ttl_hours=1, max_messages=10)
    ctx.add_user_message(thread_ts, "How many customers?")
    ctx.add_assistant_message(thread_ts, "We have 14 customers...")

    # Get history for next message
    history = ctx.get_history(thread_ts)
"""

import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class ThreadState:
    """State for a single conversation thread."""

    messages: List[Dict] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    last_used: float = field(default_factory=time.time)


class ConversationContext:
    """
    Thread-safe conversation context manager with TTL cleanup.

    Features:
    - Automatic cleanup of stale threads
    - Max message limit per thread
    - Thread-safe operations
    - Background cleanup thread
    """

    def __init__(
        self,
        ttl_hours: float = 1.0,
        max_messages: int = 10,
        cleanup_interval_minutes: float = 5.0,
    ):
        """
        Initialize context manager.

        Args:
            ttl_hours: Hours before a thread expires (default 1)
            max_messages: Max messages to keep per thread (default 10)
            cleanup_interval_minutes: How often to run cleanup (default 5)
        """
        self.ttl_seconds = ttl_hours * 3600
        self.max_messages = max_messages
        self.cleanup_interval = cleanup_interval_minutes * 60

        self._threads: Dict[str, ThreadState] = {}
        self._lock = threading.Lock()

        # Start background cleanup
        self._cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self._cleanup_thread.start()

    def get_history(self, thread_ts: str) -> List[Dict]:
        """
        Get conversation history for a thread.

        Args:
            thread_ts: Slack thread timestamp

        Returns:
            List of message dicts for the API
        """
        with self._lock:
            state = self._threads.get(thread_ts)
            if not state:
                return []

            state.last_used = time.time()
            return state.messages.copy()

    def add_user_message(self, thread_ts: str, text: str):
        """Add a user message to the thread."""
        message = {"role": "user", "content": [{"type": "text", "text": text}]}
        self._add_message(thread_ts, message)

    def add_assistant_message(self, thread_ts: str, text: str):
        """Add an assistant response to the thread."""
        message = {"role": "assistant", "content": [{"type": "text", "text": text}]}
        self._add_message(thread_ts, message)

    def _add_message(self, thread_ts: str, message: Dict):
        """Add a message to the thread (internal)."""
        with self._lock:
            if thread_ts not in self._threads:
                self._threads[thread_ts] = ThreadState()

            state = self._threads[thread_ts]
            state.messages.append(message)
            state.last_used = time.time()

            # Trim to max messages
            if len(state.messages) > self.max_messages:
                state.messages = state.messages[-self.max_messages :]

    def has_context(self, thread_ts: str) -> bool:
        """Check if thread has existing context."""
        with self._lock:
            return thread_ts in self._threads

    def clear_thread(self, thread_ts: str):
        """Clear context for a specific thread."""
        with self._lock:
            self._threads.pop(thread_ts, None)

    def get_stats(self) -> Dict:
        """Get context manager statistics."""
        with self._lock:
            return {
                "active_threads": len(self._threads),
                "total_messages": sum(
                    len(state.messages) for state in self._threads.values()
                ),
            }

    def _cleanup_loop(self):
        """Background cleanup loop."""
        while True:
            time.sleep(self.cleanup_interval)
            self._cleanup_expired()

    def _cleanup_expired(self):
        """Remove expired threads."""
        now = time.time()
        expired = []

        with self._lock:
            for thread_ts, state in self._threads.items():
                if now - state.last_used > self.ttl_seconds:
                    expired.append(thread_ts)

            for thread_ts in expired:
                del self._threads[thread_ts]

        if expired:
            print(f"🧹 Cleaned up {len(expired)} expired conversation thread(s)")


# Singleton instance for global use
_default_context: Optional[ConversationContext] = None


def get_context(ttl_hours: float = 1.0, max_messages: int = 10) -> ConversationContext:
    """Get or create the default context manager."""
    global _default_context

    if _default_context is None:
        _default_context = ConversationContext(
            ttl_hours=ttl_hours, max_messages=max_messages
        )

    return _default_context
