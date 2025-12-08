# Slack Bot - Developer Notes

## Overview

A production Slack bot that connects users to Snowflake Cortex Agents. Users ask questions in natural language, the bot calls Snowflake's Intelligence layers, and returns formatted answers with SQL and metadata.

## Architecture

```
User in Slack → Slack Bolt App → Snowflake Agent API → Streaming Response → Parse → Format → Slack
```

**Key Insight:** We don't do the intelligence - Snowflake's agents handle:
- Semantic model selection
- Tool orchestration (Cortex Analyst, Cortex Search)
- SQL generation
- Multi-turn context

We just call the API and format responses.

## Core Functions

### `format_for_slack(answer, question)`
**Purpose:** Clean up verbose agent responses for Slack display  
**Input:** Raw agent response text  
**Output:** Cleaned, Slack-formatted text  
**What it does:**
- Removes overly verbose explanations
- Converts Markdown `**bold**` to Slack `*bold*`
- Truncates to 2500 chars if needed
- Keeps key sections and bullet points

**Why:** Snowflake agents are verbose and executive-focused. Slack users want concise answers.

### `ask_agent(question, agent, conversation_history, progress_callback)`
**Purpose:** Core function to call Snowflake Agent API  
**Input:**
- `question`: Natural language question
- `agent`: Which agent ("intelligence", "contracts", "perf")
- `conversation_history`: List of previous Q&A for multi-turn
- `progress_callback`: Function to call with status updates

**Output:** Dict with:
- `answer`: The response text
- `sql`: Generated SQL query (extracted from execution_trace)
- `tools_used`: List of tools called
- `thinking`: Agent's reasoning (for debugging)
- `raw_events`: All streaming events

**What it does:**
1. Builds payload with conversation history for multi-turn
2. Calls Snowflake Agent Object API endpoint
3. Parses Server-Sent Events stream
4. Extracts answer, SQL, tools, and metadata
5. Calls progress_callback for status updates

**Key Details:**
- Uses PAT authentication with `PROGRAMMATIC_ACCESS_TOKEN` header
- Streams response to show progress
- Extracts SQL from `execution_trace` event (not tool_result!)
- Multi-turn works by passing full conversation history

### `ask(question, agent)`
**Purpose:** Simple wrapper for basic usage  
**Input:** Question and optional agent  
**Output:** Just the answer text (string)  
**Why:** Simple API for non-Slack usage (notebooks, scripts)

## Slack Handlers

### `handle_message(message, say)`
**Purpose:** Handle @mentions and DMs, including multi-turn in threads  
**Triggers:** @bot mentions, DMs, replies in threads we created  

**Flow:**
1. Check if it's our thread or @mention
2. Get conversation history from `thread_context` dict
3. Show smart progress (key milestones + every 5s)
4. Call `ask_agent()` with history for context
5. Format and post answer
6. Save conversation history for follow-ups

**Multi-Turn Magic:**
```python
thread_context[thread_ts] = [
  {"role": "user", "content": [{"text": "Q1"}]},
  {"role": "assistant", "content": [{"text": "A1"}]},
  {"role": "user", "content": [{"text": "Q2"}]}  # Has context!
]
```

### `handle_ask_acme(ack, command, say, respond)`
**Purpose:** Handle `/ask-acme` slash command  
**Why separate:** Creates visible thread so everyone sees the question

**Flow:**
1. `ack()` immediately (must be < 3s to avoid Slack timeout)
2. Post question publicly with Slack blocks (makes it bold/visible)
3. Create thread
4. Call agent
5. Post answer in thread with SQL and metadata
6. Save context for follow-ups

**Key:** Slash commands can't be used in threads (Slack limitation), so we guide users to @mention for follow-ups.

### `handle_contracts(ack, command, respond)`
**Purpose:** Specialized `/contracts` command  
**Same pattern:** ack → call agent("contracts") → respond

### `handle_perf(ack, command, respond)`
**Purpose:** Specialized `/perf` command  
**Same pattern:** ack → call agent("perf") → respond

## Data Structures

### `thread_context` (dict)
**Purpose:** Store conversation history for multi-turn  
**Key:** Slack `thread_ts` (timestamp)  
**Value:** List of messages in Snowflake format
```python
{
  "1234567890.123456": [
    {"role": "user", "content": [...]},
    {"role": "assistant", "content": [...]},
    ...
  ]
}
```

**Note:** Keeps last 10 messages to avoid token limits

### `AGENTS` (dict)
Maps friendly names to Snowflake agent names:
```python
{
  "intelligence": "ACME_INTELLIGENCE_AGENT",
  "contracts": "ACME_CONTRACTS_AGENT",
  "perf": "DATA_ENGINEER_ASSISTANT"
}
```

## Key Technical Decisions

### 1. Conversation History vs thread_id
**Tried:** Snowflake's `thread_id` and `parent_message_id` parameters  
**Problem:** Response doesn't include these, hard to track  
**Solution:** Pass full conversation history in `messages` array  
**Result:** Multi-turn works reliably!

### 2. Progress Updates
**Tried:** Show every status event  
**Problem:** 40+ messages, UX nightmare  
**Solution:** Show key milestones + updates every 5s  
**Result:** Users stay informed without annoyance

### 3. SQL Extraction
**Tried:** `response.tool_result`, `response.tool_use`  
**Problem:** SQL not in those events  
**Solution:** Extract from `execution_trace` event  
**Result:** Shows actual generated SQL!

### 4. Slack Formatting
**Tried:** Let Snowflake's Markdown pass through  
**Problem:** `**bold**` doesn't work in Slack  
**Solution:** Convert to `*bold*` and clean verbose text  
**Result:** Clean, readable responses

## Configuration

### Environment Variables Required
```bash
SNOWFLAKE_ACCOUNT="trb65519"
SNOWFLAKE_PAT="your-pat-token"
SLACK_BOT_TOKEN="xoxb-..."
SLACK_APP_TOKEN="xapp-..."
```

### Snowflake Setup Required
- Agent Object API endpoint accessible
- PAT token with proper permissions
- Agents deployed: ACME_INTELLIGENCE_AGENT, etc.

## Testing Approach

### Development Flow
1. Test API manually in `testing_ground/`
2. Capture real responses
3. Build based on actual behavior
4. Run `test_bot.py` health check
5. Deploy and iterate

### Testing Ground
Contains scripts that:
- Test raw Agent API calls
- Capture streaming responses
- Document real JSON structures
- Validate multi-turn behavior

**Key files:**
- `test_with_pat.py` - Working API test
- `examples.md` - Real response documentation

## Known Issues & Limitations

### Current Limitations
1. **Thread context never expires** - Memory leak for long-running bot
2. **No rate limiting** - Could hit Snowflake/Slack limits
3. **No retry logic** - Transient failures aren't retried
4. **No usage analytics** - Can't track what users ask
5. **Progress in separate messages** - Can't update one message
6. **Slash commands in threads** - Slack limitation, can't fix

### Error Handling
✅ Empty responses caught  
✅ Exceptions logged  
✅ User-friendly error messages  
❌ No retry on transient failures  
❌ No fallback agents

## Future Improvements

### High Priority

**1. Update Progress in Place**
Instead of posting new messages, update ONE message:
```python
progress_msg = say("🤔 Analyzing...")
# Later:
app.client.chat_update(
    channel=channel,
    ts=progress_msg['ts'],
    text="⚡ Executing SQL..."
)
```

**2. Rich Slack Blocks**
```python
blocks = [
    {"type": "section", "text": {"type": "mrkdwn", "text": answer}},
    {"type": "divider"},
    {"type": "context", "elements": [{"type": "mrkdwn", "text": f"⏱️ {elapsed}s"}]}
]
```

**3. Interactive Buttons**
```python
{
    "type": "actions",
    "elements": [
        {"type": "button", "text": "Show SQL", "action_id": "show_sql"},
        {"type": "button", "text": "Ask Follow-up", "action_id": "followup"}
    ]
}
```

**4. Thread Context Expiry**
```python
# Clean up threads older than 1 hour
def cleanup_old_threads():
    now = time.time()
    for thread_ts, data in list(thread_context.items()):
        if now - data.get('last_used', 0) > 3600:
            del thread_context[thread_ts]
```

**5. Better SQL Display**
Create a code snippet instead of inline:
```python
app.client.files_upload(
    channels=channel_id,
    content=result["sql"],
    filename="query.sql",
    thread_ts=thread_ts
)
```

### Medium Priority

**6. Usage Analytics**
Log questions, response times, tools used to database or file.

**7. Retry Logic**
```python
@retry(stop=stop_after_attempt(3), wait=wait_exponential())
def ask_agent_with_retry(...):
    return ask_agent(...)
```

**8. Rate Limiting**
```python
from ratelimit import limits, sleep_and_retry

@sleep_and_retry
@limits(calls=10, period=60)
def ask_agent(...):
    ...
```

**9. Conversation Context in Slack UI**
Show what context the bot is using:
```
💭 Context: Using conversation from last 3 messages
```

**10. Agent Performance Metrics**
```
_⏱️ 15.7s • Model: claude-3-7-sonnet • Tools: cortex_analyst • Tokens: ~1.2k_
```

### Low Priority

**11. Caching**
```python
from functools import lru_cache

@lru_cache(maxsize=100)
def ask_agent_cached(question, agent):
    ...
```

**12. Slash Command Help**
```
/ask-acme help
→ Shows example questions and tips
```

**13. Export Data**
Button to export query results as CSV.

**14. Schedule Reports**
```
/ask-acme schedule daily "Show me yesterday's revenue"
```

**15. Admin Commands**
```
/ask-acme stats  # Show bot usage statistics
/ask-acme clear  # Clear thread context
```

## Performance Characteristics

### Response Times (Observed)
- Simple queries: 10-20s
- Complex multi-tool: 40-60s
- Multi-turn follow-ups: 15-25s

### Bottlenecks
1. **Snowflake orchestration** (5-10s) - Planning and tool selection
2. **SQL generation** (3-7s) - Cortex Analyst
3. **SQL execution** (2-10s) - Query complexity
4. **Response formatting** (3-5s) - Agent writes answer

**We can't optimize Snowflake** - focus on UX to make wait feel shorter!

## Maintenance

### Regular Tasks
- Monitor thread_context size (memory leak risk)
- Review error logs for patterns
- Update agent configurations in Snowflake
- Test multi-turn scenarios

### When Things Break
1. Check logs: `tail -f bot_logs.txt`
2. Run health check: `python test_bot.py`
3. Test API directly: `cd testing_ground && python test_with_pat.py`
4. Check Snowflake agents: `snow sql -q "SHOW AGENTS..."`

### Deployment Checklist
- [ ] Run `python test_bot.py`
- [ ] Check environment variables set
- [ ] Verify Snowflake PAT is valid
- [ ] Test slash command in Slack
- [ ] Test @mention
- [ ] Test multi-turn conversation
- [ ] Monitor logs for errors

## What We Learned

### Key Insights
1. **Test the real API first** - Don't assume, capture actual responses
2. **Let Snowflake do the intelligence** - One agent handles everything
3. **Conversation history > thread_id** - More reliable for multi-turn
4. **Progress matters** - But balance info vs spam
5. **Slack formatting is different** - `*bold*` not `**bold**`

### Common Mistakes to Avoid
- ❌ Using `**text**` in Slack (doesn't work)
- ❌ Not calling `ack()` in slash commands (timeout)
- ❌ Showing every status update (UX nightmare)
- ❌ Forgetting to import modules at top level
- ❌ Not testing before deploying

### What Actually Matters
- ✅ Multi-turn conversations (conversation history)
- ✅ Clean, concise responses
- ✅ Showing progress without spam
- ✅ Displaying SQL for transparency
- ✅ Fast acknowledgment to avoid timeouts

## Code Map

### Initialization (Lines 1-50)
- Imports and logging setup
- Configuration from environment variables
- Agent mappings
- Thread context storage

### Utilities (Lines 51-110)
- `format_for_slack()` - Response formatting
- Helper functions

### Core API (Lines 111-220)
- `ask_agent()` - Main Snowflake API call
- Streaming response parsing
- SQL extraction
- Progress callbacks

### Slack Integration (Lines 221-550)
- Message handler (for @mentions and threads)
- Slash command handlers (3 commands)
- Progress tracking
- Thread management

### Main (Lines 551-end)
- Bot startup
- Health checks
- Error handling

## File Purposes

### `simple_bot.py` (621 lines)
The entire bot. Everything you need.

### `slack_app_manifest.yml`
Slack app configuration - paste into Slack to create app instantly.

### `test_bot.py`
Health check script - run before deploying to catch issues early.

### `quick_start.ipynb`
Interactive notebook for testing the `ask()` function without Slack.

### `testing_ground/`
Research folder with:
- API response examples
- Test scripts
- Documentation of real behavior

## Future Improvements (Prioritized)

### Must Have (Before Production)
1. **Thread context expiry** - Currently leaks memory
2. **Update progress in place** - Use `chat_update` not new messages
3. **Error retry logic** - Handle transient failures
4. **Better SQL formatting** - Use code snippets or files

### Should Have
5. **Rich Slack blocks** - Better visual structure
6. **Interactive buttons** - "Show SQL", "Export Data", etc.
7. **Usage analytics** - Track what users ask
8. **Response caching** - Same question = instant answer
9. **Agent performance metrics** - Which tools, how long, token count

### Nice to Have
10. **Slash command help** - `/ask-acme help`
11. **Scheduled reports** - Daily/weekly summaries
12. **Admin commands** - Stats, clear cache, etc.
13. **Chart support** - Display charts from agent responses
14. **Citation display** - Show data sources used

## Debug Tips

### If multi-turn doesn't work:
```python
# Check thread context
logger.info(f"Thread context: {thread_context.get(thread_ts, 'None')}")

# Verify conversation history being sent
logger.info(f"Sending {len(messages)} messages to Snowflake")
```

### If SQL doesn't show:
```python
# Check if execution_trace event exists
logger.info(f"Event types: {[e.get('event_type') for e in result['raw_events']]}")

# Look for SQL in events
for event in result['raw_events']:
    if 'sql' in str(event).lower():
        logger.info(f"Found SQL reference: {event}")
```

### If progress spam returns:
```python
# Check timing logic
logger.info(f"Time since last update: {now - last_update}")
logger.info(f"Is milestone: {is_milestone}")
```

## API Details

### Endpoint
```
POST https://trb65519.snowflakecomputing.com/api/v2/databases/SNOWFLAKE_INTELLIGENCE/schemas/AGENTS/agents/ACME_INTELLIGENCE_AGENT:run
```

### Request Format
```json
{
  "messages": [
    {"role": "user", "content": [{"type": "text", "text": "Question"}]},
    {"role": "assistant", "content": [{"type": "text", "text": "Previous answer"}]},
    {"role": "user", "content": [{"type": "text", "text": "Follow-up"}]}
  ]
}
```

### Response Format (Server-Sent Events)
```
event: response.status
data: {"message":"Planning...","status":"planning"}

event: response.text.delta
data: {"content_index":4,"text":"Based on our data..."}

event: execution_trace
data: [...contains SQL in nested JSON...]

event: response
data: {"role":"assistant","content":[...]}

event: done
data: [DONE]
```

### Key Events
- `response.status` - Progress updates
- `response.text.delta` - Answer tokens
- `response.tool_use` - Tool invocations
- `execution_trace` - Contains SQL!
- `response` - Final aggregated response

## Testing

### Test Locally
```bash
# Health check
python test_bot.py

# Interactive testing
jupyter notebook quick_start.ipynb

# Run bot
python simple_bot.py
```

### Test in Slack
```
# Basic
/ask-acme How many customers do we have?

# Multi-turn
@bot Who are those customers?
@bot Which one has highest revenue?

# Different agents
/contracts Show me at-risk contracts
/perf What are my slowest queries?
```

## Dependencies

### Required
- `requests` - HTTP calls to Snowflake
- `slack-bolt` - Slack integration

### Built-in
- `time` - Timing and throttling
- `json` - JSON parsing
- `re` - Regex for patterns
- `logging` - Debug and monitoring

## Environment

### Development
- Conda environment: `service_titan`
- Python 3.11+
- Socket Mode (no public endpoint needed)

### Production Considerations
- Deploy to cloud (AWS Lambda, Cloud Run, etc.)
- Use Events API instead of Socket Mode
- Secure token storage (not environment vars)
- Add monitoring and alerting
- Scale horizontally if needed

## Success Metrics

### What's Working
✅ Multi-turn conversations maintain context  
✅ SQL displays correctly  
✅ Smart progress updates  
✅ Responses are clean and formatted  
✅ Timing shown on all responses  
✅ Tools used displayed  
✅ No dispatch_failed errors

### What to Monitor
- Average response time
- Error rate
- Most common questions
- Multi-turn usage
- Which agents/tools used most

---

**This bot is simple by design** - Let Snowflake do the hard work, we just make it accessible in Slack! 🎯
