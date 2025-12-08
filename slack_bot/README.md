# Snowflake Intelligence Slack Bot 🎿

A clean, modular Slack bot that connects your team to Snowflake Cortex Agents.

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure environment
export SNOWFLAKE_ACCOUNT="your-account"
export SNOWFLAKE_PAT="your-pat-token"
export SLACK_BOT_TOKEN="xoxb-..."
export SLACK_APP_TOKEN="xapp-..."

# 3. Run the bot
python bot.py

# Or test without Slack
python bot.py --test "How many customers do we have?"
```

## Features

- ✅ **Multi-turn conversations** - Full context in threads
- ✅ **Smart progress updates** - Updates in place, no spam
- ✅ **Data tables** - Formatted query results
- ✅ **SQL display** - See generated queries
- ✅ **Chart rendering** - Vega-Lite to PNG
- ✅ **CSV export** - Download data files
- ✅ **Thread context TTL** - No memory leaks
- ✅ **Config-driven** - Easy to customize

## Architecture

```
slack_bot/
├── bot.py           # Main entry point & Slack handlers
├── agent.py         # Cortex Agent REST client
├── formatters.py    # Slack Block Kit formatting
├── context.py       # Thread context with TTL
├── config.yml       # Configuration
└── requirements.txt
```

**Simple by design** - Snowflake agents handle the intelligence, we handle the UX.

## Configuration

Edit `config.yml` to customize agents and behavior:

```yaml
agents:
  default:
    name: RESORT_EXECUTIVE_DEV
    database: SKI_RESORT_DB
    schema: AGENTS
    emoji: "🎿"

  # Add more agents as needed
  operations:
    name: LIFT_OPERATIONS_AGENT
    database: SKI_RESORT_DB
    schema: AGENTS
    emoji: "🚡"
```

## Slack Setup

### 1. Create Slack App

1. Go to https://api.slack.com/apps
2. Click **"Create New App"** → **"From an app manifest"**
3. Paste contents of `slack_app_manifest.yml`
4. Install to your workspace

### 2. Get Tokens

- **Bot Token**: OAuth & Permissions → Bot User OAuth Token (`xoxb-...`)
- **App Token**: Basic Information → App-Level Tokens (`xapp-...`)

### 3. Configure Environment

```bash
export SLACK_BOT_TOKEN="xoxb-your-bot-token"
export SLACK_APP_TOKEN="xapp-your-app-token"
```

## Usage

### In Slack

**Slash command:**
```
/ask What was our revenue last month?
```

**Direct message:**
> How many customers do we have?

**Thread follow-ups:**
> @bot Which segment is most profitable?

### Command Line Testing

```bash
# Test agent connection
python bot.py --test "Hello, are you working?"

# Test specific question
python bot.py --test "Show me daily revenue for December"
```

### Python API

```python
from agent import AgentClient

client = AgentClient.from_env(
    agent_name="RESORT_EXECUTIVE_DEV",
    database="SKI_RESORT_DB",
    schema="AGENTS"
)

result = client.ask("What is total revenue?")
print(result.answer)
print(result.sql)
```

## Module Details

### `agent.py` - Agent Client

Clean REST client for Cortex Agents:

```python
from agent import AgentClient, AgentResult

# From config
client = AgentClient.from_config(config['agents']['default'])

# From environment
client = AgentClient.from_env("MY_AGENT", "MY_DB", "MY_SCHEMA")

# Ask with progress
result = client.ask(
    "How many customers?",
    progress_callback=lambda s: print(f"Status: {s}")
)

# Access results
print(result.answer)      # Text response
print(result.sql)         # Generated SQL
print(result.has_data)    # Has result_set?
print(result.chart_specs) # Vega-Lite charts
```

### `formatters.py` - Slack Formatting

Converts agent results to Slack blocks:

```python
from formatters import SlackFormatter, ChartRenderer

formatter = SlackFormatter(
    max_response_length=2500,
    max_table_rows=15,
    show_sql=True
)

blocks = formatter.format_response(result, question)

# Chart rendering
renderer = ChartRenderer()
if renderer.available:
    png = renderer.render(chart_spec)
```

### `context.py` - Thread Context

Thread-safe conversation context with auto-cleanup:

```python
from context import ConversationContext

ctx = ConversationContext(
    ttl_hours=1,        # Expire after 1 hour
    max_messages=10     # Keep last 10 messages
)

# Get history for multi-turn
history = ctx.get_history(thread_ts)

# Add messages
ctx.add_user_message(thread_ts, "Question?")
ctx.add_assistant_message(thread_ts, "Answer!")

# Stats
print(ctx.get_stats())  # {"active_threads": 5, "total_messages": 23}
```

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `SNOWFLAKE_ACCOUNT` | Yes | Snowflake account identifier |
| `SNOWFLAKE_PAT` | Yes | Personal Access Token |
| `SLACK_BOT_TOKEN` | For Slack | Bot OAuth token (`xoxb-...`) |
| `SLACK_APP_TOKEN` | For Slack | App-level token (`xapp-...`) |
| `AGENT_NAME` | No | Default agent name |
| `AGENT_DATABASE` | No | Default database |
| `AGENT_SCHEMA` | No | Default schema |

## Troubleshooting

### Bot doesn't respond
1. Check environment variables are set
2. Run `python bot.py --test "test"` to verify agent connection
3. Ensure bot is invited to the channel

### No data tables
- Check if agent query returned results
- Look at logs for `result_set` extraction

### Charts not rendering
- Install: `pip install altair vl-convert-python`
- Check logs for Vega-Lite parsing errors

### Memory usage growing
- Context auto-cleans after 1 hour (configurable)
- Check `context.get_stats()` for active threads

## Development

### File Structure

```
bot.py          # ~300 lines - Main bot, Slack handlers
agent.py        # ~250 lines - Agent REST client
formatters.py   # ~200 lines - Slack formatting
context.py      # ~120 lines - Thread context
config.yml      # ~50 lines  - Configuration
```

Total: ~900 lines (vs 1000+ in original single file)

### Testing

```bash
# Test agent client
python -c "from agent import ask; print(ask('test'))"

# Test formatter
python -c "from formatters import SlackFormatter; print(SlackFormatter())"

# Test context
python -c "from context import ConversationContext; print(ConversationContext().get_stats())"
```

### Adding New Agents

1. Add agent config to `config.yml`:
   ```yaml
   agents:
     myagent:
       name: MY_AGENT_NAME
       database: MY_DB
       schema: MY_SCHEMA
       emoji: "🤖"
   ```

2. Use in code:
   ```python
   client = get_agent_client("myagent")
   ```

## Legacy Files

The original `simple_bot.py` is preserved for reference but the new modular structure is recommended:

| Old | New |
|-----|-----|
| `simple_bot.py` | `bot.py` + `agent.py` + `formatters.py` + `context.py` |
| Hardcoded config | `config.yml` |
| In-memory dict | `ConversationContext` with TTL |
| ~1000 lines | ~900 lines across 4 files |

---

**Simple by design** - Let Snowflake handle the AI, we handle the Slack UX! 🎿
