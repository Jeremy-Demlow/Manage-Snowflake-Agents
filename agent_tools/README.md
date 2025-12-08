# Agent Tools - Snowpark Deployment

Custom tools library for Snowflake Intelligence agents using **Snowflake CLI + Snowpark** architecture.

## 🏗️ **Project Structure**

```
agent_tools/
├── snowflake.yml          # Snowpark project configuration
├── requirements.txt       # Python dependencies
├── setup.py              # Package configuration
│
├── src/                  # Tool source code
│   ├── email_tools/
│   │   ├── __init__.py  
│   │   └── sender.py     # Email sender implementation
│   └── web_tools/
│       ├── __init__.py
│       ├── core.py       # Web scraping core functionality
│       ├── analyzer.py   # AI analysis with Claude-3.5-Sonnet
│       └── scraper.py    # Main scraper entry point
│
├── tests/                # Local testing framework
│   ├── test_email_sender.py
│   └── test_web_scraper.py
│
└── test_local_scraper.py # Standalone local testing script
```

## 🚀 **Snowflake CLI + Snowpark Deployment**

### Prerequisites

1. **Snowflake CLI v3.7+**:
   ```bash
   pip install snowflake-cli-labs
   snow connection add  # Configure with appropriate role
   ```

2. **Python Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

### Deploy Tools to Snowflake

```bash
# Build artifacts (creates src.zip)
snow snowpark build

# Deploy procedures to Snowflake
snow snowpark deploy

# Deploy to specific connection
snow snowpark deploy --connection my_connection
```

**Deployment Target**: `AGENT_TOOLS_CENTRAL.AGENT_TOOLS`
- Creates database `AGENT_TOOLS_CENTRAL` if needed
- Creates schema `AGENT_TOOLS` if needed  
- Creates stage `AGENT_TOOLS_STAGE` for artifacts
- Deploys procedures: `SEND_MAIL`, `WEB_SCRAPE`

## 📋 **snowflake.yml Configuration**

Our Snowpark project is defined in `snowflake.yml`:

```yaml
definition_version: 2

env:
  database: "AGENT_TOOLS_CENTRAL"
  schema: "AGENT_TOOLS"
  warehouse: "HOL_WAREHOUSE"
  stage: "AGENT_TOOLS_STAGE"

entities:
  send_mail:
    type: "procedure"
    handler: "email_tools.sender.send_email_for_agent"
    signature:
      - name: "recipient"
        type: "TEXT"
      - name: "subject"
        type: "TEXT"
      - name: "text"
        type: "TEXT"
    returns: "TEXT"
    stage: "AGENT_TOOLS_STAGE"
    artifacts: ["src/"]

  web_scrape:
    type: "procedure"
    handler: "web_tools.scraper.scrape_website_for_agent"
    signature:
      - name: "url"
        type: "TEXT"
    returns: "TEXT"
    stage: "AGENT_TOOLS_STAGE"
    artifacts: ["src/"]
    external_access_integrations: ["AGENT_TOOLS_EXTERNAL_ACCESS_INTEGRATION"]
```

### Key Configuration Points

- **Centralized Deployment**: All tools deploy to shared `AGENT_TOOLS_CENTRAL` database
- **Stage Management**: Automatically managed via `AGENT_TOOLS_STAGE`
- **External Access**: Web scraper requires `AGENT_TOOLS_EXTERNAL_ACCESS_INTEGRATION`
- **Clean Handlers**: Each tool has dedicated handler function in `src/`

## 🛠️ **Available Tools**

### 1. Email Sender (`SEND_MAIL`)

**Purpose**: Send emails from Snowflake Intelligence agents

**Usage**:
```sql
CALL AGENT_TOOLS_CENTRAL.AGENT_TOOLS.SEND_MAIL(
    'recipient@example.com',
    'Subject Line',
    '<h1>HTML Content</h1><p>Email body here</p>'
);
```

**Features**:
- HTML email support
- Uses Snowflake notification integrations
- Error handling and logging
- Agent-friendly return format

### 2. Web Scraper (`WEB_SCRAPE`)

**Purpose**: AI-powered competitive intelligence and market research

**Usage**:
```sql
CALL AGENT_TOOLS_CENTRAL.AGENT_TOOLS.WEB_SCRAPE('https://example.com');
```

**Features**:
- **Claude-3.5-Sonnet AI**: 10,000 token analysis
- **Real Competitor Detection**: Industry-specific competitor databases  
- **Rich HTML Reports**: Formatted for agent consumption
- **External Access**: Secure web scraping via Snowflake integrations
- **Content Extraction**: Up to 5,000 characters with smart summarization

## 🧪 **Local Testing**

### Run Local Tests
```bash
# Full test suite
python -m pytest tests/

# Individual tool tests  
python -m pytest tests/test_email_sender.py
python -m pytest tests/test_web_scraper.py

# Standalone web scraper testing
python test_local_scraper.py
```

### Local Testing Features
- **No Snowflake Required**: Tools gracefully handle missing Snowflake session
- **Real Data Processing**: Tests use actual web scraping and analysis
- **Development Friendly**: Quick iteration without deployment

## 🔗 **Agent Integration**

### Add Tools to Agent YAML Configuration

```yaml
# In snowflake_agents/agent_configs/your_agent.yml
tools:
  - name: "Send_Email"
    type: "generic"
    description: "Send emails to recipients with HTML content"
    input_schema:
      type: "object"
      properties:
        recipient: { type: "string" }
        subject: { type: "string" }
        text: { type: "string" }
      required: ["recipient", "subject", "text"]
    resource:
      type: "procedure"
      identifier: "AGENT_TOOLS_CENTRAL.AGENT_TOOLS.SEND_MAIL"
      name: "SEND_MAIL(VARCHAR, VARCHAR, VARCHAR)"

  - name: "Web_scrape"
    type: "generic"  
    description: "AI-powered competitive intelligence and web analysis"
    input_schema:
      type: "object"
      properties:
        url: { type: "string" }
      required: ["url"]
    resource:
      type: "procedure"
      identifier: "AGENT_TOOLS_CENTRAL.AGENT_TOOLS.WEB_SCRAPE"
      name: "WEB_SCRAPE(VARCHAR)"
```

## 🔧 **Development Workflow**

### 1. Develop Locally
```bash
# Edit tool source code in src/
# Test locally without Snowflake
python test_local_scraper.py
python -m pytest tests/
```

### 2. Deploy to Snowflake  
```bash
# Build and deploy in one command
snow snowpark deploy

# Or step by step
snow snowpark build    # Creates src.zip artifact
snow snowpark deploy   # Deploys procedures
```

### 3. Test in Snowflake
```bash
# Test email tool
snow sql -q "CALL AGENT_TOOLS_CENTRAL.AGENT_TOOLS.SEND_MAIL('test@example.com', 'Test', 'Hello!')"

# Test web scraper  
snow sql -q "CALL AGENT_TOOLS_CENTRAL.AGENT_TOOLS.WEB_SCRAPE('https://amce.com')"
```

### 4. Update Agent Configuration
```bash
cd ../snowflake_agents
python agent_generator.py --agent your_agent
python manage_agents.py deploy your_agent
```

## 📦 **Adding New Tools**

### 1. Create Tool Module
```bash
mkdir src/your_tool
touch src/your_tool/__init__.py  
touch src/your_tool/tool_logic.py
```

### 2. Implement Handler Function
```python
# src/your_tool/tool_logic.py
def your_tool_for_agent(session, param1: str, param2: str) -> str:
    """
    Tool handler for Snowflake agent integration

    Args:
        session: Snowflake session (automatically provided)
        param1, param2: Tool parameters

    Returns:
        str: Tool result (JSON or HTML formatted)
    """
    # Your tool logic here
    return result
```

### 3. Add to snowflake.yml
```yaml
entities:
  your_tool:
    type: "procedure"
    handler: "your_tool.tool_logic.your_tool_for_agent"
    signature:
      - name: "param1"
        type: "TEXT"
      - name: "param2"  
        type: "TEXT"
    returns: "TEXT"
    stage: "AGENT_TOOLS_STAGE"
    artifacts: ["src/"]
    # Add external_access_integrations if needed
```

### 4. Deploy
```bash
snow snowpark deploy
```

## 🚨 **Common Issues & Solutions**

### "Object does not exist" Errors
- **Cause**: Insufficient permissions or database doesn't exist
- **Solution**: Ensure your Snowflake connection has `CREATE DATABASE` privileges

### Build Failures
```bash
# Clean and rebuild
rm -f src.zip
snow snowpark build
```

### External Access Issues (Web Scraper)
- **Cause**: `AGENT_TOOLS_EXTERNAL_ACCESS_INTEGRATION` not created
- **Solution**: Contact admin to create external access integration for web endpoints

### Permission Errors
- **Cause**: Agent can't access `AGENT_TOOLS_CENTRAL.AGENT_TOOLS.*`
- **Solution**: Grant usage permissions to agent's execution role

## 🎯 **Best Practices**

### Tool Development
- ✅ **Single Responsibility**: Each tool does one thing well
- ✅ **Error Handling**: Graceful failures with informative messages  
- ✅ **Agent-Friendly Output**: Return formatted strings (JSON/HTML)
- ✅ **Local Testing**: Test without Snowflake dependencies

### Deployment
- ✅ **Centralized Tools**: Deploy to `AGENT_TOOLS_CENTRAL` for sharing
- ✅ **Version Control**: Use `snow snowpark build` for consistent artifacts
- ✅ **Environment Management**: Use different connections for dev/prod

### Integration  
- ✅ **Clear Documentation**: Document tool parameters and return format
- ✅ **YAML Configuration**: Use agent YAML configs for maintainable setup
- ✅ **Separation of Concerns**: Tools ≠ Agents ≠ Data Models

---

This Snowpark project enables **scalable, maintainable tool development** for Snowflake Intelligence agents with clean separation of concerns and professional deployment practices.
