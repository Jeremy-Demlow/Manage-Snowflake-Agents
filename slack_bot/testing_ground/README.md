# Testing Ground - Understanding Snowflake Agent API Responses

This directory contains simple scripts to test and understand the actual behavior of Snowflake Cortex Agents before building the full Slack bot.

## Purpose

Before building a complex Slack integration, we need to understand:
1. What the actual Agent API responses look like
2. How the streaming Server-Sent Events work
3. What JSON structure we get back
4. How the orchestration layer really behaves
5. What errors we might encounter

## Files

- `simple_agent_test.py` - Basic script to test agent connectivity and responses
- `api_response_analyzer.py` - Captures and analyzes API response structures
- `examples.md` - Documents our findings and learnings
- `test_questions.json` - Sample questions to test different agent routing

## Usage

```bash
# Test basic connectivity
python simple_agent_test.py

# Analyze API responses in detail  
python api_response_analyzer.py

# Test specific questions
python api_response_analyzer.py --questions test_questions.json
```

## Goal

Understand the real API behavior so we can build a Slack bot that actually works with the real responses, not what we think they might look like.
