"""
Conversation Report Tool - Send SI conversations as emails with PowerPoint attachments.
Uses presigned URLs for attachment delivery (Snowflake email doesn't support direct attachments).

Deployed via snowflake.yml as: AGENT_TOOLS_CENTRAL.AGENT_TOOLS.SEND_CONVERSATION_REPORT
"""

import os
import logging
from datetime import datetime
from snowflake.snowpark import Session

logger = logging.getLogger(__name__)


def send_conversation_report(
    session: Session,
    recipient: str,
    question: str,
    response: str,
    report_title: str = None,
) -> str:
    """
    Send an SI conversation as an email with a PowerPoint report attachment.

    Creates a professional PowerPoint presentation from the conversation,
    uploads it to a Snowflake stage, generates a presigned URL, and sends
    an email with the download link.

    Args:
        session: Snowflake session (auto-provided by Snowpark)
        recipient: Email address to send report to
        question: The original question asked to the agent
        response: The agent's response (markdown supported)
        report_title: Optional custom title for the report

    Returns:
        Success/failure message
    """
    try:
        from presentation_tools.pptx_generator import create_conversation_pptx

        title = report_title or f"SI Report - {datetime.now().strftime('%Y-%m-%d')}"

        logger.info(f"Generating PowerPoint for: {question[:50]}...")
        pptx_path = create_conversation_pptx(
            question=question,
            response=response,
            title=title,
            subtitle=f"Generated {datetime.now().strftime('%B %d, %Y at %I:%M %p')}",
        )

        file_name = os.path.basename(pptx_path)
        stage_path = "@AGENT_TOOLS_CENTRAL.AGENT_TOOLS.REPORT_FILES_STAGE"

        logger.info(f"Uploading {file_name} to stage...")
        session.file.put(pptx_path, stage_path, auto_compress=False, overwrite=True)

        presigned_url = session.sql(
            f"""
            SELECT GET_PRESIGNED_URL(
                {stage_path},
                '{file_name}',
                86400
            ) AS url
        """
        ).collect()[0]["URL"]

        logger.info("Presigned URL generated, sending email...")

        email_body = _format_email_with_attachment(
            question=question,
            response=response,
            title=title,
            download_url=presigned_url,
            file_name=file_name,
        )

        subject = f"📊 {title}"

        session.call(
            "SYSTEM$SEND_EMAIL",
            "ai_email_int",
            recipient,
            subject,
            email_body,
            "text/html",
        )

        logger.info(f"Report sent successfully to {recipient}")

        return (
            f"✅ Conversation report sent to {recipient}!\n"
            f"📎 PowerPoint attachment available for 24 hours\n"
            f"📄 Report title: {title}"
        )

    except Exception as e:
        logger.error(f"Failed to send conversation report: {e}")
        return f"❌ Failed to send report: {str(e)}"


def _format_email_with_attachment(
    question: str, response: str, title: str, download_url: str, file_name: str
) -> str:
    """Format HTML email with download link styled as attachment."""

    import markdown

    try:
        md = markdown.Markdown(extensions=["tables", "fenced_code"])
        response_html = md.convert(response)
    except Exception:
        response_html = f"<pre>{response}</pre>"

    escaped_question = (
        question[:100].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    )
    if len(question) > 100:
        escaped_question += "..."

    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<style>
body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
    line-height: 1.6;
    color: #333333;
    max-width: 800px;
    margin: 0 auto;
    padding: 20px;
    background-color: #f9f9f9;
}}
.header {{
    background: linear-gradient(135deg, #1a73e8, #0d47a1);
    color: white;
    padding: 24px;
    border-radius: 12px 12px 0 0;
}}
.header h1 {{
    margin: 0;
    font-size: 20px;
    color: white;
    font-weight: 600;
}}
.header .meta {{
    opacity: 0.9;
    font-size: 12px;
    margin-top: 8px;
    color: rgba(255,255,255,0.9);
}}
.attachment-box {{
    background: #e8f0fe;
    border: 2px solid #1a73e8;
    border-radius: 8px;
    padding: 16px;
    margin: 16px 0;
    display: flex;
    align-items: center;
    gap: 12px;
}}
.attachment-icon {{
    font-size: 32px;
}}
.attachment-info {{
    flex: 1;
}}
.attachment-name {{
    font-weight: 600;
    color: #1a73e8;
    font-size: 14px;
}}
.attachment-desc {{
    font-size: 12px;
    color: #666;
    margin-top: 4px;
}}
.download-btn {{
    display: inline-block;
    background: #1a73e8;
    color: white !important;
    padding: 10px 20px;
    border-radius: 6px;
    text-decoration: none;
    font-weight: 600;
    font-size: 14px;
}}
.download-btn:hover {{
    background: #0d47a1;
}}
.content {{
    background: #ffffff;
    padding: 24px;
    border: 1px solid #e0e0e0;
    border-top: none;
    border-radius: 0 0 12px 12px;
}}
.question-box {{
    background: #f5f5f5;
    border-left: 4px solid #1a73e8;
    padding: 12px 16px;
    margin-bottom: 20px;
    font-style: italic;
}}
table {{
    border-collapse: collapse;
    width: 100%;
    margin: 16px 0;
    font-size: 14px;
}}
th {{
    background-color: #f5f5f5;
    padding: 12px;
    text-align: left;
    border-bottom: 2px solid #1a73e8;
    font-weight: 600;
}}
td {{
    padding: 12px;
    border-bottom: 1px solid #eeeeee;
}}
h2 {{
    color: #1a73e8;
    border-bottom: 2px solid #1a73e8;
    padding-bottom: 8px;
    margin-top: 24px;
}}
.footer {{
    text-align: center;
    padding: 16px;
    color: #666666;
    font-size: 12px;
}}
.expiry-note {{
    background: #fff3cd;
    border: 1px solid #ffc107;
    border-radius: 4px;
    padding: 8px 12px;
    font-size: 12px;
    color: #856404;
    margin-top: 8px;
}}
</style>
</head>
<body>

<div class="header">
    <h1>📊 {title}</h1>
    <div class="meta">Generated by Snowflake Intelligence | {datetime.now().strftime("%B %d, %Y at %I:%M %p")}</div>
</div>

<div class="content">

    <div class="attachment-box">
        <div class="attachment-icon">📎</div>
        <div class="attachment-info">
            <div class="attachment-name">{file_name}</div>
            <div class="attachment-desc">PowerPoint Report with Executive Summary, Data Tables & Charts</div>
        </div>
        <a href="{download_url}" class="download-btn">⬇️ Download</a>
    </div>
    <div class="expiry-note">⏰ This download link expires in 24 hours</div>

    <h2>Question Asked</h2>
    <div class="question-box">{escaped_question}</div>

    <h2>Response Summary</h2>
    {response_html}

</div>

<div class="footer">
    <strong>Snowflake Intelligence</strong><br/>
    <span style="font-size: 10px; color: #999999;">
        This report was automatically generated. The attached PowerPoint contains
        formatted slides with executive summary, data tables, and visualizations.
    </span>
</div>

</body>
</html>"""


def test_locally(
    question: str = "What were our top 5 products by revenue last quarter?",
    response: str = """## Revenue Analysis

Here are the top 5 products by revenue:

| Product | Revenue | Growth |
|---------|---------|--------|
| Product A | $1,250,000 | +15% |
| Product B | $980,000 | +8% |
| Product C | $750,000 | +22% |
| Product D | $620,000 | -3% |
| Product E | $510,000 | +12% |

### Key Insights
- Product A continues to lead with strong 15% growth
- Product C shows the highest growth rate at 22%
- Product D needs attention due to negative growth
""",
) -> str:
    """Local testing without Snowflake session."""
    from presentation_tools.pptx_generator import create_conversation_pptx

    print(f"[LOCAL TEST] Generating PowerPoint...")
    pptx_path = create_conversation_pptx(
        question=question, response=response, title="Test Report"
    )
    print(f"[LOCAL TEST] Created: {pptx_path}")

    print(f"[LOCAL TEST] Would send email to recipient with download link")
    return f"Local test: created {pptx_path}"
