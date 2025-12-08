"""
Email Formatter for Scheduled Alerts

Converts agent responses to beautifully formatted HTML emails.
Supports markdown conversion with inline CSS for email client compatibility.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import markdown


@dataclass
class EmailConfig:
    """Configuration for email formatting."""

    brand_name: str = "Ski Resort Analytics"
    brand_tagline: str = "Powered by Snowflake Cortex"
    primary_color: str = "#1a73e8"
    secondary_color: str = "#0d47a1"
    emoji_prefix: str = "🎿"


class EmailFormatter:
    """
    Formats agent responses as beautiful HTML emails.

    Uses inline CSS for maximum email client compatibility.
    """

    def __init__(self, config: Optional[EmailConfig] = None):
        self.config = config or EmailConfig()
        self._md = markdown.Markdown(
            extensions=["tables", "fenced_code", "nl2br", "sane_lists", "smarty"]
        )

    def _convert_markdown(self, text: str) -> str:
        """Convert markdown to HTML."""
        self._md.reset()
        return self._md.convert(text)

    def _get_base_styles(self) -> str:
        """Base CSS styles (inlined in HTML)."""
        return f"""
            body {{
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
                margin: 0;
                padding: 0;
                background-color: #f6f9fc;
                color: #1f2937;
                line-height: 1.6;
            }}
            .container {{
                max-width: 700px;
                margin: 0 auto;
                background: #ffffff;
                border-radius: 12px;
                overflow: hidden;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.05);
            }}
            .header {{
                background: linear-gradient(135deg, {self.config.primary_color}, {self.config.secondary_color});
                color: #ffffff;
                padding: 32px 40px;
            }}
            .header h1 {{
                font-size: 22px;
                font-weight: 600;
                margin: 0 0 12px 0;
                color: #ffffff;
            }}
            .header .meta {{
                font-size: 13px;
                opacity: 0.9;
            }}
            .content {{
                padding: 32px 40px;
                font-size: 15px;
            }}
            h2 {{
                color: {self.config.primary_color};
                font-size: 18px;
                font-weight: 600;
                margin: 28px 0 14px 0;
                padding-bottom: 8px;
                border-bottom: 2px solid #e5e7eb;
            }}
            h3 {{
                color: #374151;
                font-size: 16px;
                font-weight: 600;
                margin: 20px 0 10px 0;
            }}
            p {{
                margin: 14px 0;
            }}
            ul, ol {{
                margin: 16px 0;
                padding-left: 24px;
            }}
            li {{
                margin: 8px 0;
            }}
            strong {{
                color: {self.config.primary_color};
                font-weight: 600;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin: 20px 0;
                font-size: 14px;
            }}
            th {{
                background: {self.config.primary_color};
                color: #ffffff;
                padding: 12px 16px;
                text-align: left;
                font-weight: 600;
            }}
            td {{
                padding: 12px 16px;
                border: 1px solid #e5e7eb;
            }}
            tr:nth-child(even) {{
                background: #f9fafb;
            }}
            code {{
                background: #f3f4f6;
                padding: 2px 6px;
                border-radius: 4px;
                font-family: 'Monaco', 'Menlo', monospace;
                font-size: 13px;
            }}
            pre {{
                background: #1f2937;
                color: #e5e7eb;
                padding: 16px;
                border-radius: 8px;
                overflow-x: auto;
            }}
            pre code {{
                background: transparent;
                padding: 0;
                color: inherit;
            }}
            blockquote {{
                border-left: 4px solid {self.config.primary_color};
                margin: 16px 0;
                padding: 12px 20px;
                background: #f9fafb;
                color: #4b5563;
            }}
            .footer {{
                background: #f8f9fa;
                padding: 24px 40px;
                font-size: 13px;
                color: #6b7280;
                border-top: 1px solid #e5e7eb;
            }}
            .footer strong {{
                color: #374151;
            }}
            .manage-link {{
                font-size: 12px;
                color: #9ca3af;
                margin-top: 8px;
            }}
        """

    def format(
        self, question: str, response: str, timestamp: Optional[datetime] = None
    ) -> str:
        """
        Format agent response as HTML email.

        Args:
            question: The original question asked
            response: The agent's markdown response
            timestamp: Optional timestamp (defaults to now)

        Returns:
            Complete HTML email string with inline styles
        """
        if timestamp is None:
            timestamp = datetime.now()

        timestamp_str = timestamp.strftime("%B %d, %Y at %I:%M %p")

        # Convert response markdown to HTML
        response_html = self._convert_markdown(response)

        # Truncate question for title
        title = question[:80] + "..." if len(question) > 80 else question

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{self.config.emoji_prefix} {title}</title>
    <style>
        {self._get_base_styles()}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>{self.config.emoji_prefix} {title}</h1>
            <div class="meta">Generated by Snowflake Intelligence | {timestamp_str}</div>
        </div>
        <div class="content">
            {response_html}
        </div>
        <div class="footer">
            <strong>{self.config.brand_name}</strong> | {self.config.brand_tagline}
            <div class="manage-link">
                To manage alerts: "What alerts do I have?" or "Unsubscribe from [topic]"
            </div>
        </div>
    </div>
</body>
</html>"""

    def format_with_inline_css(
        self, question: str, response: str, timestamp: Optional[datetime] = None
    ) -> str:
        """
        Format with fully inlined CSS for maximum email client compatibility.

        Uses premailer to inline all CSS if available, otherwise returns
        the standard format.
        """
        html = self.format(question, response, timestamp)

        try:
            import premailer

            return premailer.transform(html)
        except ImportError:
            # premailer not installed, return as-is
            return html

    def format_error(
        self, question: str, error: str, timestamp: Optional[datetime] = None
    ) -> str:
        """Format an error message as HTML email."""
        if timestamp is None:
            timestamp = datetime.now()

        timestamp_str = timestamp.strftime("%B %d, %Y at %I:%M %p")

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Alert Processing Error</title>
</head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; padding: 0; background: #f6f9fc;">
    <div style="max-width: 600px; margin: 0 auto; background: #ffffff; border-radius: 12px; overflow: hidden;">
        <div style="background: #ef4444; color: #ffffff; padding: 32px 40px;">
            <h1 style="font-size: 20px; margin: 0;">⚠️ Alert Processing Error</h1>
            <div style="font-size: 13px; opacity: 0.9; margin-top: 8px;">{timestamp_str}</div>
        </div>
        <div style="padding: 32px 40px;">
            <p style="margin: 0 0 16px 0;"><strong>Question:</strong></p>
            <p style="margin: 0 0 24px 0; color: #6b7280;">{question}</p>
            <p style="margin: 0 0 16px 0;"><strong>Error:</strong></p>
            <pre style="background: #fef2f2; border: 1px solid #fecaca; padding: 16px; border-radius: 8px; color: #b91c1c; overflow-x: auto;">{error}</pre>
        </div>
        <div style="background: #f8f9fa; padding: 24px 40px; font-size: 13px; color: #6b7280;">
            Please contact support if this error persists.
        </div>
    </div>
</body>
</html>"""
