"""
Snowpark project setup for Agent Tools
"""

from setuptools import setup, find_packages

setup(
    name="agent-tools",
    version="1.0.0",
    description="Clean agent tools for Snowflake Intelligence",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.8",
    install_requires=[
        "snowflake-snowpark-python",
        "snowflake-cortex",
        "requests>=2.31.0",
        "beautifulsoup4>=4.12.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-mock>=3.11.1",
        ]
    },
)
