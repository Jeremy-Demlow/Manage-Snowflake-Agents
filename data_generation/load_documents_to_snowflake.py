"""
Load documents to Snowflake and create Cortex Search service.
"""

import json
from snowflake_connection import SnowflakeConnection

def main():
    print("🔗 Connecting to Snowflake...")
    conn = SnowflakeConnection.from_snow_cli("snowflake_agents")

    # Create table
    print("📋 Creating documents table...")
    conn.sql("""
        CREATE TABLE IF NOT EXISTS SKI_RESORT_DB.DOCS.RESORT_DOCUMENTS (
            DOC_ID VARCHAR(50) PRIMARY KEY,
            DOC_TYPE VARCHAR(50),
            TITLE VARCHAR(500),
            CONTENT TEXT,
            SOURCE_FILE VARCHAR(200),
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
    """).collect()

    # Clear existing docs
    conn.sql("TRUNCATE TABLE SKI_RESORT_DB.DOCS.RESORT_DOCUMENTS").collect()

    # Load documents from JSON
    print("📄 Loading documents...")
    with open('documents.json') as f:
        documents = json.load(f)

    for doc in documents:
        # Escape single quotes
        content = doc['content'].replace("'", "''")
        title = doc['title'].replace("'", "''")

        sql = f"""
            INSERT INTO SKI_RESORT_DB.DOCS.RESORT_DOCUMENTS
            (DOC_ID, DOC_TYPE, TITLE, CONTENT, SOURCE_FILE)
            VALUES ('{doc['doc_id']}', '{doc['doc_type']}', '{title}', '{content}', '{doc['source_file']}')
        """
        conn.sql(sql).collect()
        print(f"   ✅ {doc['doc_id']}: {doc['title']}")

    # Verify count
    result = conn.sql("SELECT COUNT(*) as cnt FROM SKI_RESORT_DB.DOCS.RESORT_DOCUMENTS").to_pandas()
    print(f"\n📊 Total documents loaded: {result['CNT'].iloc[0]}")

    # Create Cortex Search Service
    print("\n🔍 Creating Cortex Search Service...")
    try:
        conn.sql("""
            CREATE OR REPLACE CORTEX SEARCH SERVICE SKI_RESORT_DB.DOCS.RESORT_DOCS_SEARCH
              ON CONTENT
              ATTRIBUTES DOC_TYPE, TITLE
              WAREHOUSE = COMPUTE_WH
              TARGET_LAG = '1 hour'
              AS (
                SELECT DOC_ID, DOC_TYPE, TITLE, CONTENT, SOURCE_FILE
                FROM SKI_RESORT_DB.DOCS.RESORT_DOCUMENTS
              )
        """).collect()
        print("   ✅ Cortex Search Service created!")
    except Exception as e:
        print(f"   ⚠️ Search service creation: {e}")

    print("\n✅ Done!")
    conn.close()


if __name__ == '__main__':
    main()
