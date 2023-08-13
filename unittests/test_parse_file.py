import os
import subprocess
import sqlite3
import hashlib
import pytest


@pytest.fixture
def setup_json_db():
    # Setup: Delete the test database if it exists to start fresh
    dbpath = "unittest_json.db"
    if os.path.exists(dbpath):
        os.remove(dbpath)
    yield  # This is where the test function executes
    # Teardown: Remove the database after tests (if necessary)
    for suffix in ["", "-shm", "-wal"]:
        filepath = f"{dbpath}{suffix}"
        if os.path.exists(filepath):
            os.remove(filepath)


@pytest.fixture
def setup_text_db():
    # Setup: Delete the test database if it exists to start fresh
    dbpath = "unittest_text.db"
    if os.path.exists(dbpath):
        os.remove(dbpath)
    yield  # This is where the test function executes
    # Teardown: Remove the database after tests (if necessary)
    for suffix in ["", "-shm", "-wal"]:
        filepath = f"{dbpath}{suffix}"
        if os.path.exists(filepath):
            os.remove(filepath)


def execute_sql(dbname, query):
    rows = []
    with sqlite3.connect(dbname) as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            rows.append(row)
    return rows


def compute_db_hash(db_path: str) -> str:
    """Compute a hash of the database contents."""
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
        tables = cursor.fetchall()
        hash_obj = hashlib.sha256()
        # Open the file to write the contents
        for table in tables:
            query = f"SELECT * FROM {table[0]}"
            if table[0] == "message_links":
                query = f"{query} order by message_id, relation_id, message_type, relation_type desc"
            cursor.execute(query)
            for row in cursor.fetchall():
                hash_obj.update(str(row).encode('utf-8'))

        return hash_obj.hexdigest()


def test_parser_text_output(setup_text_db):
    # Step 2: Execute the parser command
    cmd = ["nanologp", "--file", "unittests/data/text.log",
           "--format", "text", "--db", "unittest_text.db"]
    # 'check=True' will raise an error if the command fails
    subprocess.run(cmd, check=True)

    # Step 3: Compute hash of database content
    hash_result = compute_db_hash("unittest_text.db")

    # Step 4: Compare the hash
    expected_hash = "8ebf16de4ddba266cde3e889094a91f4b78995a44ce618614bc4821bd3d897a1"
    assert hash_result == expected_hash, f"Expected {expected_hash}, but got {hash_result}"


def test_parser_json_output(setup_json_db):
    # Step 2: Execute the parser command
    cmd = ["nanologp", "--file", "unittests/data/json.log",
           "--format", "json", "--db", "unittest_json.db"]
    # 'check=True' will raise an error if the command fails
    subprocess.run(cmd, check=True)

    # Step 3: Compute hash of database content
    hash_result = compute_db_hash("unittest_json.db")

    # Step 4: Compare the hash
    expected_hash = "8975958213c406365efef439a2c0e03f3896df8d8901f4122c96a5c29833a4a6"
    assert hash_result == expected_hash, f"Expected {expected_hash}, but got {hash_result}"


def test_parser_json_output_with_node(setup_json_db):
    # Step 2: Execute the parser command
    dbname = "unittest_json.db"
    cmd = ["nanologp", "--file", "unittests/data/json.log",
           "--format", "json", "--db", dbname, "--node", "pr1"]
    # 'check=True' will raise an error if the command fails
    subprocess.run(cmd, check=True)
    rows = execute_sql(
        dbname, "SELECT * from unknownmessage limit 1")

    result = rows[0][2]
    # Step 4: Compare the hash
    expected_result = "pr1"
    assert result == expected_result, f"Expected {expected_result}, but got {result}"
