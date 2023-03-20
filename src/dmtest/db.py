import sqlite3
import zlib
from typing import NamedTuple, Optional


class TestResult(NamedTuple):
    test_name: str
    pass_fail: str  # FIXME: change to bool
    log: str
    kernel_version: str
    duration: float


class TestResults:
    def __init__(self, path):
        # Connect to the SQLite database (create the file if it doesn't exist)
        self._conn = sqlite3.connect(path)
        self._create_tables()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._conn.close()

    def _create_tables(self):
        cursor = self._conn.cursor()

        # Create the 'kernel_versions' table
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS kernel_versions (
            version_id INTEGER PRIMARY KEY,
            version TEXT UNIQUE
        )
        """
        )

        # Create the 'test_names' table
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS test_names (
            test_name_id INTEGER PRIMARY KEY,
            test_name TEXT UNIQUE
        )
        """
        )

        # Create the 'test_results' table
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS test_results (
            test_id INTEGER PRIMARY KEY,
            test_name_id INTEGER,
            pass_fail TEXT,
            log BLOB,
            version_id INTEGER,
            duration REAL,
            FOREIGN KEY (version_id) REFERENCES kernel_versions (version_id)
            FOREIGN KEY (test_name_id) REFERENCES test_names (test_name_id),
            UNIQUE (test_name_id, version_id)
        )
        """
        )

        # Commit the changes
        self._conn.commit()

    # Function to insert a software version
    def insert_kernel_version(self, version):
        cursor = self._conn.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO kernel_versions (version) VALUES (?)", (version,)
        )
        self._conn.commit()

    # Function to get the version_id for a given software version
    def get_kernel_version_id(self, version):
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT version_id FROM kernel_versions WHERE version = ?", (version,)
        )
        row = cursor.fetchone()

        if row is None:
            return None

        return row[0]

    # Function to insert a test name
    def insert_test_name(self, test_name):
        cursor = self._conn.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO test_names (test_name) VALUES (?)", (test_name,)
        )
        self._conn.commit()

    # Function to get the test_name_id for a given test name
    def get_test_name_id(self, test_name):
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT test_name_id FROM test_names WHERE test_name = ?", (test_name,)
        )
        row = cursor.fetchone()

        if row is None:
            return None

        return row[0]

    # Function to insert a test result
    def insert_test_result(self, result):
        self.insert_test_name(result.test_name)
        test_name_id = self.get_test_name_id(result.test_name)

        self.insert_kernel_version(result.kernel_version)
        version_id = self.get_kernel_version_id(result.kernel_version)

        cursor = self._conn.cursor()
        if test_name_id and version_id:
            try:
                # We don't care if this fails
                cursor.execute(
                    "DELETE FROM test_results WHERE test_name_id = ? AND version_id = ?",
                    (test_name_id, version_id),
                )
            finally:
                pass

        compressed_log = zlib.compress(result.log.encode("utf-8"))
        cursor.execute(
            "INSERT INTO test_results (test_name_id, pass_fail, log, version_id, duration) VALUES (?, ?, ?, ?, ?)",
            (
                test_name_id,
                result.pass_fail,
                compressed_log,
                version_id,
                result.duration,
            ),
        )
        self._conn.commit()

    def get_test_result(self, test_name: str, version: str) -> Optional[TestResult]:
        test_name_id = self.get_test_name_id(test_name)
        version_id = self.get_kernel_version_id(version)

        if test_name_id is None or version_id is None:
            return None

        cursor = self._conn.cursor()
        cursor.execute(
            """
            SELECT test_names.test_name, test_results.pass_fail, test_results.log, kernel_versions.version, test_results.duration
            FROM test_results
            JOIN test_names ON test_results.test_name_id = test_names.test_name_id
            JOIN kernel_versions ON test_results.version_id = kernel_versions.version_id
            WHERE test_results.test_name_id = ? AND test_results.version_id = ?
        """,
            (test_name_id, version_id),
        )

        row = cursor.fetchone()

        if row is None:
            return None

        log = zlib.decompress(row[2]).decode("utf-8")
        test_result = TestResult(
            test_name=row[0],
            pass_fail=row[1],
            log=log,
            kernel_version=row[3],
            duration=row[4],
        )

        return test_result
