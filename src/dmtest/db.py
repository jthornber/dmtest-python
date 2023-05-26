import sqlite3
import zlib
from typing import NamedTuple, Optional, List


class TestResult(NamedTuple):
    test_name: str
    pass_fail: str
    log: str
    dmesg: str
    result_set: str
    duration: float


class NoSuchResultSet(Exception):
    pass


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

        # Create the 'result_sets' table
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS result_sets (
            result_set_id INTEGER PRIMARY KEY,
            result_set TEXT UNIQUE
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
            dmesg BLOB,
            result_set_id INTEGER,
            duration REAL,
            FOREIGN KEY (result_set_id) REFERENCES result_sets (result_set_id)
            FOREIGN KEY (test_name_id) REFERENCES test_names (test_name_id),
            UNIQUE (test_name_id, result_set_id)
        )
        """
        )

        # Commit the changes
        self._conn.commit()

    def insert_result_set(self, result_set):
        cursor = self._conn.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO result_sets (result_set) VALUES (?)", (result_set,)
        )
        self._conn.commit()

    def get_result_set_id(self, result_set):
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT result_set_id FROM result_sets WHERE result_set = ?", (result_set,)
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

        self.insert_result_set(result.result_set)
        result_set_id = self.get_result_set_id(result.result_set)

        cursor = self._conn.cursor()
        if test_name_id and result_set_id:
            try:
                # We don't care if this fails
                cursor.execute(
                    "DELETE FROM test_results WHERE test_name_id = ? AND result_set_id = ?",
                    (test_name_id, result_set_id),
                )
            finally:
                pass

        compressed_log = zlib.compress(result.log.encode("utf-8"))
        compressed_dmesg = zlib.compress(result.dmesg.encode("utf-8"))
        cursor.execute(
            "INSERT INTO test_results (test_name_id, pass_fail, log, dmesg, result_set_id, duration) VALUES (?, ?, ?, ?, ?, ?)",
            (
                test_name_id,
                result.pass_fail,
                compressed_log,
                compressed_dmesg,
                result_set_id,
                result.duration,
            ),
        )
        self._conn.commit()

    def get_test_result(self, test_name: str, result_set: str) -> Optional[TestResult]:
        test_name_id = self.get_test_name_id(test_name)
        result_set_id = self.get_result_set_id(result_set)

        if test_name_id is None or result_set_id is None:
            return None

        cursor = self._conn.cursor()
        cursor.execute(
            """
            SELECT test_names.test_name, test_results.pass_fail, test_results.log, test_results.dmesg, result_sets.result_set, test_results.duration
            FROM test_results
            JOIN test_names ON test_results.test_name_id = test_names.test_name_id
            JOIN result_sets ON test_results.result_set_id = result_sets.result_set_id
            WHERE test_results.test_name_id = ? AND test_results.result_set_id = ?
        """,
            (test_name_id, result_set_id),
        )

        row = cursor.fetchone()

        if row is None:
            return None

        log = zlib.decompress(row[2]).decode("utf-8")
        dmesg = zlib.decompress(row[3]).decode("utf-8")
        test_result = TestResult(
            test_name=row[0],
            pass_fail=row[1],
            log=log,
            dmesg=dmesg,
            result_set=row[4],
            duration=row[5],
        )

        return test_result

    def get_result_sets(self) -> List[str]:
        cursor = self._conn.cursor()
        cursor.execute("SELECT result_set FROM result_sets")
        rows = cursor.fetchall()
        return [row[0] for row in rows]

    # Removes a result_set and all test results associated with it.
    def delete_result_set(self, result_set):
        cursor = self._conn.cursor()
        result_set_id = self.get_result_set_id(result_set)

        if result_set_id is None:
            raise NoSuchResultSet("Result set '{result_set}' not found")

        # Remove test results associated with the result_set_id
        cursor.execute(
            "DELETE FROM test_results WHERE result_set_id = ?", (result_set_id,)
        )
        self._conn.commit()

        # Remove the result set
        cursor.execute(
            "DELETE FROM result_sets WHERE result_set_id = ?", (result_set_id,)
        )
        self._conn.commit()
