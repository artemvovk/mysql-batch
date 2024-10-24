#!/usr/bin/env python3

# Author: Gabriel Bordeaux (gabfl)
# Github: https://github.com/gabfl/mysql-batch
# Compatible with python 2.7 & 3

import sys
import threading
import time
import pymysql.cursors
import pymysql.constants.CLIENT
import argparse
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Dict
import threading
from threading import Lock

class DatabaseConnectionManager:
    def __init__(self, host: str, user: str, port: int, password: str, database: str):
        self.host = host
        self.user = user
        self.port = port
        self.password = password
        self.database = database
        self.connections: Dict[int, Any] = {}
        self.lock = Lock()

    def get_connection(self, thread_id: int):
        with self.lock:
            if thread_id not in self.connections:
                self.connections[thread_id] = connect(
                    self.host,
                    self.user,
                    self.port,
                    self.password,
                    self.database
                )
            return self.connections[thread_id]

    def close_all(self):
        with self.lock:
            for conn in self.connections.values():
                try:
                    conn.close()
                except:
                    pass
            self.connections.clear()

def execute(
    host: str,
    user: str,
    port: int,
    password: str,
    database: str,
    action: str,
    table: str,
    where: str,
    set_: Optional[str] = None,
    no_confirm: bool = False,
    primary_key: str = 'id',
    read_batch_size: int = 10000,
    write_batch_size: int = 50,
    sleep: float = 0,
    max_workers: int = 4
):
    """
    Execute batch update or delete operations in parallel using a temporary table
    """
    if action == 'update' and set_ is None:
        raise RuntimeError('Error: argument -s/--set is required for updates.')

    # Create connection manager
    conn_manager = DatabaseConnectionManager(host, user, port, password, database)

    try:
        # Use main thread connection for setup
        main_conn = conn_manager.get_connection(0)
        with main_conn.cursor() as cursor:
            # Create temporary table
            create_temp_table(cursor, primary_key)

            # Populate temporary table with all matching primary keys
            populate_temp_table(cursor, table, where, primary_key, read_batch_size, main_conn)

            # Get total count of records to process
            cursor.execute("SELECT COUNT(*) FROM temp_batch_keys")
            total_count = cursor.fetchone()[0]

            if total_count == 0:
                print("* No rows to modify!")
                return True

            print(f"* Found {total_count} rows to process")

            # Process in parallel batches
            process_parallel_batches(
                conn_manager,
                action,
                table,
                set_,
                primary_key,
                write_batch_size,
                sleep,
                max_workers
            )

    except SystemExit:
        print("* Program exited")
    finally:
        # Drop temporary table using main connection
        main_conn = conn_manager.get_connection(0)
        with main_conn.cursor() as cursor:
            cursor.execute("DROP TEMPORARY TABLE IF EXISTS temp_batch_keys")

        # Close all connections
        conn_manager.close_all()

    return True

def create_temp_table(cursor, primary_key: str):
    """Create temporary table for storing primary keys"""
    print("* Creating temporary table...")
    cursor.execute(f"""
        CREATE TEMPORARY TABLE temp_batch_keys (
            {primary_key} bigint PRIMARY KEY,
            processed boolean DEFAULT false,
            worker_id int DEFAULT NULL,
            INDEX (processed, worker_id)
        )
    """)

def populate_temp_table(cursor, table: str, where: str, primary_key: str, batch_size: int, connection):
    """Populate temporary table with all matching primary keys using LIMIT-based pagination"""
    print("* Populating temporary table with primary keys...")

    # Insert a batch of records using LIMIT/OFFSET
    cursor.execute(f"""
        INSERT INTO temp_batch_keys ({primary_key})
        SELECT {primary_key}
        FROM {table}
        WHERE {where}
        LIMIT 1000000
    """)

    rows_inserted = cursor.rowcount

    total_inserted = rows_inserted
    print(f"* Inserted batch: {rows_inserted} rows (Total: {total_inserted})")

    connection.commit()
    offset += batch_size

def get_next_batch(connection, primary_key: str, batch_size: int, worker_id: int) -> List[int]:
    """Get next batch of unprocessed IDs using MySQL-compatible approach"""
    with connection.cursor() as cursor:
        cursor.execute("START TRANSACTION")

        try:
            cursor.execute(f"""
                SELECT {primary_key}
                FROM temp_batch_keys
                WHERE processed = false AND worker_id IS NULL
                LIMIT {batch_size}
            """)
            rows = cursor.fetchall()

            if not rows:
                cursor.execute("COMMIT")
                return []

            ids = [row[0] for row in rows]

            id_list = ','.join(map(str, ids))
            cursor.execute(f"""
                UPDATE temp_batch_keys
                SET processed = true, worker_id = {worker_id}
                WHERE {primary_key} IN ({id_list})
            """)

            cursor.execute("COMMIT")
            return ids

        except Exception as e:
            cursor.execute("ROLLBACK")
            print(f"Error in get_next_batch: {e}")
            return []

def process_parallel_batches(
    conn_manager: DatabaseConnectionManager,
    action: str,
    table: str,
    set_: Optional[str],
    primary_key: str,
    batch_size: int,
    sleep: float,
    max_workers: int
):
    """Process batches in parallel using ThreadPoolExecutor"""
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while True:
            # Get connection for the main thread
            main_conn = conn_manager.get_connection(0)

            batches = []
            for worker_id in range(max_workers):
                batch = get_next_batch(main_conn, primary_key, batch_size, worker_id)
                if not batch:
                    break
                batches.append((batch, worker_id))

            if not batches:
                break

            futures = []
            for batch, worker_id in batches:
                if action == 'delete':
                    future = executor.submit(
                        delete_batch,
                        conn_manager,
                        batch,
                        table,
                        sleep,
                        primary_key,
                        worker_id
                    )
                else:
                    future = executor.submit(
                        update_batch,
                        conn_manager,
                        batch,
                        table,
                        set_,
                        sleep,
                        primary_key,
                        worker_id
                    )
                futures.append(future)

            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    print(f"Error processing batch: {e}")

def delete_batch(
    conn_manager: DatabaseConnectionManager,
    ids: List[int],
    table: str,
    sleep: float,
    primary_key: str,
    worker_id: int
):
    """Execute delete batch"""
    # Get connection specific to this worker
    connection = conn_manager.get_connection(worker_id)

    print(f"Worker {worker_id}: Deleting batch of {len(ids)} records")

    with connection.cursor() as cursor:
        sql = f"DELETE FROM {table} WHERE {primary_key} IN ({','.join(map(str, ids))})"
        cursor.execute(sql)
        connection.commit()

        if sleep > 0:
            time.sleep(sleep)

def update_batch(
    conn_manager: DatabaseConnectionManager,
    ids: List[int],
    table: str,
    set_: str,
    sleep: float,
    primary_key: str,
    worker_id: int
):
    """Execute update batch"""
    # Get connection specific to this worker
    connection = conn_manager.get_connection(worker_id)

    print(f"Worker {worker_id}: Updating batch of {len(ids)} records")

    with connection.cursor() as cursor:
        sql = f"UPDATE {table} SET {set_} WHERE {primary_key} IN ({','.join(map(str, ids))})"
        cursor.execute(sql)
        connection.commit()

        if sleep > 0:
            time.sleep(sleep)

def get_input():
    """
        Get user input
    """

    # Get user choice with python 2.7 retro-compatibility
    if sys.version_info >= (3, 0):
        # Python 3
        # print ("python >= 3");
        return input().lower()
    else:
        # Python 2.7 retro-compatibility
        # print ("python 2.7");
        return raw_input().lower()


def query_yes_no(question, default="yes"):
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".

    (thanks https://code.activestate.com/recipes/577058/)
    """

    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}

    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = get_input()

        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")


def connect(host, user, port, password, database):
    """
        Connect to a MySQL database
    """

    # Connect to the database
    try:
        return pymysql.connect(host=host,
                               user=user,
                               port=port,
                               password=password,
                               db=database,
                               charset='utf8mb4',
                               client_flag=pymysql.constants.CLIENT.MULTI_STATEMENTS)
    except Exception:
        raise RuntimeError('Error: MySQL connection failed.')

def main():
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-H", "--host", default="127.0.0.1",
                        help="MySQL server host")
    parser.add_argument("-P", "--port", type=int, default=3306,
                        help="MySQL server port")
    parser.add_argument("-U", "--user", required=True,
                        help="MySQL user")
    parser.add_argument("-p", "--password", default='',
                        help="MySQL password")
    parser.add_argument("-d", "--database", required=True,
                        help="MySQL database name")
    parser.add_argument("-t", "--table", required=True,
                        help="MySQL table")
    parser.add_argument("-id", "--primary_key", default='id',
                        help="Name of the primary key column")
    parser.add_argument("-w", "--where", required=True,
                        help="Select WHERE clause")
    parser.add_argument("-s", "--set",
                        help="Update SET clause")
    parser.add_argument("-rbz", "--read_batch_size", type=int, default=10000,
                        help="Select batch size")
    parser.add_argument("-wbz", "--write_batch_size", type=int, default=50,
                        help="Update/delete batch size")
    parser.add_argument("-S", "--sleep", type=float, default=0.00,
                        help="Sleep after each batch")
    parser.add_argument("-a", "--action", default='update', choices=['update', 'delete'],
                        help="Action ('update' or 'delete')")
    parser.add_argument("-n", "--no_confirm", action='store_true',
                        help="Don't ask for confirmation before to run the write queries")
    args = parser.parse_args()

    execute(host=args.host,
            user=args.user,
            port=args.port,
            password=args.password,
            database=args.database,
            action=args.action,
            table=args.table,
            where=args.where,
            set_=args.set,
            no_confirm=args.no_confirm,
            primary_key=args.primary_key,
            read_batch_size=args.read_batch_size,
            write_batch_size=args.write_batch_size,
            sleep=args.sleep
            )


if __name__ == '__main__':
    main()
