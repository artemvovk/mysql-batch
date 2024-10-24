#!/usr/bin/env python3

# Author: Gabriel Bordeaux (gabfl)
# Github: https://github.com/gabfl/mysql-batch
# Compatible with python 2.7 & 3

import sys
import time
import pymysql.cursors
import pymysql.constants.CLIENT
import argparse
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

def update_batch(ids, table, set_, sleep=0, primary_key='id'):
    """
        Update a batch of rows
    """

    global confirmed_write

    # Leave if ids is empty
    if not ids or len(ids) == 0:
        return None

    # Prepare update
    print('* Updating %i rows...' % len(ids))
    sql = "UPDATE " + table + " SET " + set_ + \
        " WHERE {0} IN (".format(primary_key) + \
        ', '.join([str(x) for x in ids]) + ")"
    print("   query: " + sql)

    if confirmed_write or query_yes_no("* Start updating?"):
        # Switch confirmed_write skip the question for the next update
        confirmed_write = True

        # Execute query
        run_query(sql, sleep)
    else:  # answered "no"
        print("Error: Update declined.")
        sys.exit()

    return True


def delete_batch(ids, table, sleep=0, primary_key='id'):
    """
        Delete a batch of rows
    """

    global confirmed_write

    # Leave if ids is empty
    if not ids or len(ids) == 0:
        return None

    # Prepare delete
    print('* Deleting %i rows...' % len(ids))
    sql = "DELETE FROM " + table + \
        " WHERE {0} IN (".format(primary_key) + \
        ', '.join([str(x) for x in ids]) + ")"
    print("   query: " + sql)

    if confirmed_write or query_yes_no("* Start deleting?"):
        # Switch confirmed_write skip the question for the next delete
        confirmed_write = True

        # Execute query
        run_query(sql, sleep)
    else:  # answered "no"
        print("Error: Delete declined.")
        sys.exit()

    return True


def run_query(sql, sleep=0):
    """Execute a write query"""

    # Execute query
    with connection.cursor() as cursorUpd:
        cursorUpd.execute(sql)
        connection.commit()

    # Optional Sleep
    if sleep > 0:
        time.sleep(sleep)

    return True


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
                               client_flag=pymysql.constants.CLIENT.MULTI_STATEMENTS,
                               cursorclass=pymysql.cursors.DictCursor)
    except Exception:
        raise RuntimeError('Error: MySQL connection failed.')



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

    Args:
        host: Database host
        user: Database user
        port: Database port
        password: Database password
        database: Database name
        action: Either 'update' or 'delete'
        table: Table to modify
        where: WHERE clause for selecting records
        set_: SET clause for updates
        no_confirm: Skip confirmation prompt
        primary_key: Primary key column name
        read_batch_size: Number of records to read at once
        write_batch_size: Number of records to write in each batch
        sleep: Sleep time between batches
        max_workers: Maximum number of parallel workers
    """
    global confirmed_write, connection

    if action == 'update' and set_ is None:
        raise RuntimeError('Error: argument -s/--set is required for updates.')

    connection = connect(host, user, port, password, database)
    confirmed_write = no_confirm

    try:
        with connection.cursor() as cursor:
            # Create temporary table
            create_temp_table(cursor, primary_key)

            # Populate temporary table with all matching primary keys
            populate_temp_table(cursor, table, where, primary_key, read_batch_size)

            # Get total count of records to process
            cursor.execute("SELECT COUNT(*) as count FROM temp_batch_keys")
            total_count = cursor.fetchone()['count']

            if total_count == 0:
                print("* No rows to modify!")
                return True

            print(f"* Found {total_count} rows to process")

            # Process in parallel batches
            process_parallel_batches(
                cursor,
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
        connection.close()

    return True

def create_temp_table(cursor, primary_key: str):
    """Create temporary table for storing primary keys"""
    print("* Creating temporary table...")
    cursor.execute(f"""
        CREATE TEMPORARY TABLE temp_batch_keys (
            {primary_key} bigint PRIMARY KEY,
            processed boolean DEFAULT false
        )
    """)

def populate_temp_table(cursor, table: str, where: str, primary_key: str, batch_size: int):
    """Populate temporary table with all matching primary keys"""
    print("* Populating temporary table with primary keys...")
    cursor.execute(f"""
        INSERT INTO temp_batch_keys ({primary_key})
        SELECT {primary_key} FROM {table}
        WHERE {where}
    """)

def get_next_batch(cursor, primary_key: str, batch_size: int) -> List[int]:
    """Get next batch of unprocessed IDs"""
    cursor.execute(f"""
        UPDATE temp_batch_keys
        SET processed = true
        WHERE {primary_key} IN (
            SELECT {primary_key}
            FROM temp_batch_keys
            WHERE processed = false
            LIMIT {batch_size}
        )
        RETURNING {primary_key}
    """)
    return [row[primary_key] for row in cursor.fetchall()]

def process_parallel_batches(
    cursor,
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
            # Get multiple batches for parallel processing
            batches = []
            for _ in range(max_workers):
                batch = get_next_batch(cursor, primary_key, batch_size)
                if not batch:
                    break
                batches.append(batch)

            if not batches:
                break

            # Submit batch jobs
            futures = []
            for batch in batches:
                if action == 'delete':
                    future = executor.submit(delete_batch, batch, table, sleep, primary_key)
                else:
                    future = executor.submit(update_batch, batch, table, set_, sleep, primary_key)
                futures.append(future)

            # Wait for all batches to complete
            for future in futures:
                future.result()

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
