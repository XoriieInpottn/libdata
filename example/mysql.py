#!/usr/bin/env python3

__author__ = "xi"

from argparse import ArgumentParser
from datetime import datetime

from libdata.common import DocReader
from libdata.mysql import LazyMySQLClient
from libdata.url import URL


def main():
    parser = ArgumentParser()
    parser.add_argument("--url", required=True)
    args = parser.parse_args()

    # You can set the connection pool size.
    # The default size is 16.
    LazyMySQLClient.DEFAULT_CONN_POOL.max_size = 20

    url = args.url
    print(url)

    ################################################################################
    # Create Table
    # You can execute an SQL by the client.
    ################################################################################
    sql = """
    CREATE TABLE IF NOT EXISTS my_test (
        id INT NOT NULL AUTO_INCREMENT, 
        name VARCHAR(256), 
        age INT, 
        create_time DATETIME, 
        PRIMARY KEY (id)
    );
    """
    with LazyMySQLClient.from_url(url) as client:
        cur = client.execute(sql)
        if cur.close():
            print("Table created.")
        else:
            print("Failed to create table.")
        print()

    ################################################################################
    # Insert Data
    # Although you can write your own SQL to insert data, there is a more convenient
    # way to `insert`, `find`, `update` and `delete` samples.
    ################################################################################
    with LazyMySQLClient.from_url(url) as client:
        now = datetime.now()
        docs = [
            {"name": "LiLei", "age": 16, "create_time": now},
            {"name": "HanMeimei", "age": 14, "create_time": now},
            {"name": "LinTao", "age": 20, "create_time": now},
        ]
        for doc in docs:
            client.insert(doc, table="my_test")
        print("Data inserted.")
        print("Current table data:")
        for doc in client.find(table="my_test"):
            print(doc)
        print()

    ################################################################################
    # Read all samples via DocReader
    ################################################################################
    print("Read all samples via DocReader")
    url = URL.ensure_url(url)
    reader_url = url.model_copy()
    reader_url.path = reader_url.path.rstrip() + "/my_test"
    print(f"Table URL: {reader_url}")
    reader = DocReader.from_url(reader_url)
    for doc in reader:
        print(doc)
    print()

    ################################################################################
    # Update Data
    ################################################################################
    with LazyMySQLClient.from_url(url) as client:
        if client.update(set="age = 17", where="name = \"HanMeimei\"", table="my_test"):
            print("Data updated.")
        else:
            print("Failed to update.")
        print("Current table data:")
        for doc in client.find(table="my_test"):
            print(doc)
        print()

    ################################################################################
    # Delete Data
    ################################################################################
    with LazyMySQLClient.from_url(url) as client:
        if client.delete(where="age > 18", table="my_test"):
            print("Data deleted.")
        else:
            print("Failed to delete.")
        print("Current table data:")
        for doc in client.find(table="my_test"):
            print(doc)
        print()

    ################################################################################
    # Transaction
    # Add parameter "autocommit=false" to url means you must commit the transaction
    # manually.
    ################################################################################
    print("Utilize a transaction")
    url = URL.ensure_url(url)
    trans_url = url.model_copy()
    if not trans_url.parameters:
        trans_url.parameters = {}
    trans_url.parameters["autocommit"] = "false"
    print(trans_url)
    trans_client = LazyMySQLClient.from_url(trans_url)
    trans_client.start_transaction()
    with trans_client.cursor(dictionary=True, buffered=True) as cur, trans_client:
        cur.execute("SELECT * FROM my_test WHERE name = \"LiLei\";")
        doc = cur.fetchone()
        cur.execute("UPDATE my_test SET age = %s WHERE name = \"LiLei\";", params=(doc["age"] + 1,))
        trans_client.commit()
    with LazyMySQLClient.from_url(url) as client:
        print("Current table data:")
        for doc in client.find(table="my_test"):
            print(doc)
        print()

    ################################################################################
    # Drop Table
    # There is no shortcuts for dropping table, since it's dangerous.
    ################################################################################
    with LazyMySQLClient.from_url(url) as client:
        sql = "DROP TABLE my_test;"
        cur = client.execute(sql)
        if cur.close():
            print("Table dropped.")
        else:
            print("Failed to drop table.")

    ################################################################################
    # Check table exists
    ################################################################################
    with LazyMySQLClient.from_url(url) as client:
        if client.table_exists("my_test"):
            print("my_test exists.")
        else:
            print("my_test does not exist.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
