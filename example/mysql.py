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
    client = LazyMySQLClient.from_url(url)

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
    cur = client.execute(sql)
    if cur.close():
        print("Table created.")
    else:
        print("Failed to create table.")
    client.commit()
    print()

    ################################################################################
    # Insert Data
    # Although you can write your own SQL to insert data, there is a more convenient
    # way to `insert`, `find`, `update` and `delete` samples.
    ################################################################################
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
    url.path = url.path.rstrip() + "/my_test"
    print(f"Table URL: {url}")
    reader = DocReader.from_url(url)
    for doc in reader:
        print(doc)
    print()

    ################################################################################
    # Update Data
    ################################################################################
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
    if client.delete(where="age > 18", table="my_test"):
        print("Data deleted.")
    else:
        print("Failed to delete.")
    print("Current table data:")
    for doc in client.find(table="my_test"):
        print(doc)
    print()

    ################################################################################
    # Drop Table
    # There is no shortcuts for dropping table, since it's dangerous.
    ################################################################################
    sql = "DROP TABLE my_test;"
    cur = client.execute(sql)
    if cur.close():
        print("Table dropped.")
    else:
        print("Failed to drop table.")
    client.commit()

    ################################################################################
    # Check table exists
    ################################################################################
    if client.table_exists("my_test"):
        print("my_test exists.")
    else:
        print("my_test does not exist.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
