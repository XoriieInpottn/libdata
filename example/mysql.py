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

    url = args.url
    print(url)
    client = LazyMySQLClient.from_url(url)

    ################################################################################
    # Create Table
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
    ################################################################################
    sql = f"INSERT INTO my_test (name, age, create_time) VALUES (%s, %s, %s);"
    now = datetime.now()
    values = [
        ("LiLei", 16, now),
        ("HanMeimei", 14, now),
        ("LinTao", 20, now),
    ]
    for value in values:
        cur = client.execute(sql, params=value)
        if cur.close():
            print("One sample inserted.")
        else:
            print("Failed insert one sample.")
    client.commit()

    print("Current table data:")
    sql = "SELECT * FROM my_test;"
    with client.execute(sql, dictionary=True) as cur:
        for doc in cur:
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
    sql = "UPDATE my_test SET age = 17 WHERE name = \"HanMeimei\";"
    cur = client.execute(sql)
    if cur.close():
        print("Updated.")
    else:
        print("Failed to update.")
    client.commit()

    print("Current table data:")
    sql = "SELECT * FROM my_test;"
    with client.execute(sql, dictionary=True) as cur:
        for doc in cur:
            print(doc)
    print()

    ################################################################################
    # Delete Data
    ################################################################################
    sql = "DELETE FROM my_test WHERE age > 18;"
    cur = client.execute(sql)
    if cur.close():
        print("Deleted.")
    else:
        print("Failed to delete.")
    client.commit()

    print("Current table data:")
    sql = "SELECT * FROM my_test;"
    with client.execute(sql, dictionary=True) as cur:
        for doc in cur:
            print(doc)
    print()

    ################################################################################
    # Drop Table
    ################################################################################
    sql = "DROP TABLE my_test;"
    cur = client.execute(sql)
    if cur.close():
        print("Table dropped.")
    else:
        print("Failed to drop table.")
    client.commit()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
