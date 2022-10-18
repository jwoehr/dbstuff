#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
mariadb_to_mongo.py

Convert table from MySQL/MariaDB to mongodb.
Uses the mariadb driver and pymongo

Created on Sat Aug  8 21:00:44 2020

@author: jwoehr
Copyright 2020, 2022 Jack Woehr jwoehr@softwoehr.com PO Box 82, Beulah, CO 81023-0082.
Apache-2 -- See LICENSE which you should have received with this code.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
WITHOUT ANY EXPRESS OR IMPLIED WARRANTIES.
"""

import argparse
import decimal
import datetime

# import json
import sys
from typing import List
import mariadb
from pymongo import MongoClient
from pymongo.results import InsertManyResult
from bson.decimal128 import Decimal128

ListDict = List[dict]


class MariaDBToMongo:
    """ """

    def __init__(self):
        """
        Ctor

        Returns
        -------
        None.

        """
        self.connection = None
        self.client = None

    def connect(self, host: str, database: str, user: str, password: str) -> None:
        """
        Connect to the MySQL MariaDB instance

        Parameters
        ----------
        host : str
            Host name or URL.
        database : str
            Database name.
        user : str
            User id
        password : str
            password.

        Returns
        -------
        None.

        """
        try:
            self.connection = mariadb.connect(
                user=user, password=password, host=host, database=database
            )
        except mariadb.Error as err:
            print(err)

    def open_client(self, uri: str) -> None:
        self.client = MongoClient(uri)

    def close_client(self) -> None:
        self.client.close()
        self.client = None

    def close_connection(self) -> None:
        self.connection.close()
        self.connection = None

    def close(self) -> None:
        """
        Close the MySQL/MariaDB and MongoDB connections if open.

        Returns
        -------
        None.

        """
        if self.connection:
            self.close_connection()
        if self.client:
            self.close_client()

    def get_column_names(self, table_name: str) -> List:
        """
        Get ordered list of column names for table

        Parameters
        ----------
        table_name : str
            name of table for which columns sought
            table schema is that of the connection

        Returns
        -------
        List
            ordered list of column names

        """
        _col_list = []
        _c = self.connection.cursor()
        _c.execute(
            "select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='{}'".format(
                table_name
            )
        )
        for row in _c:
            _col_list.append(row[0])
        _c.close()
        return _col_list

    def get_rows_dict_list(self, table_name: str) -> ListDict:
        """
        Get the table rows as a list of dictionaries.
        Convert Decimal and Date values along the way.

        Parameters
        ----------
        table_name : str
            The original table name

        Returns
        -------
        ListDict
            the table rows as a list of dictionaries

        """
        col_names = self.get_column_names(table_name)
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM {}".format(table_name))
        rows_dict_list = []
        for row in cursor:
            row_dict = {}
            for i in range(len(row)):
                col_name = col_names[i]
                value = row[i]
                if isinstance(value, decimal.Decimal):
                    value = Decimal128(value)
                elif isinstance(value, datetime.date):
                    value = datetime.datetime(
                        value.year, value.month, value.day, 0, 0, 0, 0
                    )
                elif isinstance(value, datetime.timedelta):
                    value = datetime.datetime.utcnow()
                row_dict[col_name] = value
            rows_dict_list.append(row_dict)
        return rows_dict_list

    def put_rows(
        self, rows: ListDict, db_name: str, collection_name: str
    ) -> InsertManyResult:
        """
        Put the table rows into the collection

        Parameters
        ----------
        rows : ListDict
            the list of dictionaries representing the original table.
        db_name : str
            the MongoDB dbname
        collection_name : str
            the MongoDB collection name

        Returns
        -------
        InsertManyResult
            Return from Pymongo.
            See https://pymongo.readthedocs.io/en/stable/api/pymongo/results.html

        """
        db = self.client[db_name]
        collection = db[collection_name]
        return collection.insert_many(rows)

    def get_put_rows(
        self, table_name: str, db_name: str, collection_name: str
    ) -> InsertManyResult:
        """
        Get the rows from the original table and put the rows as documents
        in the collection

        Parameters
        ----------
        table_name : str
            the original table name
        db_name : str
            the original db name
        collection_name : str
            name of the NongoDB collection.

        Returns
        -------
        InsertManyResult
           Return from Pymongo.
           See https://pymongo.readthedocs.io/en/stable/api/pymongo/results.html


        """
        return self.put_rows(
            self.get_rows_dict_list(table_name), db_name, collection_name
        )


if __name__ == "__main__":
    EXPLANATION = """
    mariadb_to_mongo.py Convert MySQL or MariaDB database table to MongoDB document collection.
    Exits 0 on success, 1 on argument error, 100 on runtime error.
    Copyright 2020, 2022 Jack Woehr jwoehr@softwoehr.com PO Box 82, Beulah, CO 81023-0082.
    Apache-2 license -- See LICENSE which you should have received with this code.
    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
    WITHOUT ANY EXPRESS OR IMPLIED WARRANTIES.
    """
    PARSER = argparse.ArgumentParser(description=EXPLANATION)
    PARSER.add_argument(
        "-m", "--mariadb", action="store", required=True, help="mariadb host"
    )
    PARSER.add_argument(
        "-o", "--mongodb", action="store", required=True, help="mongodb uri"
    )
    PARSER.add_argument(
        "-s", "--sourcedb", action="store", required=True, help="mariadb source db"
    )
    PARSER.add_argument(
        "-d", "--targetdb", action="store", required=True, help="mongodb target db"
    )
    PARSER.add_argument(
        "-t", "--table", action="store", required=True, help="mariadb source table"
    )
    PARSER.add_argument(
        "-c",
        "--collection",
        action="store",
        required=True,
        help="mongodb target collection",
    )
    PARSER.add_argument(
        "-u", "--user", action="store", required=True, help="mariadb user"
    )
    PARSER.add_argument(
        "-p", "--password", action="store", required=True, help="mariadb password"
    )

    ARGS = PARSER.parse_args()
    HOST = ARGS.mariadb
    DATABASE = ARGS.sourcedb
    USER = ARGS.user
    PASSWORD = ARGS.password
    URI = ARGS.mongodb
    SRCTBL = ARGS.table
    TARGDB = ARGS.targetdb
    COLL = ARGS.collection

    M_T_M = MariaDBToMongo()
    M_T_M.connect(HOST, DATABASE, USER, PASSWORD)
    M_T_M.open_client(URI)
    M_T_M.get_put_rows(SRCTBL, TARGDB, COLL)
    M_T_M.close()
    sys.exit(0)
