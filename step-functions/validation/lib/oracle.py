""" 
Copyright 2023 Amazon.com, Inc. and its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at

  http://aws.amazon.com/asl/

or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
"""

import oracledb
import traceback


class Connection:
    def __init__(self, hostname, port, username, password, database, oracle_owner):
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.oracle_owner = oracle_owner

    def get_query_results(self, query):
        try:
            connection = oracledb.connect(
                user=self.username,
                password=self.password,
                dsn=f"{self.hostname}:{self.port}/{self.database}",
            )
            cursor = connection.cursor()
            cursor.execute(query)
            response = cursor.fetchall()
            return response
        except Exception as e:
            print(traceback.format_exc())
            raise
        finally:
            connection.close()
