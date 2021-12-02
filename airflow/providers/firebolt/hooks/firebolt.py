#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
from firebolt.db import Connection
from typing import Optional, Union, Any, Dict
from contextlib import closing

from airflow.hooks.dbapi import DbApiHook


class FireboltHook(DbApiHook):
    """Interact with Firebolt."""

    conn_name_attr = 'firebolt_conn_id'
    default_conn_name = 'firebolt_default'
    conn_type = 'firebolt'
    hook_name = 'Firebolt'

    @staticmethod
    def get_connection_form_widgets() -> Dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "extra__firebolt__api": StringField(lazy_gettext('Api End Point'), widget=BS3TextFieldWidget()),
        }

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        """Returns custom field behaviour"""

        return {
            "hidden_fields": ['port', 'extra'],
            "relabeling": {'schema': 'Database', 'host': 'Engine URL'},
            "placeholders": {
                'host': 'firebolt engine url',
                'schema': 'firebolt database',
                'login': 'firebolt userid',
                'password': 'password',
                'extra__firebolt__api': 'firebolt api end point',
            },
        }

    def get_conn(self) -> Connection:
        """Return Firebolt connection object"""
        conn = self.get_connection(self.firebolt_conn_id)

        conn_config = {
            "username": conn.login,
            "password": conn.password or '',
            "database": conn.schema,
            "engine_url": conn.host or 'localhost',
            "api_endpoint": conn.extra_dejson.get('extra__firebolt__api', 'api.app.firebolt.io')
        }

        conn = Connection(**conn_config)
        return conn

    def run(self, sql: Union[str, list], autocommit: bool = False, parameters: Optional[dict] = None) -> None:
        """
        Runs a command or a list of commands. Pass a list of sql
        statements to the sql parameter to get them to execute
        sequentially
        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        :param autocommit: What to set the connection's autocommit setting to
            before executing the query.
        :type autocommit: bool
        :param parameters: The parameters to render the SQL query with.
        :type parameters: dict or iterable
        """
        scalar = isinstance(sql, str)
        if scalar:
            sql = [sql]

        with closing(self.get_conn()) as conn:
            for query in sql:
                self.log.info(query)
                with closing(conn.cursor()) as cursor:
                    for sql_statement in sql:
                        self.log.info(f"Running statement: {sql_statement}, parameter: {parameters}")
                        if parameters:
                            cursor.execute(sql_statement, parameters)
                        else:
                            cursor.execute(sql_statement)

                        execution_info = []
                        for row in cursor:
                            self.log.info(f"Statement execution info - {row}")
                            execution_info.append(row)

                        self.log.info(f"Rows affected: {cursor.rowcount}")

        return execution_info

    def test_connection(self):
        """Test the Firebolt connection by running a simple query."""
        try:
            self.run(sql="select 1")
        except Exception as e:
            return False, str(e)
        return True, "Connection successfully tested"
