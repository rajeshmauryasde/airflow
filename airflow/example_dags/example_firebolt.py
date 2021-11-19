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
"""
Example use of Firebolt related operators.
"""
from datetime import datetime

from airflow import DAG
from airflow.providers.firebolt.operators.firebolt import FireboltOperator

FIREBOLT_CONN_ID = 'firebolt_conn_id'
FIREBOLT_SAMPLE_TABLE = 'sample_table'

# SQL commands
CREATE_TABLE_SQL_STRING = (
    f"select * from lineitem limit 1"
)
SQL_INSERT_STATEMENT = f"select * from lineitem limit 1"
SQL_LIST = ["select * from lineitem limit 1"]

# [START howto_operator_snowflake]

dag = DAG(
    'example_firebolt',
    start_date=datetime(2021, 1, 1),
    default_args={'firebolt_conn_id': FIREBOLT_CONN_ID},
    tags=['example'],
    catchup=False,
)


firebolt_op_sql_str = FireboltOperator(
    task_id='firebolt_op_sql_str',
    dag=dag,
    sql=CREATE_TABLE_SQL_STRING,
)

firebolt_op_with_params = FireboltOperator(
    task_id='firebolt_op_with_params',
    dag=dag,
    sql=SQL_INSERT_STATEMENT,
    parameters={"id": 56},
)

firebolt_op_sql_list = FireboltOperator(
    task_id='firebolt_op_sql_list',
    dag=dag,
    sql=SQL_LIST,
)

# firebolt_op_template_file = FireboltOperator(
#     task_id='firebolt_op_template_file',
#     dag=dag,
#     sql='/path/to/sql/<filename>.sql',
# )

(
    firebolt_op_sql_str
    >> [
        firebolt_op_with_params,
        firebolt_op_sql_list,
    ]
)
