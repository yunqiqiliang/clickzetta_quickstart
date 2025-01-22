# ********************************************************************#
# author: qiliang
# create time: 2025-01-15 14:03:46
# ********************************************************************#
import subprocess
import sys

# Install necessary packages
subprocess.run([sys.executable, "-m", "pip", "install", "clickzetta_zettapark_python", "--target", "/home/system_normal", "-i", "https://pypi.tuna.tsinghua.edu.cn/simple"], stderr=subprocess.DEVNULL)
sys.path.append('/home/system_normal')

schema_name = ${schema}

def create_tables(
    clickzetta,
    schema: str = schema_name,
):
    clickzetta.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}").collect()

    clickzetta.sql(f"DROP TABLE IF EXISTS {schema}.customer").collect()
    clickzetta.sql(
        f"""
              CREATE TABLE {schema}.customer (
                id INT,
                first_name STRING,
                last_name STRING,
                state_id STRING,
                datetime_created TIMESTAMP,
                datetime_updated TIMESTAMP,
                etl_inserted TIMESTAMP,
                partition STRING
                ) 
                PARTITIONED BY (partition)
              """
    ).collect()

    clickzetta.sql(f"DROP TABLE IF EXISTS {schema}.orders").collect()
    clickzetta.sql(
        f"""
              CREATE TABLE {schema}.orders (
                order_id STRING,
                customer_id INT,
                item_id STRING,
                item_name STRING,
                delivered_on TIMESTAMP,
                datetime_order_placed TIMESTAMP,
                etl_inserted TIMESTAMP,
                partition STRING
                ) 
                PARTITIONED BY (partition)
              """
    ).collect()


def drop_tables(
    clickzetta,
    schema: str = "adventureworks",
):
    clickzetta.sql(f"DROP TABLE IF EXISTS {schema}.customer").collect()
    clickzetta.sql(f"DROP TABLE IF EXISTS {schema}.orders").collect()
    clickzetta.sql(f"DROP schema IF EXISTS {schema}").collect()

if __name__ == '__main__':
    hints = dict()
    hints['sdk.job.timeout'] = 3
    hints['query_tag'] = 'test_conn_hints_zettapark'
    connection_parameters = {
        "service": ${service},
        "instance": ${instance},
        "workspace": ${workspace},
        "schema": ${schema},
        "vcluster": ${vcluster},
        "username": ${username},
        "password": ${password},
        "sdk_job_timeout": 10,
        "hints": hints,
    }
    from clickzetta.zettapark.session import Session
    clickzetta = Session.builder.configs(connection_parameters).create()
    create_tables(clickzetta)
    clickzetta.close()
    print("Table Created")
