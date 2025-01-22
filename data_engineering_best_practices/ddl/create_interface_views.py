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
dst_schema_name = "businessintelligence"

def create_tables(
    clickzetta,
    dst_schema: str = dst_schema_name,
    src_schema: str = schema_name,
):
    clickzetta.sql(f"CREATE SCHEMA IF NOT EXISTS {dst_schema}").collect()
    clickzetta.sql(f"DROP VIEW IF EXISTS {dst_schema}.sales_mart").collect()
    clickzetta.sql(
        f"""
              CREATE VIEW IF NOT EXISTS {dst_schema}.sales_mart
              AS
              SELECT state_id
              , deliver_date as delivery_date
              , num_orders
              FROM {src_schema}.sales_mart
              WHERE partition in
              (SELECT max(partition) FROM {src_schema}.sales_mart)
              """
    ).collect()


def drop_tables(clickzetta, schema: str = dst_schema_name):
    clickzetta.sql(f"DROP VIEW IF EXISTS {schema}.sales_mart").collect()
    clickzetta.sql(f"DROP SCHEMA IF EXISTS {schema}").collect()

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
