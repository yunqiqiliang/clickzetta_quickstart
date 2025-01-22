from create_bronze_tables import create_tables as create_bronze
from create_gold_tables import create_tables as create_gold
from create_interface_views import create_tables as create_interface
from create_silver_tables import create_tables as create_silver
from clickzetta.zettapark.session import Session
import json

if __name__ == "__main__":
    # 从配置文件中读取参数
    with open('adventureworks/config/config.json', 'r') as config_file:
        config = json.load(config_file)
    print("正在连接到云器Lakehouse.....\n")
    clickzetta = Session.builder.configs(config).create()
    print("连接成功！...\n")
    create_bronze(clickzetta)
    create_silver(clickzetta)
    create_gold(clickzetta)
    create_interface(clickzetta)
    print("建表成功！...\n")
    clickzetta.close()
