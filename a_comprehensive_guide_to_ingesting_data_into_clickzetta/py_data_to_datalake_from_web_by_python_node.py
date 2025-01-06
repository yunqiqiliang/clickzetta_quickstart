import os,io
import subprocess
from datetime import datetime, timedelta
import oss2

# 阿里云OSS配置
ACCESS_KEY_ID = '${ak}'
ACCESS_KEY_SECRET = '${sk}'
BUCKET_NAME = '你的bucketname'
ENDPOINT = 'oss-cn-hangzhou-internal.aliyuncs.com'
ROOT_PATH = f'{BUCKET_NAME}/ingest_demo/from_web'

try:
    # 构建wget命令
    url = f"https://github.com/yunqiqiliang/clickzetta_quickstart/blob/main/a_comprehensive_guide_to_ingesting_data_into_clickzetta/data/lift_tickets_data.csv.gz"
    cmd = ["wget", "-qO-", url]
    print(f"wget cmd: {cmd}")

    # 执行wget命令并捕获输出
    wget_output = subprocess.check_output(cmd)
    print(f"Wget file done...")

    # 将输出转换为内存中的文件对象
    file_obj = io.BytesIO(wget_output)
except Exception as e:
    print(f"An error occurred: {e}")
    file_obj = None
    raise

if file_obj:
    try:
        # 初始化阿里云OSS
        auth = oss2.Auth(ACCESS_KEY_ID, ACCESS_KEY_SECRET)
        bucket = oss2.Bucket(auth, ENDPOINT, BUCKET_NAME)

        # 上传文件到OSS
        oss_path = f"{ROOT_PATH}/lift_tickets_data.csv.gz"
        print(f"osspath: {oss_path}")
        bucket.put_object(oss_path, file_obj)
        print(f"Put file to oss done...")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # 关闭内存中的文件对象
        file_obj.close()