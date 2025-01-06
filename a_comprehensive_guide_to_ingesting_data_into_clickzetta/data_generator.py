import os
import sys
import rapidjson as json
import optional_faker as _
import uuid
import random
import csv
import gzip
from dotenv import load_dotenv
from faker import Faker
from datetime import date, datetime, timedelta

load_dotenv()
fake = Faker('zh_CN')  # 使用中文区域
resorts = ["大董烤鸭", "京雅堂", "新荣记", "仿膳饭庄", "全聚德", 
           "利群烤鸭店", "鼎泰丰", "海底捞", "江苏会所", "店客店来",
           "周黑鸭", "夜上海", "香宫", "长安壹号", "翡翠餐厅", "北京饭店",
           "四川豆花饭庄", "海底捞火锅", "川办餐厅", "南门火锅",
           "胡同", "翠园", "利苑酒家", "御宝轩", "金鼎轩", 
           "外婆家", "大董", "顺峰海鲜酒家", "小龙坎火锅",
           "新大陆中餐厅", "京兆尹", "鼎泰丰（台湾）", "滇池来客", 
           "绿波廊", "南美时光"]

# 定义数据保存目录
data_dir = 'data'

def random_date_in_2025():
    start_date = date(2025, 1, 1)
    end_date = date(2025, 12, 31)
    return start_date + timedelta(days=random.randint(0, (end_date - start_date).days))

def random_datetime_between(start_year, end_year):
    start_datetime = datetime(start_year, 1, 1)
    end_datetime = datetime(end_year, 12, 31, 23, 59, 59)
    random_seconds = random.randint(0, int((end_datetime - start_datetime).total_seconds()))
    random_datetime = start_datetime + timedelta(seconds=random_seconds)
    return random_datetime.strftime('%Y-%m-%d %H:%M:%S')

def print_lift_ticket(json_file, csv_file, dict_writer):
    global resorts, fake
    lift_ticket = {'txid': str(uuid.uuid4()),
                   'rfid': hex(random.getrandbits(96)),
                   'resort': fake.random_element(elements=resorts),
                   'purchase_time': random_datetime_between(2021, 2024),
                   'expiration_time': random_date_in_2025().isoformat(),
                   'days': fake.random_int(min=1, max=7),
                   'name': fake.name(),
                   'address_street': fake.street_address(),
                   'address_city': fake.city(),
                   'address_state': fake.province(),
                   'address_postalcode': fake.postcode(),
                   'phone': fake.phone_number(),
                   'email': fake.email(),
                   'emergency_contact_name': fake.name(),
                   'emergency_contact_phone': fake.phone_number(),
    }

    # 保存到 JSON 文件
    json_file.write(json.dumps(lift_ticket) + '\n')
    
    # 保存到 CSV 文件
    dict_writer.writerow(lift_ticket)
    
    # 生成额外的相关数据
    generate_lift_usage_data(lift_ticket)
    generate_feedback_data(lift_ticket)
    generate_incident_reports(lift_ticket)
    generate_weather_data(lift_ticket)
    generate_accommodation_data(lift_ticket)

def generate_lift_usage_data(lift_ticket):
    with open(os.path.join(data_dir, 'lift_usage_data.json'), 'a', encoding='utf-8') as lift_usage_json_file, \
         open(os.path.join(data_dir, 'lift_usage_data.csv'), 'a', newline='', encoding='utf-8') as lift_usage_csv_file:
        usage = {'txid': lift_ticket['txid'],
                 'usage_time': random_datetime_between(2021, 2024),
                 'lift_id': fake.random_int(min=1, max=20)}
        lift_usage_json_file.write(json.dumps(usage) + '\n')
        csv.DictWriter(lift_usage_csv_file, fieldnames=usage.keys()).writerow(usage)

def generate_feedback_data(lift_ticket):
    with open(os.path.join(data_dir, 'feedback_data.json'), 'a', encoding='utf-8') as feedback_json_file, \
         open(os.path.join(data_dir, 'feedback_data.csv'), 'a', newline='', encoding='utf-8') as feedback_csv_file:
        feedback = {'txid': lift_ticket['txid'],
                    'resort': lift_ticket['resort'],
                    'feedback_time': random_datetime_between(2021, 2024),
                    'rating': fake.random_int(min=1, max=5),
                    'comment': fake.sentence()}
        feedback_json_file.write(json.dumps(feedback) + '\n')
        csv.DictWriter(feedback_csv_file, fieldnames=feedback.keys()).writerow(feedback)

def generate_incident_reports(lift_ticket):
    with open(os.path.join(data_dir, 'incident_reports.json'), 'a', encoding='utf-8') as incident_json_file, \
         open(os.path.join(data_dir, 'incident_reports.csv'), 'a', newline='', encoding='utf-8') as incident_csv_file:
        incident = {'txid': lift_ticket['txid'],
                    'incident_time': random_datetime_between(2021, 2024),
                    'incident_type': fake.word(),
                    'description': fake.text()}
        incident_json_file.write(json.dumps(incident) + '\n')
        csv.DictWriter(incident_csv_file, fieldnames=incident.keys()).writerow(incident)

def generate_weather_data(lift_ticket):
    with open(os.path.join(data_dir, 'weather_data.json'), 'a', encoding='utf-8') as weather_json_file, \
         open(os.path.join(data_dir, 'weather_data.csv'), 'a', newline='', encoding='utf-8') as weather_csv_file:
        weather = {'resort': lift_ticket['resort'],
                   'date': lift_ticket['purchase_time'].split(' ')[0],
                   'temperature': random.uniform(-10, 10),
                   'condition': fake.word()}
        weather_json_file.write(json.dumps(weather) + '\n')
        csv.DictWriter(weather_csv_file, fieldnames=weather.keys()).writerow(weather)

def generate_accommodation_data(lift_ticket):
    with open(os.path.join(data_dir, 'accommodation_data.json'), 'a', encoding='utf-8') as accommodation_json_file, \
         open(os.path.join(data_dir, 'accommodation_data.csv'), 'a', newline='', encoding='utf-8') as accommodation_csv_file:
        accommodation = {'txid': lift_ticket['txid'],
                         'hotel_name': fake.company(),
                         'room_type': fake.word(),
                         'check_in': random_datetime_between(2021, 2024),
                         'check_out': random_datetime_between(2021, 2024)}
        accommodation_json_file.write(json.dumps(accommodation) + '\n')
        csv.DictWriter(accommodation_csv_file, fieldnames=accommodation.keys()).writerow(accommodation)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("请提供生成条数的参数。例如：python data_generator.py 100")
        sys.exit(1)
    
    total_count = int(sys.argv[1])
    
    os.makedirs(data_dir, exist_ok=True)
    
    with open(os.path.join(data_dir, 'lift_tickets_data.json'), 'w', encoding='utf-8') as json_file, \
         open(os.path.join(data_dir, 'lift_tickets_data.csv'), 'w', newline='', encoding='utf-8') as csv_file, \
         gzip.open(os.path.join(data_dir, 'lift_tickets_data.json.gz'), 'wt', encoding='utf-8') as json_gzip_file, \
         gzip.open(os.path.join(data_dir, 'lift_tickets_data.csv.gz'), 'wt', newline='', encoding='utf-8') as csv_gzip_file:
        
        keys = ['txid', 'rfid', 'resort', 'purchase_time', 'expiration_time', 'days', 'name', 'address_street',
                'address_city', 'address_state', 'address_postalcode', 'phone', 'email', 'emergency_contact_name',
                'emergency_contact_phone']
        dict_writer = csv.DictWriter(csv_file, fieldnames=keys)
        dict_writer.writeheader()
        
        gzip_dict_writer = csv.DictWriter(json_gzip_file, fieldnames=keys)
        gzip_dict_writer.writeheader()
        
        for _ in range(total_count):
            print_lift_ticket(json_file, csv_file, dict_writer)
            print_lift_ticket(json_gzip_file, csv_gzip_file, gzip_dict_writer)
