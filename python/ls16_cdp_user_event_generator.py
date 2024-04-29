import csv
import random
import datetime
import json
from faker import Faker

fake = Faker()


def generate_fake_data(batch_size):
    data = []
    for _ in range(batch_size):
        event_time = fake.past_datetime(start_date="-100d", tzinfo=None)
        event_type = random.choice(
            ['pay', 'add_shop_cat', 'browse', 'recharge', 'refund', 'buy', 'login', 'click', 'search'])
        unique_user_id = random.randint(1, 10000000)
        page_id = random.randint(1, 2000)
        item_id = [random.randint(1, 100000) for _ in range(random.randint(1, 5))]
        total_amount = round(random.uniform(10, 20000), 2)
        device_type = random.choice(['mobile', 'tablet', 'desktop'])

        # 生成随机的 JSON 数据
        random_json_data = {
            'page_id': page_id,
            'item_id': item_id,
            'search_key': fake.word(),
            'seller': fake.name()
        }
        event_param = json.dumps(random_json_data)

        location = random.choice(['city1', 'city2', 'store1', 'store2'])

        data.append([event_time, event_type, unique_user_id, page_id, item_id, total_amount, device_type,
                     event_param, location])

    return data


def save_data_to_csv(data, file_name):
    with open(file_name, 'a', newline='') as file:
        writer = csv.writer(file, delimiter='#')
        writer.writerows(data)


def main(total_rows, batch_size):
    file_name = 'cdp_user_event_data_part7_2KW.csv'

    for i in range(total_rows // batch_size):
        data = generate_fake_data(batch_size)
        save_data_to_csv(data, file_name)
        print(f"已写入 {i}*{batch_size} 行数据")

    remaining_rows = total_rows % batch_size
    if remaining_rows > 0:
        data = generate_fake_data(remaining_rows)
        save_data_to_csv(data, file_name)
        print(f"已写入 {remaining_rows} 行数据")

    print(f"生成的数据已保存到 {file_name}")


if __name__ == "__main__":
    total_rows = 20000000  # 设置生成数据的总量
    batch_size = 500000  # 设置每批次生成的数据量
    main(total_rows, batch_size)
