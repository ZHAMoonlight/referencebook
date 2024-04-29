import csv
import random
from faker import Faker

fake = Faker()

total_num = 1


def generate_fake_data(num_rows):
    global total_num
    data = []
    for _ in range(num_rows):
        unique_user_id = total_num
        total_num = total_num + 1
        name = fake.name()
        nickname = fake.user_name()
        gender = random.choice([1, 2, 3])
        birthday = fake.date_of_birth().strftime('%Y%m%d')
        user_level = random.randint(1, 5)
        register_date = fake.past_datetime(start_date="-100d", tzinfo=None)
        last_login_time = fake.past_datetime(start_date="-1d", tzinfo=None)

        data.append([unique_user_id, name, nickname, gender, birthday, user_level, register_date, last_login_time])

    return data


def save_data_to_csv(data, file_name):
    with open(file_name, 'a', newline='') as file:
        writer = csv.writer(file)
        # writer.writerow(['unique_user_id', 'name', 'nickname', 'gender', 'birthday', 'user_level', 'register_date',
        #                  'last_login_time'])
        writer.writerows(data)


def main(total_rows, batch_size):
    file_name = 'cdp_user_data.csv'

    for i in range(total_rows // batch_size):
        data = generate_fake_data(batch_size)
        save_data_to_csv(data, file_name)
        print(f"已写入 {batch_size} 行数据")

    remaining_rows = total_rows % batch_size
    if remaining_rows > 0:
        data = generate_fake_data(remaining_rows)
        save_data_to_csv(data, file_name)
        print(f"已写入 {remaining_rows} 行数据")

    print(f"生成的数据已保存到 {file_name}")


if __name__ == "__main__":
    total_rows = 10000000  # 设置生成数据的总量
    batch_size = 100000  # 设置每批次生成的数据量
    main(total_rows, batch_size)