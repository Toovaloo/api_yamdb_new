import sqlite3

import pandas as pd

TABLES = {
    'api_category': 'data/category.csv',
    'api_comment': 'data/comments.csv',
    'api_genre': 'data/genre.csv',
    'api_review': 'data/review.csv',
    'api_title': 'data/titles.csv',
    'api_title_genre': 'data/genre_title.csv',
    # 'api_user': 'data/users.csv',
}

cnx = sqlite3.connect('db.sqlite3')

for table_name, path in TABLES.items():
    with open(path, 'r', encoding='UTF8') as csv_file:
        df = pd.read_csv(csv_file)
        df.to_sql(table_name, cnx, if_exists='append', index=False)
        print(f'В БД обновлена таблица {table_name}.')


with open('data/users.csv', 'r', encoding='UTF8') as csv_file:
    df = pd.read_csv(csv_file)
    df.fillna('', inplace=True)
    num_of_users = df.shape[0]
    df['is_superuser'] = ['False'] * num_of_users
    df['is_staff'] = ['False'] * num_of_users
    df['is_active'] = ['False'] * num_of_users
    df['date_joined'] = ['2020-01-13T23:20:02.422Z'] * num_of_users
    df['password'] = ['123'] * num_of_users
    df.to_sql('api_user', cnx, if_exists='append', index=False)
    print('В БД обновлена таблица api_user.')

cnx.close()

print('Скрипт выполнился.')
