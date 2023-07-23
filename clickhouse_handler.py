import clickhouse_connect

def get_client(host, port, user, password, db_name, https = True):
    return clickhouse_connect.get_client(interface = 'https' if https else 'http', host=host, port=port, username=user, password=password, database=db_name)

def get_columns_and_types_clickhouse(table, host, port, user, password, db_name):
    url = f"https://{host}:{port}/?database={db_name}"
    query = f"DESCRIBE TABLE {table}"
    response = requests.post(url, data=query, auth=HTTPBasicAuth(user, password))
    data = json.loads(response.text)

    columns = [(item['name'], item['type']) for item in data]
    return columns

def test_connection(host, port, user, password, db_name):
    client = get_client(host, port, user, password, db_name)
    try: 
        client.command('SELECT 1')
        print("Successfully connected to the database.")
    except:
        print("Failed to connect to the database.")


def load_data_to_sql(data, table, fields_matching, host, port, user, password, db_name):
    url = f"https://{host}:{port}/?database={db_name}"
    total = len(data)
    progress_bar = tqdm(total=total, position=0, leave=True)
    k = 0

    for row in data:
        row = {fields_matching.get(key, key): value for key, value in row.items()}
        columns = ', '.join(row.keys())
        values = ', '.join([str(x) for x in row.values()])
        query = f"INSERT INTO {table} ({columns}) VALUES ({values})"
        response = requests.post(url, data=query, auth=HTTPBasicAuth(user, password))

        k += 1
        if k % 100 == 0:
            progress_bar.update(100)
