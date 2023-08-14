import json
import time
import requests
import yaml
from handlers import sql_handler, postgresql_handler, clickhouse_handler
import shutil
import preprocessing_functions as pf
import warnings
from tqdm import tqdm
preprocessing_done = {}

# TODO решить с разруливанием списочных полей:
# Сделать в конфиге отдельный раздел ручных лямбд для списочных полей

## TODO сделать автоматическое архивирование функции и удаление системных файлов 
# через zip -d function.zip "__MACOSX/*"

has_errors = False

# MARK: MAPPERS
table_settings_mapper = {
    'deal': 'deal_fields',
    'contact': 'contact_fields',
    'company': 'company_fields',
    'lead': 'lead_fields',
}

database_import_mapper = {
    'MySQL': sql_handler,
    'PostgreSQL': postgresql_handler,
    'ClickHouse': clickhouse_handler
}

obligatory_fields = ['ID', 'DATE_CREATE']

# CONFIGURE
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)
preprocessing_config = {}
list_of_errors = []
database_type = config.get('database_type')

if database_type in database_import_mapper:
    handler = database_import_mapper[database_type]

    # MARK: COPY FILES
    destination_file = 'cf/db_handler.py'
    source_file = f'handlers/{handler.__name__.split(".")[-1]}.py'
    shutil.copyfile(source_file, destination_file)
    destination_file = 'cf/requirements.txt'
    source_file = f'requirements/{handler.__name__.split(".")[-1]}_requirements.txt'
    shutil.copyfile(source_file, destination_file)
    destination_file = 'cf/preprocessing_functions.py'
    source_file = 'preprocessing_functions.py'
    shutil.copyfile(source_file, destination_file)
    destination_file = 'cf/config.yaml'
    source_file = 'config.yaml'
    shutil.copyfile(source_file, destination_file)
else:
    print('Unsupported database type')
    # docker_service.close()

# MARK: CHECK CONNECTION
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)
handler.test_connection(**config['db'])
columns_dump = {}
# MARK: CHECK ERRORS
for table_type in config['table_names'].keys():
    print(table_type)
    if config['table_names'][table_type]:
        preprocessing_config[table_type] = {}

        # MergeTree check for ClickHouse
        e,d = handler.check_tabletype_errors(config['table_names'][table_type], **config['db'])
        if e:
            list_of_errors.append(d)
        columns = handler.get_columns_and_types(
            config['table_names'][table_type], **config['db'])
        columns_dump[table_type] = columns
        for column in columns_dump[table_type]:
            column['type'] = str(column['type'])
        dict_of_columns = {column['name']: (
            column['type'], column['nullable']) for column in columns}
        # print(dict_of_columns)
        
        # columns_dump = dict_of_columns
        for obligatory_field in obligatory_fields:
            if obligatory_field not in config[table_settings_mapper[table_type]].keys():
                if has_errors == False:
                    has_errors = True
                list_of_errors.append(
                    f"Column {obligatory_field} not found in config.yaml in {table_settings_mapper[table_type]} settings. \
It is obligatory field.")

        for column in config[table_settings_mapper[table_type]]:
            if config[table_settings_mapper[table_type]][column] not in dict_of_columns.keys():
                if has_errors == False:
                    has_errors = True
                list_of_errors.append(
                    f"Column {column} not found in table {table_settings_mapper[table_type]}. Check settings.")
            else:
                nullable = dict_of_columns[config[table_settings_mapper[table_type]][column]][1]
                target_data_type = str(dict_of_columns[config[table_settings_mapper[table_type]][column]][0])
                # print(target_data_type)
                current_preproc = preprocessing_config[table_type].get(
                    column, [])

                if column in config[f'{table_type}_functions'].keys():
                    function_name = config[f'{table_type}_functions'][column]
                    current_preproc = [getattr(pf, function_name)]
                else:
                    if nullable:
                        current_preproc.append(lambda x: pf.void_to_null(x))
                    else:
                        current_preproc.append(lambda x: pf.safe_from_null(x)) 
                    if ("String" in target_data_type) or ('VARCHAR' in target_data_type) or ('TEXT' in target_data_type) or ('CHAR' in target_data_type) or ('TINYTEXT' in target_data_type) or ('MEDIUMTEXT' in target_data_type) or ('LONGTEXT' in target_data_type):
                        current_preproc.append(lambda x: pf.smth_to_string(x))
                    elif ("Float" in target_data_type) or ("Decimal" in target_data_type) or ("DECIMAL" in target_data_type) or ((',' in target_data_type ) and ("NUMERIC" in target_data_type)):
                        current_preproc.append(lambda x: pf.smth_to_float(x))
                    elif ("Int" in target_data_type) or ("INTEGER" in target_data_type) or ("BIGINT" in target_data_type) or ("TINYINT" in target_data_type) or ("SMALLINT" in target_data_type) or ("MEDIUMINT" in target_data_type) or ("NUMERIC" in target_data_type) :
                        current_preproc.append(lambda x: pf.smth_to_int(x))
                    elif ("DateTime" in target_data_type) or ("TIMESTAMP" in target_data_type) or ("DATE" in target_data_type) or ("TIME" in target_data_type) or ("YEAR" in target_data_type):
                        current_preproc.append(lambda x: pf.string_to_datetime(x, database_type))
                if current_preproc:
                    preprocessing_config[table_type][column] = current_preproc

with open('cf/columns_and_types.yaml', 'w') as file:
    yaml.dump(columns_dump, file)
    
if has_errors:
    print(*list_of_errors, sep='\n')
    print('Please, check your config.yaml file and database. After that, restart the container.')

# print(preprocessing_config)

basic_params = {}
if config['filter_date']['lower']:
    basic_params['filter[>=DATE_CREATE]'] = config['filter_date']['lower']
if config['filter_date']['upper']:
    basic_params['filter[<=DATE_CREATE]'] = config['filter_date']['upper']
URL = config['b24_key']
result = {}

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
for table_type in preprocessing_config.keys():
    print(f'{table_type.capitalize()} import started')
    method = f'crm.{table_type}.list.json'
    params = basic_params.copy()
    for num, param in enumerate(config[table_settings_mapper[table_type]]):
        params[f'select[{num}]'] = param
    # print(params)
    r = requests.get(URL + method, params=params).json()
    if 'result' in r.keys():
        result[table_type] = r['result'].copy()
        k = 1
        total = r['total']
        progress_bar = tqdm(total=total, position=0, leave=True)
        time.sleep(0.5)
        while 'next' in r.keys():
            k += 1
            params['start'] = r['next']
            r = requests.get(URL + method, params=params).json()
            result[table_type] += r['result']
            progress_bar.update(50)
    progress_bar.close()


def print_dict(dictionary):
    for key, value in dictionary.items():
        print(f'{key}: {value}', sep='\n')

# print_dict(preprocessing_config)

preprocessing_done

for table_type in result.keys():

    # TODO У нас очень должго идеёт импорт данных в БД, из за проверки каждого идентификатора на присутствие в БД
    # TODO Нужно сделать проверку оптимальной

    if not preprocessing_done.get(table_type, False):
        print(f'Preprocessing {table_type} data')
        for row in result[table_type]:
            for column in preprocessing_config[table_type].keys():
                for func in preprocessing_config[table_type][column]:
                    row[column] = func(row[column])

    preprocessing_done[table_type] = True
    print(f'Loading {table_type} data to Database')
    handler.load_data_to_sql(result[table_type],
                            config['table_names'][table_type], 
                            config[f'{table_type}_fields'], 
                            **config['db'],
                            relaxing = True)
    



