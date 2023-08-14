import os
import yaml
import base64
import json
import urllib
import ipaddress
import re
import requests
from urllib.parse import urlparse, parse_qsl, unquote
import db_handler as db
import bitrix24 as bx24

import preprocessing_functions as pf

def handler(event, context):

    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    with open('columns_and_types.yaml', 'r') as file:
        columns_dump = yaml.safe_load(file)

    ip = event.get('requestContext', {}).get('identity', {}).get('sourceIp')

    if event['isBase64Encoded']:
        request = event['body'].encode('utf-8')
        request = base64.b64decode(request).decode('utf-8')
    else:
        request = event['body']
    post = dict(parse_qsl(request))
    URL = config['b24_key']
    app_token = os.environ['app_token']
    if post['auth[application_token]'] == app_token:
        print(post)
        if 'DEAL' in post['event']:
            data_type = 'deal'
        elif 'CONTACT' in post['event']:
            data_type = 'contact'
        elif 'COMPANY' in post['event']:
            data_type = 'company'
        elif 'LEAD' in post['event']:
            data_type = 'lead'
        else:
            data_type = None
            return {
            'statusCode': 404,
            'headers': {
                        'Content-Type': 'text/plain;charset=utf-8',
                    },
            'body': 'Unknown Data Type!',
            }   
    database_type = config['database_type']
    data = bx24.get_data(post['data[FIELDS][ID]'], data_type, URL)
    data_types = {}
    transaction = {}
    if f'{data_type}_fields' in config.keys():
        for field in columns_dump[data_type]:
            data_types[field['name']] = {'nullable': field['nullable'], 'type': field['type']}
        for field in config[f'{data_type}_fields']:
            if field in data.keys():
                transaction[field] = data[field]
                nullable = data_types[config[f'{data_type}_fields'][field]]['nullable']
                target_data_type = data_types[config[f'{data_type}_fields'][field]]['type']
                current_preproc = []
                if field in config[f'{data_type}_functions'].keys():
                    function_name = config[f'{data_type}_functions'][field]
                    current_preproc = [getattr(pf, function_name)]
                else:
                    if nullable:
                        current_preproc.append(lambda x: pf.void_to_null(x))
                    else:
                        current_preproc.append(lambda x: pf.safe_from_null(x)) 
                    if ("String" in target_data_type) or ('VARCHAR' in target_data_type) or ('TEXT' in target_data_type) or ('CHAR' in target_data_type) or ('TINYTEXT' in target_data_type) or ('MEDIUMTEXT' in target_data_type) or ('LONGTEXT' in target_data_type):
                        current_preproc.append(lambda x: pf.smth_to_string(x))
                    elif ("Int" in target_data_type) or ("INTEGER" in target_data_type) or ("BIGINT" in target_data_type) or ("TINYINT" in target_data_type) or ("SMALLINT" in target_data_type) or ("MEDIUMINT" in target_data_type) :
                        current_preproc.append(lambda x: pf.smth_to_int(x))
                    elif ("Float" in target_data_type) or ("Decimal" in target_data_type) or ("DECIMAL" in target_data_type):
                        current_preproc.append(lambda x: pf.smth_to_float(x))
                    elif ("DateTime" in target_data_type) or ("TIMESTAMP" in target_data_type) or ("DATE" in target_data_type) or ("TIME" in target_data_type) or ("YEAR" in target_data_type):
                        current_preproc.append(lambda x: pf.string_to_datetime(x, database_type))

                for func in current_preproc:
                    transaction[field] = func(transaction[field])
    
    db.load_data_to_sql([transaction], config['table_names'][data_type], config[f'{data_type}_fields'], **config['db'])

    return {
        'statusCode': 200,
        'body': 'Hello World!',
    }


    