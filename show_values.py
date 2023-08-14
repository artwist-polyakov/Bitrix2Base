import json
import time
import requests
import yaml

def show_values():

    table_settings_mapper = {
    'deal': 'deal_fields',
    'contact': 'contact_fields',
    'company': 'company_fields',
    'lead': 'lead_fields',
}
    # Загрузка конфигурации и других необходимых данных
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    
    basic_params = {}
    if config['filter_date']['lower']:
        basic_params['filter[>=DATE_CREATE]'] = config['filter_date']['lower']
    if config['filter_date']['upper']:
        basic_params['filter[<=DATE_CREATE]'] = config['filter_date']['upper']
    URL = config['b24_key']
    result = {}
    
    for table_type in config['table_names'].keys():
        if config['table_names'][table_type]:
            print(f'{table_type.capitalize()} SOME 20 IMPORTED VALUES')
            method = f'crm.{table_type}.list.json'
            params = basic_params.copy()
            for num, param in enumerate(config[table_settings_mapper[table_type]]):
                params[f'select[{num}]'] = param
                
            r = requests.get(URL + method, params=params).json()
            if 'result' in r.keys():
                result[table_type] = r['result'].copy()
                if len(result[table_type]) > 20:
                    k = 20
                else:
                    k = len(result[table_type])
                for row in result[table_type][k:]:
                    print(json.dumps(row, indent=4, ensure_ascii=False))
            else:
                print(f"No result for {table_type}")

if __name__ == "__main__":
    show_values()