import requests
import yaml

def show_fields():
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    URL = config['b24_key']
    for table_type in config['table_names'].keys():
        requestURL = URL + f'crm.{table_type}.fields'
        r = requests.get(requestURL)
        print(table_type.capitalize() + " fields:")
        print("=========================")
        result = r.json()['result']
        for field_name in result.keys():
            title = result[field_name]['title']
            print(f'Field name:{field_name}, field title: {title}')
        print("=========================")
show_fields()

if __name__ == "__main__":
    show_values()