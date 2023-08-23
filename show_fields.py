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
        # userfields = {element['FIELD_NAME']: for element in requests.get(URL+f'crm.{table_type}.userfield.list').json()['result']}
        for field_name in result.keys():
            if 'UF_' in field_name:
                title = result[field_name]['formLabel']
            else:
                title = result[field_name]['title']
            print(f'Field name:{field_name}, field title: {title}')
        print("=========================")

if __name__ == "__main__":
    show_fields()