def get_data(id, type, url):
    method = f'crm.{type}.get'
    params = {'id': id}
    response = requests.get(url+method, params=params)
    return response.json()['result']