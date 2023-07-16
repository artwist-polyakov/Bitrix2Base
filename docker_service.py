import docker
def close():
    client = docker.from_env()
    container = client.containers.get('bitrix24_loader')
    container.stop()