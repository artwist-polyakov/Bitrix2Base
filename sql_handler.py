from sqlalchemy import create_engine, inspect

def get_columns_and_types_sql(table, host, port, user, password, db_name):
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}") 
    inspector = inspect(engine)
    columns = inspector.get_columns(table)
    return columns

def test_connection(host, port, user, password, db_name):
    try:
        # Создаем движок SQLAlchemy
        engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}") 
        # Выполняем простой SQL-запрос
        result = engine.execute("SELECT 1")
        # Если запрос выполнен успешно, выводим сообщение об успешном подключении
        print("Successfully connected to the database.")
    except Exception as e:
        # Если при выполнении запроса возникла ошибка, выводим сообщение об ошибке
        print("Failed to connect to the database.")
        print(str(e))