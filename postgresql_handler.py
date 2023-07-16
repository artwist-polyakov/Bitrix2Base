from sqlalchemy import create_engine, inspect

def get_columns_and_types_postgresql(table, host, port, user, password, db_name):
    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}") 
    inspector = inspect(engine)
    columns = inspector.get_columns(table)
    return columns

def test_connection(host, port, user, password, db_name):
    try:
        engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}") 
        result = engine.execute("SELECT 1")
        print("Successfully connected to the database.")
    except Exception as e:
        print("Failed to connect to the database.")
        print(str(e))
