from datetime import datetime, timezone

# MARK: LAMBDAS
def smth_to_string(value):
    if value == None:
        return None
    elif value == "$$NULL$$" or not value:
        return ''
    else:
        return value
    
def void_to_nonnull_int(value):
    if value == "$$NULL$$" or not value:
        return 0
    else:
        return int(value)

def void_to_nonnull_float(value):
    if value == "$$NULL$$" or not value:
        return 0.0
    else:
        return float(value)
    
def void_to_nonnull_datetime(value, database_type):
    if value == "$$NULL$$" or not value:
        if database_type ==  "MySQL":
            return '0000-00-00 00:00:00'
        return 0
    else:
        
        dt = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S%z')
        if database_type ==  "MySQL":
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        return int(dt.replace(tzinfo=timezone.utc).timestamp())

def smth_to_int(value):
    if value == None:
        return None
    elif value == "$$NULL$$" or not value:
        return 0
    else:
        return int(value)
    
def void_to_null(value):
    if not value:
        return None
    else:
        return value
    
def smth_to_float(value):
    if value == None:
        return None
    elif value == "$$NULL$$" or not value:
        return 0.0
    else:
        return float(value)
    
def string_to_datetime(value, database_type):
    if value == None:
        return None
    elif value == "$$NULL$$" or not value:
        return void_to_nonnull_datetime(value, database_type)
    else:
        dt = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S%z')
        if database_type ==  "MySQL":
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        return int(dt.replace(tzinfo=timezone.utc).timestamp())

def safe_from_null(value):
    if value == None:
        return "$$NULL$$"
    else:
        return value