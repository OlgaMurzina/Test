HOST = "localhost",
DATABASE = "db",
USER = "user_name",
PASSWORD = "password"

def connection_string():
    return f'host={ HOST} dbname={DATABASE} user={USER} password={PASSWORD}'