import json

with open(r'maps.json','r',encoding="utf-8") as f:
    maps = json.load(f)

class Sheet:
    def __init__(self, id, key, update_column_names):
        self.id = id
        self.key = key
        self.update_column_names = update_column_names
class DB:
    def __init__(self, server, user, name):
        self.server = server
        self.user = user
        self.name = name
class Table:
    def __init__(self, name, key, lookup_columns):
        self.name = name
        self.key = key
        self.lookup_columns = lookup_columns

for map in maps:
    sheet = map['sheet']
    db = map['db']
    table = map['table']
    sheet_object = Sheet(sheet['id'], sheet['key'], sheet['update_column_names'])
    db_object = DB(db['server'],db['user'],db['name'])
    table_object = Table(table['name'],table['key'],table['lookup_columns'])
    print(sheet_object.id, sheet_object.key, sheet_object.update_column_names)
    print(db_object.server, db_object.user, db_object.name)
    print(table_object.name, table_object.key, table_object.lookup_columns)
