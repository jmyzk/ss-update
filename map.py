import json
import smartsheet

access_token = "luc6e8gxpjz2xxipkv5900ta68" #get_secret('smartsheet-access-token')
smartsheet_client = smartsheet.Smartsheet(access_token)


with open(r'maps.json','r',encoding="utf-8") as f:
    maps = json.load(f)

class Sheet:
    def __init__(self, id, key, column_names, column_dic):
        self.id = id
        self.key = key
        self.column_names = column_names
        self.column_dic = column_dic

class DB:
    def __init__(self, server, user, name):
        self.server = server
        self.user = user
        self.name = name
class Table:
    def __init__(self, name, key, columns):
        self.name = name
        self.key = key
        self.columns = columns

def update_sheet(sheet_object, db_object, table_object):
    smart_sheet = smartsheet_client.Sheets.get_sheet(sheet_object.id)
    data = json.loads(str(smart_sheet))

    print("sheet_object.column_names : ", sheet_object.column_names)

    # set column_dic for target columns
    sheet_name = data["name"]
    columns = data["columns"]
    print(sheet_name)

    for column in columns:
        sheet_object.column_dic[column["title"]] = column["id"]

    for column_name in sheet_object.column_names:
        print(column_name, " : ", sheet_object.column_dic[column_name])


for map in maps:
    sheet = map['sheet']
    db = map['db']
    table = map['table']
    sheet_object = Sheet(sheet['id'], sheet['key'], sheet['column_names'],{})
    db_object = DB(db['server'],db['user'],db['name'])
    table_object = Table(table['name'],table['key'],table['columns'])

    update_sheet(sheet_object, db_object, table_object)
