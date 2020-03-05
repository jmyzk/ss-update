import base64
import smartsheet
import mysql.connector
from mysql.connector import Error
import json
import os
from google.cloud import secretmanager


with open(r'maps.json','r',encoding="utf-8") as f:
    maps = json.load(f)

class Sheet:
    def __init__(self, id, key, column_names, column_ids):
        self.id = id
        self.key = key
        self.column_names = column_names
        self.column_ids = column_ids
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

# get secert from SecretManager
def get_secret(secret_name):
    client = secretmanager.SecretManagerServiceClient()
    project_id = os.getenv('GCP_PROJECT')
    name = client.secret_version_path(project_id, secret_name, '1')
    res = client.access_secret_version(name)
    secret_string = res.payload.data.decode('utf-8')
    return secret_string

access_token = get_secret('smartsheet-access-token')
sql_pw = get_secret('cloud-sql-pw')
smartsheet_client = smartsheet.Smartsheet(access_token)

def hello_pubsub(event, context):
    sheetid = base64.b64decode(event['data']).decode('utf-8')

    for map in maps:
        sheet = map['sheet']
        db = map['db']
        table = map['table']
        sheet_object = Sheet(sheet['id'], sheet['key'], sheet['column_names'],[])
        db_object = DB(db['server'],db['user'],db['name'])
        table_object = Table(table['name'],table['key'],table['columns'])

        if sheetid == sheet.id:
            update_sheet(sheet_object, db_object, table_object)

def updateRow(rowId, key, column_ids):
    try:
        # connect to mysql
        connection = mysql.connector.connect(
            host= dbServer,
            database= dbName,
            user= dbUser,
            password= sql_pw
        )
        print("connection.is_connected() :", connection.is_connected())
        cursor = connection.cursor()
        try:
            # run query
            # cursor.execute(query,(tuple,,))
            cursor.execute(query, (key,))
            results = cursor.fetchone()
            print(results)
            new_row = smartsheet.models.Row()
            new_row.id = rowId
            update_column_id_number = 0
            for result in results:
                new_cell = smartsheet.models.Cell()
                new_cell.column_id = update_column_ids[update_column_id_number]
                new_cell.value = result
                new_cell.strict = False
                new_row.cells.append(new_cell)
                update_column_id_number  += 1
            return new_row
        except:
            # no results
            return "no data"

    except Error as e :
        # connection error
         print ("Error while connecting to MySQL", e)
    finally:
        # close database connection
        if(connection.is_connected()):
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

def update_sheet(sheet_object, db_object, table_object):
    smart_sheet = smartsheet_client.Sheets.get_sheet(sheet_object.id)
    data = json.loads(str(smart_sheet))

    # set column_dic for target columns
    columns = data["columns"]
    for column in columns:
        id = column["id"]
        title = column["title"]
        column_dic.update({title: id})

    # get columnId for key column in smartsheet
    key_column_id = column_dic[key_column_name]

    # get column ids for target update columns
    update_column_ids = []
    for update_column_name in update_column_names:
        update_column_ids.append(column_dic[update_column_name])

    # get column id for first column in target columns to test if value exists
    first_update_column_id = update_column_ids[0]

    rows = data["rows"]
    totalRow = data["totalRowCount"]
    rowsToUpdate = []
    # check rows backwards
    # use the commented out code to limit the number of rows go backward
    # for i in reversed(range(totalRow-num_rows_backward,totalRow)):
    for i in reversed(range(totalRow)):
        rowId = rows[i]['id']
        cells = rows[i]['cells']
        for cell in cells:
            if cell['columnId'] == key_column_id:
                if "displayValue" in cell:
                    key = cell["displayValue"]
                    # key = str(int(cell["value"])).zfill(6)
                    print("key : ", key)
                else:
                    key = False
            if cell['columnId'] == first_update_column_id:
                if "value" in cell:
                    first_update_column_value_exists = True
                else:
                    first_update_column_value_exists = False
        if key and not first_update_column_value_exists:
            print("update key : " , key)
            rowToUpdate = updateRow(rowId, key, update_column_ids)
            if not rowToUpdate == "no data":
                rowsToUpdate.append(rowToUpdate)
    smartsheet_client.Sheets.update_rows(sheetid, rowsToUpdate)
    print('done')
