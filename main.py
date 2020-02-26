import base64
import smartsheet
import mysql.connector
from mysql.connector import Error
import json
import os
from google.cloud import secretmanager

with open('config.json') as f:
    data = json.load(f)
    target_sheetid = data['sheetid']
    query = data['query']
    key_column_name = data['key_column_name']
    update_column_names = data['update_column_names']
# target_sheetid = "xxxx 1697155573409668"

print("target_sheetid: " , target_sheetid)

def get_secret(secret_name):
    client = secretmanager.SecretManagerServiceClient()
    project_id = os.getenv('GCP_PROJECT')
    name = client.secret_version_path(project_id, secret_name, '1')
    res = client.access_secret_version(name)
    secret_string = res.payload.data.decode('utf-8')
    return secret_string

access_token = get_secret('smartsheet-access-token')
smartsheet_client = smartsheet.Smartsheet(access_token)

sql_pw = get_secret('cloud-sql-pw')

def hello_pubsub(event, context):
    sheetid = base64.b64decode(event['data']).decode('utf-8')
    if sheetid == target_sheetid:
        update_sheet(sheetid)

def updateRow(rowId, key, update_column_ids):
    # update_query = query + key
    try:
        # connect to mysql
        connection = mysql.connector.connect(
            host='10.93.144.7 ',
            database='support_center',
            user='root',
            password= sql_pw
        )
        print("connection.is_connected() :", connection.is_connected())
        cursor = connection.cursor()
        try:
            # run query
            # cursor.execute(update_query)
            cursor.execute(query, key)
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

def update_sheet(sheetid):
    smart_sheet = smartsheet_client.Sheets.get_sheet(sheetid)
    data = json.loads(str(smart_sheet))

    # set column_dic for target columns
    columns = data["columns"]
    column_dic={}
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
    for i in reversed(range(totalRow)):
        rowId = rows[i]['id']
        cells = rows[i]['cells']
        for cell in cells:
            if cell['columnId'] == key_column_id:
                if "displayValue" in cell:
                    key = cell["displayValue"]
                    # key = str(int(cell["value"])).zfill(6)
                    print("key = ", key)
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
