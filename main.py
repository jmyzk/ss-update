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

print("target_sheetid: " , target_sheetid)
print("query: ", query)

def get_secret(secret_name):
    client = secretmanager.SecretManagerServiceClient()
    project_id = os.getenv('GCP_PROJECT')
    name = client.secret_version_path(project_id, secret_name, '1')
    res = client.access_secret_version(name)
    secret_string = res.payload.data.decode('utf-8')
    return secret_string

access_token = get_secret('smartsheet-access-token')
sql_pw = get_secret('cloud-sql-pw')
print(sql_pw)
smartsheet_client = smartsheet.Smartsheet(access_token)
# target_sheetid = os.environ.get('target_sheetid', 'Specified environment variable is not set.')
# target_sheetid = "xxxx 1697155573409668"
def hello_pubsub(event, context):
    sheetid = base64.b64decode(event['data']).decode('utf-8')
    if sheetid == target_sheetid:
        update_sheet(sheetid)

def updateRow(rowId, postcode, columnIds):
#    sql = "select postName, postType, BC, email, tel, address from postcodeMaster where postcode = " + postcode
    update_query = query + postocde
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
            cursor.execute(update_query)
            results = cursor.fetchone()
            print(results)
            new_row = smartsheet.models.Row()
            new_row.id = rowId
            columnIdNumber = 1
            for result in results:
                new_cell = smartsheet.models.Cell()
                new_cell.column_id = columnIds[columnIdNumber]
                new_cell.value = result
                new_cell.strict = False
                new_row.cells.append(new_cell)
                columnIdNumber  += 1
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
    # get columnId for target columns
    columns = data["columns"]
    column_dic={}
    for column in columns:
        id = column["id"]
        title = column["title"]
        column_dic.update({title: id})
    columnIds = [
        column_dic['局所コード'],
        column_dic['正局所名'],
        column_dic['正局種'],
        column_dic['物流センター'],
        column_dic['局email'],
        column_dic['局電話'],
        column_dic['局住所']
    ]

    postcodeColumnId = columnIds[0]
    truePostNameColumnId = columnIds[1]
    rows = data["rows"]
    totalRow = data["totalRowCount"]
    rowsToUpdate = []
    # check rows backwards
    for i in reversed(range(totalRow)):
        rowId = rows[i]['id']
        cells = rows[i]['cells']
        for cell in cells:
            if cell['columnId'] == postcodeColumnId:
                if "value" in cell:
                    postcode = str(int(cell["value"])).zfill(6)
                    # postcode = int(cell["value"])
                    print("postcode = ", postcode)
                else:
                    postcode = "no postcode value"
            if cell['columnId'] == truePostNameColumnId:
                if "value" in cell:
                    truePostNameExists = True
                else:
                    truePostNameExists = False
        if postcode != "no postcode value" and not truePostNameExists:
            print("update postcode : " , postcode)
            rowToUpdate = updateRow(rowId, postcode, columnIds)
            if not rowToUpdate == "no data":
                rowsToUpdate.append(rowToUpdate)
    smartsheet_client.Sheets.update_rows(sheetid, rowsToUpdate)
    print('done')
