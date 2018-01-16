#!/usr/bin/env python

from __future__ import print_function
from future.standard_library import install_aliases
import cx_Oracle
install_aliases()
import json
import os
import sys
sys.path.append('cognitiveSQL')
from flask import Flask
from flask import request
from flask import make_response
from flask import url_for, redirect
from flask_socketio import SocketIO, send, emit
import subprocess

import cognitiveSQL.Database as Database
import cognitiveSQL.LangConfig as LangConfig
import cognitiveSQL.Parser as Parser
import cognitiveSQL.Thesaurus as Thesaurus
import cognitiveSQL.StopwordFilter as StopwordFilter
from cognitiveSQL.HashMap import hashMap_columns



# Flask app should start in global layout
app = Flask(__name__, static_url_path='')
socketio = SocketIO(app)

parser = ""

#### if WINDOWS
# import os
# currDir = os.getcwd()
# print(currDir)
# ORACLE_HOME = os.path.join(currDir,"lib")
# PATH = os.environ.get('PATH')
# os.environ['ORACLE_HOME'] = ORACLE_HOME
# os.environ['PATH'] = ORACLE_HOME + ";" + PATH

### if LINUX
os.environ['LD_LIBRARY_PATH'] = '/app/lib/'
subprocess.call(['sh','downloadLib.sh'])

dsnStr = cx_Oracle.makedsn("129.158.70.122", "1521", "ORCL")

conn = cx_Oracle.connect(user="C##DAOHCM", password="C##DAOHCM", dsn=dsnStr)
print ("Using Database version:" + conn.version)

@app.route('/speech')
def speech():
    return redirect(url_for('static', filename='index.html'))

@app.route('/webhook', methods=['POST'])
def webhook():
    req = request.get_json(silent=True, force=True)

    print("Request:")
    print(json.dumps(req, indent=4))

    res = processRequest(req)

    res = json.dumps(res, indent=4)
    print(res)
    r = make_response(res)
    r.headers['Content-Type'] = 'application/json;charset=UTF-8'
    return r


def processRequest(req):
    if (req.get("request").get("intent").get("name") == "InventorySearch"):
        print("InventorySearch")
        incoming_query = req.get("request").get("intent").get("slots").get("message").get("value")
        print(incoming_query)
        hashColumn_csv = 'cognitiveSQL/alias/synonyms.csv'
        (input_sentence,OutMap) = hashMap_columns(str(incoming_query).lower(), hashColumn_csv)
        print(OutMap)
        print(input_sentence)
        queries = parser.parse_sentence(input_sentence)
        # queries = parser.parse_sentence(incoming_query)
        # print(query for query in queries)
        queryString = ""
        table = ""
        for query in queries:
            table = query.get_from().get_table()
            columns = query.get_select().get_columns()
            conditions = query.get_where().get_conditions()
            queryString = queryString + str(query)

        print(table)
        print(list(columns))

        # print(conditions[1][1].get_column_type())

        print(queryString)
        cur = conn.cursor()
        cur.execute(queryString)
        rows = cur.fetchall()
        count = 0
        if len(conditions) != 0:
            whereColumn=[]
            whereValue =[]
            for i in range(0, len(conditions)):
                print(conditions[i][1].get_column().rsplit('.',1)[1].rstrip(')'))
                print(conditions[i][1].get_value().strip("'"))
                whereColumn.append(conditions[i][1].get_column().rsplit('.',1)[1].rstrip(')'))

                if " MAX" not in conditions[i][1].get_value()and " MIN" not in conditions[i][1].get_value():
                    whereValue.append(conditions[i][1].get_value().strip("'"))
                else:
                 if " MAX" in conditions[i][1].get_value():
                     whereValue.append("max")
                 else:
                     whereValue.append("min")
        outText = "The "
        for row in rows:
            isLast = len(row)
            for element in row:
                isLast = isLast - 1
                value = str(element).split(".")[0]
                if (columns[count][0] is not None):
                    # print(columns)
                    column = columns[count][0].split('.')[1]
                operation = columns[count][1]
                if (operation is None):
                    print("The Operation is None")
                    column = OutMap.get(column)
                    whereValue1 = OutMap.get(whereValue[0]) if (OutMap.get(whereValue[0])) else whereValue[0]
                    whereColumn1 = OutMap.get(whereColumn[0]) if (OutMap.get(whereColumn[0])) else whereColumn[0]
                    print(whereValue[1])
                    print(whereColumn[1])
                    whereValue2 = OutMap.get(whereValue[1]) if (OutMap.get(whereValue[1])) else whereValue[1]
                    whereColumn2 = OutMap.get(whereColumn[1]) if (OutMap.get(whereColumn[1])) else whereColumn[1]
                    if 'whereColumn' in locals():
                        outText = str(column) + " " + value + " in the " + str(whereColumn1) + " " + str(whereValue1) + " has " + str(whereValue2) + " " + str(whereColumn2)
                    else:
                        outText = outText + str(column) + " is " + value
                elif (operation is "COUNT"):
                    table = OutMap.get(table)
                    print("The Operation is " + str(operation))
                    if 'whereColumn' in locals():
                        outText =  "There are " + value + " " + str(table) + " with " + str(whereValue[0]) + " " + str(whereColumn[0])
                    else:
                        outText =  "There are " + value + " " + str(table)
                else:
                    #operation = OutMap.get(str(operation).lower())
                    column = OutMap.get(column)
                    #whereValue = OutMap.get(whereValue)
                    print("The Operation is " + str(operation))
                    if 'whereColumn' in locals():
                        outText = "There are " + value + " " + str(column) + " with " + str(whereValue[0]) + " " + str(whereColumn[0])
                    else:
                        outText = "There are " + value + " " + str(column)
                if (isLast is not 0):
                    outText = outText + " and the "
                    count = count + 1
        #print(','.join(str(element) for row in rows for element in row))


        # return {
        #     "speech": type,
        #     "displayText": outText,
        #     # "data": data,
        #     # "contextOut": [],
        #     "source": "Dhaval"
        # }

        # alexaResponse.get("response").get("outputSpeech").get("text")=outText
        # alexaResponse.get("response").get("reprompt").get("outputSpeech").get("text")=outText
        print(outText)
        with open("response/alexa_response.json", 'r') as f:
            alexaResponse = json.load(f)

        alexaResponse["response"]["outputSpeech"]["text"] = outText
        return alexaResponse

if __name__ == '__main__':
    database = Database.Database()
    database.load("cognitiveSQL/database/HCM.sql")
    # database.print_me()

    config = LangConfig.LangConfig()
    config.load("cognitiveSQL/lang/english.csv")

    parser = Parser.Parser(database, config)
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    port = int(os.getenv('PORT', 5000))

    print("Starting app on port %d" % port)

    #app.run(debug=True, port=port, host='0.0.0.0')
    socketio.run(app, debug=True, port=port, host='0.0.0.0')