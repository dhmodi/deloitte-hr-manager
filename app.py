#!/usr/bin/env python
#test comment
from __future__ import print_function
from future.standard_library import install_aliases
import cx_Oracle
install_aliases()
import json
import os
import sys
import pandas as pd
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

@app.route('/')
def index():
    return redirect(url_for('static', filename='index.html'))


# @app.route('/speech')
# def speech():
#     return redirect(url_for('static', filename='index.html'))

# @app.route('/inventory')
# def inventory():
#     return redirect(url_for('static_url', filename='index.html'))


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
    print(req)
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
        print(outText)
        with open("response/alexa_response.json", 'r') as f:
            alexaResponse = json.load(f)

        alexaResponse["response"]["outputSpeech"]["text"] = outText
        return alexaResponse

    elif (req.get("request").get("intent").get("name") == "InventoryVisualization"):
        print("Inventory Visualization")
        chartType = "line"
        incoming_query = req.get("request").get("intent").get("slots").get("message").get("value")
        print(incoming_query)
        chartType = req.get("request").get("intent").get("slots").get("charttypeslot").get("value")
        if (chartType == "bar"):
            chartType = "column2d"
        elif (chartType is None):
            chartType = "line"
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

    # chartType = req.get("result").get("parameters").get("chart-type")
        # print(chartType)

        print(queryString)
        cur = conn.cursor()
        cur.execute(queryString)
        rows = cur.fetchall()
        print(rows)
        print(list(columns))
        xAxis = columns[0][0].split('.')[1]
        yAxis = columns[1][0].split('.')[1]
        xAxis = OutMap.get(xAxis) if OutMap.get(xAxis) else xAxis
        yAxis = OutMap.get(yAxis) if OutMap.get(yAxis) else yAxis
        print(xAxis)
        print(yAxis)
        print(chartType)
        df = pd.DataFrame(list(rows), columns = ["label", "value"])
        df['value'] = df['value'].fillna(0)
        agg_df = df.groupby(['label'], as_index=False).agg({"value": "sum"})
        maxRecord = agg_df.ix[agg_df['value'].idxmax()].to_frame().T
        agg_df = agg_df.reset_index()
        minRecord = agg_df.ix[agg_df['value'].idxmin()].to_frame().T
        agg_df['label'] = agg_df['label'].astype('str')
        agg_df['value'] = agg_df['value'].astype('str')
        chartData = agg_df.to_json(orient='records')
        # chartData = [{"label": str(row[0]), "value": str(row[1])} for row in rows]
        print (chartData)
        # chartData = json.dumps(chartData)
        # final_json = '[ { "type":"' + chartType + '", "chartcontainer":"barchart", "caption":"' + chartType + ' chart showing ' + xAxis + ' vs ' + yAxis + '", "subCaption":"", "xAxisName":"xAxis", "yAxisName":"yAxis","source":[ { "label": "Mon", "value": "15123" }, { "label": "Tue", "value": "14233" }, { "label": "Wed", "value": "23507" }, { "label": "Thu", "value": "9110" }, { "label": "Fri", "value": "15529" }, { "label": "Sat", "value": "20803" }, { "label": "Sun", "value": "19202" } ]}]'
        final_json = '[ { "type":"' + chartType + '", "chartcontainer":"barchart", "caption":"A ' + chartType + ' chart showing ' + xAxis + ' vs ' + yAxis + '", "subCaption":"", "xAxisName":"' + xAxis + '", "yAxisName":"' + yAxis + '", "source":' + chartData + '}]'
        print(final_json)

        socketio.emit('chartdata', final_json)
        outText = "The " + xAxis + " " + str(maxRecord['label'].values[0]) + " has maximum " + yAxis + " while the " + xAxis + " " + str(minRecord['label'].values[0]) + " has minimum " + yAxis + ". Refer to the screen for more details."
        # outText = "Refer to the screen for more details."
        print(outText)
        with open("response/alexa_response.json", 'r') as f:
            alexaResponse = json.load(f)

        alexaResponse["response"]["outputSpeech"]["text"] = outText
        return alexaResponse

    elif (req.get("result").get("action") == "show.visualization"):
        print("Inventory Visualization")
        incoming_query = req.get("result").get("resolvedQuery")
        print(incoming_query)
        chartType = req.get("result").get("parameters").get("chart-type")
        print(chartType)
        queries = parser.parse_sentence(incoming_query.lower())
        print(query for query in queries)
        queryString = ""
        table = ""
        for query in queries:
            table = query.get_from().get_table()
            columns = query.get_select().get_columns()
            queryString = queryString + str(query)
        print(queryString)
        cur = conn.cursor()
        cur.execute(queryString)
        rows = cur.fetchall()
        print(rows)
        print(list(columns))

        xAxis = columns[0][0].split('.')[1]
        yAxis = columns[1][0].split('.')[1]
        print(xAxis)
        df = pd.DataFrame(list(rows), columns = ["label", "value"])
        agg_df = df.groupby(['label'], as_index=False).agg({"value": "sum"})
        agg_df['label'] = agg_df['label'].astype('str')
        agg_df['value'] = agg_df['value'].astype('str')
        chartData = agg_df.to_json(orient='records')
        # chartData = [{"label": str(row[0]), "value": str(row[1])} for row in rows]
        print (chartData)
        # chartData = json.dumps(chartData)
        # final_json = '[ { "type":"' + chartType + '", "chartcontainer":"barchart", "caption":"' + chartType + ' chart showing ' + xAxis + ' vs ' + yAxis + '", "subCaption":"", "xAxisName":"xAxis", "yAxisName":"yAxis","source":[ { "label": "Mon", "value": "15123" }, { "label": "Tue", "value": "14233" }, { "label": "Wed", "value": "23507" }, { "label": "Thu", "value": "9110" }, { "label": "Fri", "value": "15529" }, { "label": "Sat", "value": "20803" }, { "label": "Sun", "value": "19202" } ]}]'
        final_json = '[ { "type":"' + chartType + '", "chartcontainer":"barchart", "caption":"A ' + chartType + ' chart showing ' + xAxis + ' vs ' + yAxis + '", "subCaption":"", "xAxisName":"' + xAxis + '", "yAxisName":"' + yAxis + '", "source":' + chartData + '}]'
        print(final_json)
        maxRecord = agg_df.ix[agg_df['value'].idxmax()].to_frame().T
        print(maxRecord)
        minRecord = agg_df.ix[agg_df['value'].idxmin()].to_frame().T
        print(minRecord)
        socketio.emit('chartdata', final_json)
        outText = "The " + xAxis + " " + str(maxRecord['label'].values[0]) + " has maximum " + yAxis + " while the " + xAxis + " " + str(minRecord['label'].values[0]) + " has minimum " + yAxis
        return {
            "speech": outText,
            "displayText": outText,
            # "data": data,
            # "contextOut": [],
            "source": "Dhaval"
        }
    elif (req.get("result").get("action") == "inventory.search"):
        print("Inventory Search")
        incoming_query = req.get("result").get("resolvedQuery")
        queries = parser.parse_sentence(incoming_query.lower())
        #print(query for query in queries)
        queryString = ""
        table = ""
        for query in queries:
            table = query.get_from().get_table()
            columns = query.get_select().get_columns()
            queryString = queryString + str(query)

        print(table)
        print(list(columns))
        # xAxis = columns[0][0].split('.')[1]
        # yAxis = columns[1][0].split('.')[1]
        print(queryString)
        cur = conn.cursor()
        cur.execute(queryString)
        rows = cur.fetchall()

        # outText = ', '.join(str(x) for x in rows[0])
        # outText = ', '.join(str(element).split(".")[0] for row in rows for element in row)
        count = 0

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
                    outText = outText + column + " is " + value
                elif (operation is "COUNT"):
                    print("The Operation is " + str(operation))
                    outText = outText + operation + " of " + table + " is " + value
                else:
                    print("The Operation is " + str(operation))
                    outText = outText + operation + " of " + column + " is " + value
                if (isLast is not 0):
                    outText = outText + " and the "
                    count = count + 1
        # print(','.join(str(element) for row in rows for element in row))

        return {
            "speech": outText,
            "displayText": outText,
            # "data": data,
            # "contextOut": [],
            "source": "Dhaval"
        }
if __name__ == '__main__':
    database = Database.Database()
    database.load("cognitiveSQL/database/HCM.sql")
    # database.print_me()

    config = LangConfig.LangConfig()
    config.load("cognitiveSQL/lang/english.csv")

    parser = Parser.Parser(database, config)
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    port = int(os.getenv('PORT', 5001))

    print("Starting app on port %d" % port)

    #app.run(debug=True, port=port, host='0.0.0.0')
    socketio.run(app, debug=True, port=port, host='0.0.0.0')