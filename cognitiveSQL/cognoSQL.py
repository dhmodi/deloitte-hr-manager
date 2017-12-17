#!/usr/bin/python
# -*- coding: utf-8 -*

import os, sys, getopt
import unicodedata
import importlib
from cognitiveSQL.HashMap import hashMap_columns
from cognitiveSQL.Database import Database
from cognitiveSQL.LangConfig import LangConfig
from cognitiveSQL.Parser import Parser
from cognitiveSQL.Thesaurus import Thesaurus
from cognitiveSQL.StopwordFilter import StopwordFilter
import cx_Oracle

#importlib.reload(sys)
#sys.setdefaultencoding("utf-8")

class color:
    PURPLE = '\033[95m'
    CYAN = '\033[96m'
    DARKCYAN = '\033[36m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'

class ln2sql:
    def __init__(self, database_path, input_sentence, language_path, thesaurus_path, json_output_path):
        database = Database()
        database.load(database_path)
        #database.print_me()

        config = LangConfig()
        config.load(language_path)

        parser = Parser(database, config)

        if thesaurus_path is not None:
            thesaurus = Thesaurus()
            thesaurus.load(thesaurus_path)
            parser.set_thesaurus(thesaurus)

        queries = parser.parse_sentence(input_sentence.lower())

        if json_output_path is not None:
            self.remove_json(json_output_path)
            for query in queries:
                query.print_json(json_output_path)
        queryString = ""
        for query in queries:
            queryString = queryString + str(query)

        print(queryString)

        currDir = os.getcwd()
        print(currDir)
        os.environ["ORACLE_HOME"] = os.path.join(currDir,"..\instantclient_12_2")
        pathString = os.environ.get("PATH")
        os.environ["PATH"] = os.path.join(currDir,"..\instantclient_12_2") + ";" + pathString
        dsnStr = cx_Oracle.makedsn("129.158.70.122", "1521", "ORCL")

        conn = cx_Oracle.connect(user="C##DAOHCM", password="C##DAOHCM", dsn=dsnStr)

        print ("Using Database version:" + conn.version)

        cursor = conn.cursor()
        cursor.execute(queryString)
        for count in cursor:
            print("Values:", count)

    def remove_json(self, filename="output.json"):
        if os.path.exists(filename):
            os.remove(filename)

def print_help_message():
    print ('\n')
    print ('Usage:')
    print ('\tpython cognoSQL.py -i <input-sentence>')
    print ('Parameters:')
    print ('\t-h\t\t\tprint this help message')
    print ('\t-i <input-sentence>\tinput sentence to parse')
    print ('\n')

def main(argv):
    try:
        opts, args = getopt.getopt(argv,"d:l:i:t:j:")
        database_path = None
        input_sentence = None
        language_path = None
        thesaurus_path = None
        json_output_path = None

        for i in range(0, len(opts)):
            if opts[i][0] == "-i":
                input_sentence = opts[i][1]
            else:
                print_help_message()
                sys.exit()

        database_path = "database\HCM.sql"
        language_path = "lang\english.csv"
        #thesaurus_path = "thesaurus\\th_english.dat"
        #json_output_path = "output"

        if (database_path is None) or (input_sentence is None) or (language_path is None):
            print_help_message()
            sys.exit()
        else:
            if thesaurus_path is not None:
                thesaurus_path = str(thesaurus_path)
            if json_output_path is not None:
                json_output_path = str(json_output_path)

        #try:
        ln2sql(str(database_path), str(input_sentence), str(language_path), thesaurus_path, json_output_path)
        #except Exception, e:
        #    print color.BOLD + color.RED + str(e) + color.END

    except getopt.GetoptError:
        print_help_message()
        sys.exit()

if __name__ == '__main__':
    main(sys.argv[1:])