#!/usr/bin/env python
########################################################################################################
# Name:  SQLSnowFlakeFncts.py
#
# Desc: Common module with Snowflake DB functions.
#        NOTE: Import module into Python program to use functions.
#        NOTE: Used by DSH_AddReqEmails.py which is called by DSH_Extracts.sh.
#
# Created: Paul Baranoski 03/18/2025
# Modified:
#
# Paul Baranoski 2025-03-18 Create Module.
# Paul Baranoski 2025-05-08 Added Insert into table function.
########################################################################################################

import os
import csv
import logging

import sys
import datetime
from datetime import datetime

from snowconvert_helpers import SnowflakeHelper


import snowflake.connector
import json

class NullConnectException(Exception):
    "Connection object is null"

class NullCursorException(Exception):
    "Cursor object is null"

class ConfigFileNotfnd(Exception):
    "Configuration file not found: "
    
########################################################################################################
# Get directories and build path.
########################################################################################################
currentDirectory = os.path.dirname(os.path.realpath(__file__))
rootDirectory = os.path.abspath(os.path.join(currentDirectory, ".."))
utilDirectory = os.getenv('CMN_UTIL')
SF_XTR_WAREHOUSE = os.getenv('sf_xtr_warehouse')
print(f"{SF_XTR_WAREHOUSE=}")

sys.path.append(rootDirectory)
sys.path.append(utilDirectory)
script_name = os.path.basename(__file__)


To create a cursor using the snowconvert_helpers Python package, you typically need to establish a connection to your Snowflake database first. The snowconvert_helpers package is designed to simplify interactions with Snowflake, and creating a cursor is a common step for executing SQL queries.

Here’s an example of how you can create a cursor using the snowconvert_helpers package:

Copy the code
from snowconvert_helpers import SnowflakeHelper


# Initialize the SnowflakeHelper with connection parameters
helper = SnowflakeHelper(
    user='your_username',
    password='your_password',
    account='your_account',
    database='your_database',
    schema='your_schema',
    warehouse='your_warehouse',
    role='your_role'
)

# Establish a connection
connection = helper.get_connection()

# Create a cursor
cursor = connection.cursor()

# Example: Execute a query
cursor.execute("SELECT CURRENT_DATE;")
result = cursor.fetchall()
print(result)

# Close the cursor and connection
cursor.close()
connection.close()

    
###############################
# Functions
###############################
def closeConnection(cnx):

    print("start function closeConnection()")

    if cnx is not None:
        cnx.close()


def getSFCredentials():

    print("start function getSFCredentials()")
    
    # Get location of SF credentials file
    logonDirectory=os.path.dirname(r"/app/IDRC/XTR/CMS/scripts/logon/")
    SFCredentialsFile=os.path.join(logonDirectory, "sf.logon")

    try:     
        
        with open(SFCredentialsFile, "r") as sfCredFile:
            sfCredDict = json.load(sfCredFile)
            #print(f"{sfCredDict=}")
            print("Sucessfully read SF logon file")
            
        # return dictionary with SF credentials and connection information 
        print(f"{sfCredDict['SNOW_WAREHOUSE']=}")
        
        return sfCredDict    
        
    except Exception as e:
       print(e)
       raise
                   

def getConnection():

    #logger.debug("start function getConnection()")
    print("start function getConnection()")
    
    con = None

    try: 

        ###################################################
        # Get Snowflake credentials
        ###################################################     
        sfCredDict = getSFCredentials()

        ###################################################
        # Connect to Snowflake
        ###################################################     
        con = snowflake.connector.connect(user=sfCredDict['SNOW_USER'], password=sfCredDict['SNOW_PASSWORD'], 
            account=sfCredDict['SNOW_ACCOUNT'], 
            #warehouse=sfCredDict['SNOW_WAREHOUSE'], 
            warehouse=SF_XTR_WAREHOUSE,
            database=sfCredDict['SNOW_DATABASE'])        

        #logger.info("Connected to Database!")
        #logger.debug(getDriverVersion(con))
        print("Connected to Database!")
        
        return con

    except Exception as err:    
        #logger.error("Could NOT connect to Database!")
        #logger.error(err)
        print("Could NOT connect to Database!")
        print(err)
        
        raise


def getAllRows(sqlStmt, tupParms):
    ########################################################
    # function parms: 
    #   1) SQL string w/parm markers/or no parms
    #   2) tuple list of parms for SQL string (can be null). 
    #########################################################
    #logger.info("start function getAllRows()")
    
    print("start function getAllRows()")
    print(f"{sqlStmt=}")

    try:

        # get connection to DB
        cnx = getConnection()

        # create cursor
        curs = cnx.cursor()
        if curs is None:
            raise NullCursorException() 
        
        # create cursor for SQL statement --> fetch all rows 
        results = curs.execute(sqlStmt).fetchall()

        # display cursor ID
        print(f"{curs.sfqid=}")
        
        # create list of column names
        loadCursorColumnList(curs.description)

        # parameters must be a tuple
        #if tupParms is None:
        #    cursor = cnx.execute_string(sqlStmt)
        #else:
        #    cursor = cnx.execute_string(sqlStmt)
        
        print(f"{curs.rowcount=}")
        print(f"{curs._total_rowcount=}")

        return results     

    except Exception as e:
        print(f"Error with Select: {sqlStmt}") 
        print(e)
        raise
    
    finally: 
        if cnx is not None:
            if curs is not None:
               curs.close()
            cnx.close()    


def getOneRow(sqlStmt, tupParms):
    ########################################################
    # function parms: 
    #   1) SQL string w/parm markers/or no parms
    #   2) tuple list of parms for SQL string (can be null). 
    #########################################################
    print("start function getOneRow()")
    print(f"{sqlStmt=}")

    try:

        # get connection to DB
        cnx = getConnection()

        # create cursor
        curs = cnx.cursor()
        if curs is None:
            raise NullCursorException() 
        
        # create cursor for SQL statement --> fetch one row
        results = curs.execute(sqlStmt).fetchone()
        
        # display cursor ID
        print(f"{curs.sfqid=}")
        
        # create list of column names
        loadCursorColumnList(curs.description)

        return results     

    except Exception as e:
        print(f"Error with Select: {sqlStmt}") 
        print(e)
        raise
    
    finally: 
        if cnx is not None:
            if curs is not None:
               curs.close()
            cnx.close()    


########################################################
# To retrieve the results of a cursor in increments:
# 1) call openCursor2GetRowsInIncrements to get a cursor
# 2) call getNextRows passing in cursor from #1, and NOF rows to retrieve
# 3) call closeCursor2GetRowsInIncrements
########################################################
def openCursor2GetRowsInIncrements(sqlStmt):
    ########################################################
    # function parms: 
    #   1) SQL string w/parm markers/or no parms
    #########################################################
    print("start function openCursor2GetRowsInIncrements()")
    print(f"{sqlStmt=}")

    try:

        # get connection to DB
        cnx = getConnection()

        # create cursor
        curs = cnx.cursor()
        if curs is None:
            raise NullCursorException() 
        
        # create cursor for SQL statement --> fetch one row
        curs.execute(sqlStmt)
        
        print(f"{curs.sfqid=}")

        return curs     


    except Exception as e:
        print(f"Error with Select: {sqlStmt}") 
        print(e)
        raise


def closeCursor2GetRowsInIncrements(curs):
    ########################################################
    # function parms: 
    #   1) SQL string w/parm markers/or no parms
    #########################################################
    print("start function closeCursor2GetRowsInIncrements()")

    try:

        cnx = curs.connection 
        
        if  cnx  is not None:
            if curs is not None:
               curs.close()
            cnx.close()    
            print("SF Connection is closed")


    except Exception as e:
        print(f"Error closing connection and/or cursor") 
        print(e)
        raise
        

def getNextRows(curs, iRows2Read):
    ########################################################
    # function parms: 
    #   1) SQL string w/parm markers/or no parms
    #   2) tuple list of parms for SQL string (can be null). 
    #########################################################
    print("start function getNextRows()")

    try:

        # fetch iRows2Read rows
        results = curs.fetchmany(iRows2Read)
        
        # display cursor ID
        print(f"{curs.sfqid=}")
        
        # create list of column names
        loadCursorColumnList(curs.description)

        return results     

    except Exception as e:
        print("Error in getNextRows function")
        print(e)
            
        raise


def InsertIntoTable(sqlStmt):
    ########################################################
    # function parms: 
    #   1) SQL INSERT statement
    #########################################################
    #logger.info("start function InsertIntoTable()")
    
    print("start function InsertIntoTable()")
    print(f"{sqlStmt=}")

    try:

        # get connection to DB
        cnx = getConnection()
        
        # create cursor for SQL statement --> fetch all rows 
        result = cnx.execute_string(sqlStmt,remove_comments=True)

        # display cursor ID
        print(f"{result=}")
        
        return 0     

    except Exception as e:
        print(f"Error with Select: {sqlStmt}") 
        print(e)
        raise
    
    finally: 
        if cnx is not None:
            cnx.close()    


def loadCursorColumnList(cursorDescription):

    # cursor columns: [ResultMetadata(name='GEO_SSA_STATE_CD', type_code=2, display_size=None, internal_size=2, precision=None, scale=None, is_nullable=False)
    #                , ResultMetadata(name='GEO_SSA_STATE_NAME', type_code=2, display_size=None, internal_size=17, precision=None, scale=None, is_nullable=False)
    #                , ResultMetadata(name='ST_SORT_ORD', type_code=2, display_size=None, internal_size=1, precision=None, scale=None, is_nullable=False)
    #                , ResultMetadata(name='ST_GROUP_CD', type_code=0, display_size=None, internal_size=None, precision=2, scale=0, is_nullable=False)]
    print(f"cursor description: {cursorDescription}")

    global cursorColumnNames 
    cursorColumnNames = []
       
        
    for column in cursorDescription:
        cursorColumnNames.append(column[0])

    print(f"{cursorColumnNames=}")
    return True 

def getCursorColumnList():

    return cursorColumnNames

    
def createCSVFile(sFilename, header, rows, cDelim):
    ####################################################################
    # Rows = results-set
    # NOTE: QUOTE_ALL --> so that zip_code '00000' can be treated as 
    #       string and not integer
    #       csv.QUOTE_MINIMAL --> only use quotes when necessary like when field contains delimiter
    #       csv.QUOTE_STRINGS --> quote all strings (like '00000'?) 
    ####################################################################
    print("start function createCSVFile()")

    with open(sFilename, 'w', newline='', encoding="utf-8") as csvfile:
        filewriter = csv.writer(csvfile, delimiter=cDelim, quotechar='"', quoting=csv.QUOTE_MINIMAL)
        if header != None:                        
            filewriter.writerow(header)
        filewriter.writerows(rows)
        
    

