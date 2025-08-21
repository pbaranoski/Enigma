#!/usr/bin/env python

import sys
import SQLSnowFlakeFncts

# How to get a single Row from a select            
results = SQLSnowFlakeFncts.getOneRow("""SELECT REQUESTOR_ID, EMAIL_ADR
FROM BIA_DEV.CMS_TARGET_XTR_DEV.DSH_REQ_EMAILS
WHERE REQUESTOR_ID = 'MickeyMouse'
AND EMAIL_ADR = 'Mickey@disney.com' """, None)

print(results)

if results == None:
    # How to get a single Row from a select            
    results = SQLSnowFlakeFncts.InsertIntoTable("""INSERT INTO BIA_DEV.CMS_TARGET_XTR_DEV.DSH_REQ_EMAILS (REQUESTOR_ID, EMAIL_ADR) 
    VALUES ('MickeyMouse','Mickey@disney.com') """)

#print(results)

sys.exit(0)

#####################################################################
# Test calls
#####################################################################

# How to get a single Row from a select            
results = SQLSnowFlakeFncts.getOneRow("SELECT '5' AS COL1 FROM DUAL ", None)
print(results)

#######################################
# How to get all rows from a select and process each row in a "for" statement
results = SQLSnowFlakeFncts.getAllRows("""SELECT GEO_SSA_STATE_CD, GEO_SSA_STATE_NAME FROM IDRC_DEV.CMS_DIM_GEO_DEV.GEO_SSA_STATE_CD
            """,None)

for row in results:
    print(row) 

#################################################        
# How to create a csv file from a results-set

sFilename="/app/IDRC/XTR/CMS/scripts/run/test.csv"
header=SQLSnowFlakeFncts.getCursorColumnList()
rows=results
cDelim=","
SQLSnowFlakeFncts.createCSVFile(sFilename, header, rows, cDelim)

#################################################
# How to get all rows from a select and process each row in a "for" statement. Example #2

results = SQLSnowFlakeFncts.getAllRows("""SELECT DISTINCT 
            CASE WHEN GEO_SSA_STATE_CD BETWEEN '67' AND '96' THEN GEO_SSA_STATE_CD ELSE '99' END AS GEO_SSA_STATE_CD
            , 'RESIDENCE UNKNOWN' AS GEO_SSA_STATE_NAME
            , '5' AS ST_SORT_ORD  , 99 AS ST_GROUP_CD 
            FROM IDRC_DEV.CMS_DIM_GEO_DEV.GEO_SSA_STATE_CD
            WHERE GEO_SSA_STATE_CD BETWEEN '67' AND '96'
            OR GEO_SSA_STATE_CD IN ('99','~ ', 'UK')""",None)
            
for row in results:
    print(row) 

##################################################
# How to create a cursor to get the results-set in increments (in-case the results-set is too large).
curs = SQLSnowFlakeFncts.openCursor2GetRowsInIncrements("SELECT GEO_SSA_STATE_CD, GEO_SSA_STATE_NAME FROM IDRC_DEV.CMS_DIM_GEO_DEV.GEO_SSA_STATE_CD")

results = SQLSnowFlakeFncts.getNextRows(curs, 3)
for row in results:
    print(row) 

print("next 3")

results = SQLSnowFlakeFncts.getNextRows(curs, 3)
for row in results:
    print(row) 

print("next 3")

results = SQLSnowFlakeFncts.getNextRows(curs, 3)
for row in results:
    print(row) 

SQLSnowFlakeFncts.closeCursor2GetRowsInIncrements(curs)
    
    
sys.exit(0)    