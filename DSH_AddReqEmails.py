#!/usr/bin/env python

########################################################################################################
# Name:  DSH_AddReqEmails.py
#
# Desc: Python program to find SFUI (or override) S3 files and get file information to add to Dashboard.
#
#       ./python3 DSH_AddReqEmails.py --ReqID MickeyMouse --Email MickeyMouse@Disney.com 
#                    
# Created: Paul Baranoski
#
# Paul Baranoski 2025-05-08 Created script.
########################################################################################################

import sys
import os
import argparse
import SQLSnowFlakeFncts


####################################################################
# main
####################################################################
if __name__ == "__main__":

    bPythonExceptionOccurred=False  

    ########################################################################################################
    # RUN
    ########################################################################################################
    try:
        print("")
        print("Start DSH_AddReqEmails.py python program" )

        parser = argparse.ArgumentParser(description="DSH_AddReqEmails")
        parser.add_argument("--ReqID", help="Unique ID on the Requestor's DSH Request file")
        parser.add_argument("--Email", help="Requestor's Email address")
        
        args = parser.parse_args()

        ENVNAME=os.getenv('ENVNAME')

        ####################################################################
        # Assign parameters to variables
        ####################################################################
        ReqID = args.ReqID
        Email = args.Email

        print("Parameters received by program." )
        
        print(f"{ReqID=}")
        print(f"{Email=}")
        
        ####################################################################
        # Is DSH Request Email and Unique-ID already in DSH_REQ_EMAILS table?
        ####################################################################
        print("")
        print("Is DSH Request Unique-Id and Email already in DSH_REQ_EMAILS Table?")
        results = SQLSnowFlakeFncts.getOneRow(f"""SELECT REQUESTOR_ID, EMAIL_ADR FROM BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.DSH_REQ_EMAILS
                                                    WHERE REQUESTOR_ID = '{ReqID}' AND EMAIL_ADR = '{Email}' """, None)

        print(f"{results=}")

        ####################################################################
        # Insert new row in DSH_REQ_EMAILS table if data not in table.
        ####################################################################
        if results == None:
            print("")
            print("DSH Request Unique-Id and Email Not in DSH_REQ_EMAILS Table")
                        
            results = SQLSnowFlakeFncts.InsertIntoTable(f"""INSERT INTO BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.DSH_REQ_EMAILS (REQUESTOR_ID, EMAIL_ADR) 
                                                           VALUES ('{ReqID}','{Email}') """)

            print(f"{results=}")


        sys.exit(0)
    
            
    except Exception as e:
       print(e)

       # Let shell script know that python code failed.
       bPythonExceptionOccurred=True  

    finally:
       # Let shell script know that python code failed.      
       if bPythonExceptionOccurred == True:
          sys.exit(12) 
       else:   
          sys.exit(0)
      

