########################################################################################################
# Name:  DashboardInfo_SFUI.py
#
# Desc: Python program to find SFUI (or override) S3 files and get file information to add to Dashboard.
#
#       ./python3 DashboardInfo_SFUI.py --BktFldrNFilePrefix aws-hhs-cms-eadg-bia-ddom-extracts/xtr/DOJ/DOJ_TOUHY --FromDate 20241001 --ToDate 20241010 --TMSTMP 20250407.115600
#       ./python3 DashboardInfo_SFUI.py --BktFldrNFilePrefix aws-hhs-cms-eadg-bia-ddom-extracts/xtr/DOJ/DOJ_TOUHY  --FromDate 20250221 --ToDate 20250221 --TMSTMP 20250407.115600
#
# Ex. JOB_DTLS json record: {'log': 'DOJ_TOUHY_MICHAEL_NOLOG_20250221.202606.log', 'ext': 'DOJ_SFUI_TOUHY_NOLOG', 'runTmstmp': '20250221.202606', 'ExtractFile': 'DOJ_TOUHY_MICHAEL_CARTER_MA_2023.txt.gz', 'RecCount': 377483, 'FileByteSize': 872740696, 'HumanFileSize': '832 MB'}
# Ex. JOB_INFO json record: {"log": "DSH_Extracts_20250324.063004.log","ext": "DSH","runTmstmp": "20250324.063004","success": "Y"}
#
# Created: Paul Baranoski
#
# Paul Baranoski 2025-04-07 Created script.
########################################################################################################

import boto3 
import gzip
import argparse
import sys
import os
import datetime
import json
import logging

from datetime import datetime

currentDirectory = os.path.dirname(os.path.realpath(__file__))
rootDirectory = os.path.abspath(os.path.join(currentDirectory, ".."))
utilDirectory = os.getenv('CMN_UTIL')

sys.path.append(rootDirectory)
sys.path.append(utilDirectory)

import snowconvert_helpers
from snowconvert_helpers import Export



# 5GB limit
PUT_BKT_LIMIT = (1024 ** 3) * 5

# Setup logger to display timestamp
logging.basicConfig(format='$(levelname) %(asctime)s => %(message)s', level=logging.INFO)

# bytes pretty-printing
UNITS_MAPPING = [
    (1<<50, ' PB'),
    (1<<40, ' TB'),
    (1<<30, ' GB'),
    (1<<20, ' MB'),
    (1<<10, ' KB'),
    (1, (' byte', ' bytes')),
]

def pretty_size(bytes, units=UNITS_MAPPING):
    """Get human-readable file sizes.
    simplified version of https://pypi.python.org/pypi/hurry.filesize/
    """
    for factor, suffix in units:
        if bytes >= factor:
            break
    amount = int(bytes / factor)

    if isinstance(suffix, tuple):
        singular, multiple = suffix
        if amount == 1:
            suffix = singular
        else:
            suffix = multiple

    return str(amount) + suffix
    

def processSFUIFiles4Dashboard(folder_n_file_prefix, FromDt, ToDt, JobDtlPathNFilename, JobInfoPathNFilename):

    try:

        print("")
        print("Start processSFUIFiles4Dashboard function: " )
       
        sJOBDTLS_Recs : str = ""
        sJOBINFO_Recs : str = ""
        
        ###############################################################
        # Get list of S3 SFUI extract files
        ###############################################################
        print(f"Get list of objects {bucketname=} {folder_n_file_prefix=} {FromDt=} {ToDt=}" )

        SFUI_Files_List = get_S3_list_of_files(bucketname, folder_n_file_prefix, FromDt, ToDt)
        
        ###############################################################
        # Parse thru FilesList to build json records for load into database 
        ###############################################################
        print("")
        print("Process list of S3 files to build json records for load into Dashboard JOBDTLS and JOBINFO tables.")

        for SFUI_File in SFUI_Files_List:
            print("")
            print(f"{SFUI_File=}")

            # Key = S3 path and extract filename
            # Ex. 'xtr/DOJ/DOJ_TOUHY_MICHAEL_CARTER_MA_2024.txt.gz
            ext_filenameNPath  = SFUI_File['Key']

            # Remove S3 path from Extract filename
            # Ex. ext_filename = DOJ_TOUHY_MICHAEL_CARTER_MA_2024.txt.gz
            lstFilenameNPath = ext_filenameNPath.split("/")
            ext_filename = lstFilenameNPath[len(lstFilenameNPath) - 1]
 
            # Extract first three nodes of Extract filename     
            # Ex. DOJ_TOUHY_MICHAEL_CARTER_MA_2024.txt.gz --> ext_filename_1st_3_nodes='DOJ_TOUHY_MICHAEL'
            ext_filename_1st_3_nodes = ""
            NOF_Delim = 0

            for ch in ext_filename:
                if ch == '_':
                    NOF_Delim += 1
                    if NOF_Delim == 3:
                        break
                        
                ext_filename_1st_3_nodes += ch
            
            
            # get Timestamp and byte count
            ext_tmpstmp = SFUI_File['tmstmp']
            
            # Build bogus logname
            # Ex. "DOJ_SFUI_TOUHY_NOLOG_20240101.080201.log"  or "DOJ_TOUHY_MICHAEL_NO_LOG_20240101.080201.log"
            ext_logname = ext_filename_1st_3_nodes + "_NOLOG_" + ext_tmpstmp + ".log"

            # Get Storage Class
            ext_StorClass = SFUI_File['StorClass']
            print(f"{ext_StorClass=}")
            
            if ext_StorClass == 'DEEP_ARCHIVE':
                # This byte size is the zipped byte size
                ext_byteCount = SFUI_File['bytes']
            
                # convert bytes to Human readable form Ex. size 126 MB
                ext_human_fileSize = pretty_size(ext_byteCount)
                
                ext_recCount = 0
            else:
                #unzip file to get RecCount
                print(f"Get s3 file: {ext_filenameNPath=}")
                gzip_file =  s3_client.get_object(Bucket=bucketname, Key=ext_filenameNPath)

                print(f"Read s3 file")
                strmBytes = gzip_file["Body"].read()

                print(f"S3 zipped file size: {len(strmBytes)}")

                ###############################################################
                # Decompress S3 gzip file
                ###############################################################
                print("Decompress S3 gzip file. " )
                unzipped_content = gzip.decompress(strmBytes)

                # This byte size is the unzipped byte size
                print(f"S3 unzipped file size: {len(unzipped_content)}")
                ext_byteCount = len(unzipped_content)

                # convert bytes to Human readable form Ex. size 126 MB
                ext_human_fileSize = pretty_size(ext_byteCount)
                
                # Get record count
                print("Count newlines " )
                ext_recCount = unzipped_content.count(b'\n')            

                
            ###############################################################
            # Build Dashboard JobDtls json record for loading into table
            ###############################################################
            dctDASHBOARD_JOB_DTLS = {"log": ext_logname,
                                     "ext": ext_filename_1st_3_nodes,
                                     "runTmstmp": ext_tmpstmp,
                                     "ExtractFile": ext_filename,
                                     "RecCount": str(ext_recCount),
                                     "FileByteSize": str(ext_byteCount),
                                     "HumanFileSize": ext_human_fileSize 
                                    }
             
            # convert dict to string 
            strDASHBOARD_JOB_DTLS = json.dumps(dctDASHBOARD_JOB_DTLS)
            
            print(f"{strDASHBOARD_JOB_DTLS=}")

            # Add record to records    
            sJOBDTLS_Recs = sJOBDTLS_Recs + strDASHBOARD_JOB_DTLS + '\n'                    


            ###############################################################
            # Build Dashboard JobINFO json record for loading into table
            ###############################################################
            dctDASHBOARD_JOB_INFO = {"log": ext_logname, 
                                     "ext":  ext_filename_1st_3_nodes,
                                     "runTmstmp": ext_tmpstmp,
                                     "success": "Y"}
            # convert dict to string 
            strDASHBOARD_JOB_INFO = json.dumps(dctDASHBOARD_JOB_INFO)
                                  
            print(f"{strDASHBOARD_JOB_INFO=}")

            # Add record to records    
            sJOBINFO_Recs = sJOBINFO_Recs + strDASHBOARD_JOB_INFO + '\n'                    


        ###############################################################
        # Write Dashboard JobDtls json records into S3 file.
        ###############################################################
        print("")
        print(f"Before put_object for {JobDtlPathNFilename} ")  

        s3_client.put_object(Bucket=bucketname, Key=JobDtlPathNFilename, Body=sJOBDTLS_Recs)

        ###############################################################
        # Write Dashboard JobINFO json records into S3 file.
        ###############################################################
        print(f"Before put_object for {JobInfoPathNFilename} ")  

        s3_client.put_object(Bucket=bucketname, Key=JobInfoPathNFilename, Body=sJOBINFO_Recs)
 

    except Exception as e:  
        print(f'Error occurred in function processSFUIFiles4Dashboard: {e}')
        sys.exit(12) 
 

 
def get_S3_list_of_files(bucketname, folder_n_file_prefix, FromDt, ToDt):

    print("")
    print(f"In function get_S3_list_of_files {bucketname=} {folder_n_file_prefix=} {FromDt=} {ToDt=}" )


    def resp_to_filelist(resp):

        print(f"In function resp_to_filelist" )

        # Exclude ETag; Filter out results that are not within date range
        return [{'Key': x['Key'], 'tmstmp': x['LastModified'].strftime('%Y%m%d.%H%M%S'), 'bytes': x['Size'], 'StorClass': x['StorageClass']} for x in resp['Contents'] if  FromDt <= x['LastModified'].strftime('%Y%m%d') <= ToDt]
        
            
    ###############################################################
    # Get list of S3 SFUI extract files
    ###############################################################
    SFUI_Files_List = []

    resp = s3_client.list_objects_v2(Bucket=bucketname, Prefix=folder_n_file_prefix)
    
    print(f"S3 file objects found before date filters applied --> {resp['KeyCount']=}")
    
    if resp['KeyCount'] == 0:
        print("")
        print(f"No S3 File Objects found before date filters were applied. ")
        
        # Tell shell script that we didn't fail, but don't continue processing since there are no files to process
        sys.exit(4)

    
    print("")
    print(f"List of S3 File Objects before date filters applied and re-formatting: {resp['Contents']}")

    SFUI_Files_List.extend(resp_to_filelist(resp))

    # if there are more entries than can be returned in one request, the key
    # of the last entry returned acts as a pagination value for the next request
    while resp['IsTruncated']:
        print("Found {} objects so far".format(len(SFUI_Files_List)))
        last_key = SFUI_Files_List[-1][0]
        resp = s3_client.List_objects(Bucket=BUCKET, Prefix=folder, Marker=last_key)
        
        print("")
        print("Next set of S3 File Objects")
        #print("List of S3 File Objects before date filters and re-formatting: " + resp['Contents'])

        SFUI_Files_list.extend(resp_to_filelist(resp))


    # Filter list by date
    print()
    print(f"List of S3 SFUI Files to process: {str(SFUI_Files_List)}")

    if len(SFUI_Files_List) == 0:
        print("")
        print(f"No S3 File Objects found after date filters were applied. ")

        # Tell shell script that we didn't fail, but don't continue processing since there are no files to process
        sys.exit(4)


    return SFUI_Files_List
 

def updateSFTables(DASHBOARD_JOBINFO_FILE, DASHBOARD_JOBDTLS_FILE, EXT_NAME_LIKE, RUN_FROM_DT, RUN_TO_DT):

    print("")
    print("In function updateSFTables")
    
    ########################################################################################################
    # Define variables
    ########################################################################################################
    script_name = os.path.basename(__file__)
    con = None 
    
    now = datetime.now()
    date_time = now.strftime("%m/%d/%Y, %H:%M:%S")

    ENVNAME=os.getenv('ENVNAME')

    print(f"{ENVNAME=}")
    print(f"{DASHBOARD_JOBINFO_FILE=}")
    print(f"{DASHBOARD_JOBDTLS_FILE=}")
    print(f"{EXT_NAME_LIKE=}")
    

    # boolean - Python Exception status
    bPythonExceptionOccurred=False
    bSQLExecutedSuccessfully=False

    ########################################################################################################
    # Execute SQL
    ########################################################################################################
    print('')
    print("Run date and time: " + date_time  )
    print

    # If there is a SQL error, the exception is handled internally by snowconvert_helpers. So, the try block' "exception" is not called, but the code goes directly
    # to "finally" block instead.
    try:
       snowconvert_helpers.configure_log()
       con = snowconvert_helpers.log_on()
       snowconvert_helpers.execute_sql_statement(f"alter session set query_tag='{script_name}'",con,exit_on_error = True)
       snowconvert_helpers.execute_sql_statement("""USE WAREHOUSE ${sf_xtr_warehouse}""", con,exit_on_error = True)

       # Delete Rows that may exist on tables for current run dates - in case of re-run 
       snowconvert_helpers.execute_sql_statement(f"""DELETE FROM BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.UTIL_EXT_RUNS WHERE EXT_NAME LIKE '{EXT_NAME_LIKE}' AND CAST(RUN_TMSTMP AS DATE) BETWEEN TO_DATE('{RUN_FROM_DT}','YYYYMMDD') AND TO_DATE('{RUN_TO_DT}','YYYYMMDD') """, con, exit_on_error=True)
       snowconvert_helpers.execute_sql_statement(f"""DELETE FROM BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.UTIL_EXT_RUN_EXT_FILES WHERE EXT_NAME LIKE '{EXT_NAME_LIKE}' AND CAST(RUN_TMSTMP AS DATE) BETWEEN TO_DATE('{RUN_FROM_DT}','YYYYMMDD') AND TO_DATE('{RUN_TO_DT}','YYYYMMDD') """, con, exit_on_error=True)
     
       print("Before COPY INTO ") 
       ## INSERT DATA INTO UTIL_EXT_RUNS TABLE ##
       snowconvert_helpers.execute_sql_statement(f"""COPY INTO BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.UTIL_EXT_RUNS
        (EXT_NAME, LOG_NAME, RUN_TMSTMP, DOW, SUCCESS_IND)
        FROM (SELECT $1:ext,
                     $1:log,
                     TO_TIMESTAMP(TO_CHAR($1:runTmstmp),'YYYYMMDD.HH24MISS'), 
                     DAYNAME(TO_TIMESTAMP(TO_CHAR($1:runTmstmp),'YYYYMMDD.HH24MISS')),
                     $1:success                 
              FROM @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_DASHBOARD_STG/{DASHBOARD_JOBINFO_FILE} ) 
        FILE_FORMAT = (TYPE = JSON) FORCE=TRUE """, con,exit_on_error = True)
     
     
       ## INSERT DATA INTO UTIL_EXT_RUN_EXT_FILES TABLE ##
       snowconvert_helpers.execute_sql_statement(f"""COPY INTO BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.UTIL_EXT_RUN_EXT_FILES
        (EXT_NAME, LOG_NAME, RUN_TMSTMP, EXT_FILENAME, REC_COUNT, BYTE_COUNT, HUMAN_FILE_SIZE)
        FROM (SELECT $1:ext,
                     $1:log,
                     TO_TIMESTAMP(TO_CHAR($1:runTmstmp),'YYYYMMDD.HH24MISS') ,
                     $1:ExtractFile,
                     $1:RecCount,  
                     $1:FileByteSize, 
                     $1:HumanFileSize                  
              FROM @BIA_{ENVNAME}.CMS_STAGE_XTR_{ENVNAME}.BIA_{ENVNAME}_XTR_DASHBOARD_STG/{DASHBOARD_JOBDTLS_FILE} ) 
        FILE_FORMAT = (TYPE = JSON) FORCE=TRUE """, con,exit_on_error = True)
     
       bSQLExecutedSuccessfully=True
       snowconvert_helpers.quit_application()

    except Exception as e:
       print(e)
       # Let shell script know that python code failed.
       bPythonExceptionOccurred=True  

    finally:
       if con is not None:
          con.close()

       # Let shell script know that python code failed.      
       if bPythonExceptionOccurred == True:
          sys.exit(12) 
       else:     
          snowconvert_helpers.quit_application()
          
          if bSQLExecutedSuccessfully == False:
              sys.exit(12)
    
####################################################################
# main
####################################################################
if __name__ == "__main__":

    ##########################################################################
    # Warning-level will print out to stdout (so it will appear in log files)
    ##########################################################################
    print("")
    print("Starting Dashboard_SFUI python program")

    ##########################################################################
    # INFO: Get S3 resource and client
    ##########################################################################
    # boto3.resource is a high-level services class wrap around boto3.client.
    # boto3.Session.client is low-level service
    #
    # boto3.resource is meant to attach connected resources under where you can  
    # later use other resources without specifying the original resource-id.
    ##########################################################################
    print("Get S3 resource and client tokens")
    
    global s3_client
    #s3_client = boto3.resource('s3')
    s3_client = boto3.client("s3")

    ##########################################################################
    # Get parameters
    ##########################################################################
    try:    
        parser = argparse.ArgumentParser(description="S3 file combiner")
        parser.add_argument("--BktFldrNFilePrefix", help="SFUI bucket, folder, and file prefix of S3 files to process")
        parser.add_argument("--FromDate", help="S3 files created on or after this date to be processed. Format YYYYMMDD")
        parser.add_argument("--ToDate", help="S3 files created on or before this date to be processed. Format YYYYMMDD")
        parser.add_argument("--TMSTMP", help="Timestamp to be used for the Dashboard JobDtl and JobInfo Filenames to be created")
        
        args = parser.parse_args()

        #######################################################
        # Example parameters.
        #######################################################
        #args.BktFldrNFilePrefix = 'aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod/xtr/DEV/DOJ/DOJ_TOUHY'
        #args.BktFldrNFilePrefix = 'aws-hhs-cms-eadg-bia-ddom-extracts/xtr/DOJ/DOJ_TOUHY'
        #args.BktFldrNFilePrefix = 'aws-hhs-cms-eadg-bia-ddom-extracts/xtr/DOJ/DOJ_SFUI'
        #args.BktFldrNFilePrefix = 'aws-hhs-cms-eadg-bia-ddom-extracts/xtr/FOIA/FOIA_SFUI'
        # FromDt = '2024-10-01'  ToDt = '2024-10-01'
       
        ####################################################################
        # Assign parameters to variables
        ####################################################################
        BktFldrNFilePrefix = args.BktFldrNFilePrefix
        FromDate = args.FromDate
        ToDate = args.ToDate
        TMSTMP = args.TMSTMP


        ####################################################################
        # Display parameter values
        ####################################################################
        print("")
        print(f"{BktFldrNFilePrefix=}")
        print(f"{FromDate=}")
        print(f"{ToDate=}")
        print(f"{TMSTMP=}")

       
        ####################################################################
        # Split BktFldrNFilePrefix into 1) bucket and 2) folder/file_prefix
        ####################################################################
        BktFldrNFilePrefixParts = BktFldrNFilePrefix.split("/", 1)
        BktFldrNFilePrefixPartsLen = len(BktFldrNFilePrefixParts)
        
        # Get the bucket name 
        global bucketname
        bucketname = BktFldrNFilePrefixParts [0]
        
        # Get the folder path and file prefix 
        # Ex. folder_n_file_prefix = 'xtr/DOJ/DOJ_SFUI' or 'xtr/FOIA/FOIA_SFUI' or 'xtr/DOJ/DOJ_TOUHY'
        folder_n_file_prefix = BktFldrNFilePrefixParts[1]

       
        ####################################################################
        # Display derived variables
        ####################################################################
        print("")
        print(f"{bucketname=}")
        print(f"{folder_n_file_prefix=}")

        ####################################################################
        # Derive S3 Dashboard folder
        ####################################################################
        # Ex. xtr/DOJ/DOJ_TOUHY --> /xtr   Ex. xtr/DEV/DOJ/DOJ_TOUHY --> xtr/DEV
        ####################################################################
        FldrNFilePrefixParts = folder_n_file_prefix.split("/")
        FldrNFilePrefixPartsLen = len(FldrNFilePrefixParts) 

        print("")
        print(f"{FldrNFilePrefixPartsLen=}")
        
        #  xtr/DEV/DOJ/DOJ_TOUHY--> 4parts - 2parts --> xtr/DEV 
        #  xtr/DOJ/DOJ_TOUHY--> 3parts - 2parts --> xtr 
        BktXTRFolderLen = FldrNFilePrefixPartsLen - 2
        print(f"{BktXTRFolderLen=}")
        
        bucketNHLFldr = "/".join(FldrNFilePrefixParts [ : BktXTRFolderLen ])
        print(f"{bucketNHLFldr=}")

        # Ex. 'xtr/Dashboard/'  or  'xtr/DEV/Dashboard/'
        S3DashboardFldr = bucketNHLFldr + f"/Dashboard/"
        print(f"{S3DashboardFldr=}")

        #  xtr/DOJ/DOJ_TOUHY--> DOJ_TOUHY --> array index is 0 based, so must subtract one 
        #  xtr/DOJ/DOJ_SFUI --> DOJ_SFUI --> array index is 0 based, so must subtract one 
        ExtNameLike = FldrNFilePrefixParts[FldrNFilePrefixPartsLen - 1 ] + "%"
        print(f"{ExtNameLike=}")
        
        ####################################################################
        # Create Dashboard S3 output files
        ####################################################################
        # Ex. xtr/Dashboard/DASHBOARD_JOB_DTLS_EXTRACT_FILES_{TMSTMP}.json    
        JobDtlPathNFilename =  S3DashboardFldr + f"DASHBOARD_JOB_DTLS_EXTRACT_FILES_{TMSTMP}.json" 
        JobInfoPathNFilename =  S3DashboardFldr + f"DASHBOARD_JOB_INFO_{TMSTMP}.json" 

        ####################################################################
        # Process S3 SF UI Files
        ####################################################################
        print("Call processSFUIFiles4Dashboard function")
        processSFUIFiles4Dashboard(folder_n_file_prefix, FromDate, ToDate, JobDtlPathNFilename, JobInfoPathNFilename)
    
        ###############################################################
        # Update SF tables using json files
        ###############################################################
        updateSFTables(f"DASHBOARD_JOB_INFO_{TMSTMP}.json", f"DASHBOARD_JOB_DTLS_EXTRACT_FILES_{TMSTMP}.json", ExtNameLike, FromDate, ToDate)

        sys.exit(0)
        
        
    except Exception as e:
        print("Exception occured in DashboardInfo_SFUI.py.")
        print(e)

        sys.exit(12)  


