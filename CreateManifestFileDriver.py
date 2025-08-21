#!/usr/bin/env python
############################################################################################################
# Name:  CreateManifestFileDriver.py
#
# Desc: Create Manifest file required for transfers of Extract files to Outside Consumers using BOX 
#
# Execute as python3 CreateManifestFileDriver.py --bucket {parm1} --folder {parm2} --runToken {parm3} --BoxEmails {parm4} --Manifest_folder {parm5} --Ext_Type {parm6}
#
# parm1 = S3 bucket where extract files live.       Ex1: bucket=aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod  
#                                                   Ex2: bucket=aws-hhs-cms-eadg-bia-ddom-extracts
# parm2 = S3 folder name where extract files live.  Ex1: xtr/DEV/Blbtn/  
#                                                   Ex2: xtr/Blbtn/
# parm3 = S3 filename timestamp  Ex:  20220922.084321   
# parm4 = Box account email addresses (comma delimited string)  
# parm5 = (optional) Destination manifest_files folder (DEFAULT=Manifest_files, SSA_BOX, VA_BOX) 
# parm6 = (optional) Key to use against JIRA_Extract_Mappings.txt to find JIRA ticket # for manifest file. When Key cannot be determined by S3 folder like SSA_BOX. 
#
#
# 07/28/2025 Paul Baranoski   Created script.	
############################################################################################################
import boto3 
import logging
import sys
import argparse

import json

#import datetime
from datetime import datetime
from datetime import date,timedelta

import os
import subprocess

# Our common module with variable constants
from SET_XTR_ENV import *

LOG_DIR = "/app/IDRC/XTR/CMS/logs/"
RUNDIR = "/app/IDRC/XTR/CMS/scripts/run/"

#############################################################
# Functions
#############################################################
def setManifestLogging(LOGNAME):


    # Configure root logger
    #logging.config.fileConfig(os.path.join(config_dir,"loggerConfig.cfg"))
    
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(funcName)-22s %(message)s",
        encoding='utf-8', datefmt="%Y-%m-%d %H:%M:%S", 
        #filename=f"{LOG_DIR}BuildRunExtCalendar_{TMSTMP}.log"
        handlers=[
        logging.FileHandler(f"{LOGNAME}"),
        logging.StreamHandler(sys.stdout)],    
        level=logging.INFO)


    global manifestLogger

    loggerName = os.path.basename(f"LOGNAME").replace(f"_{TMSTMP}.log","")
    manifestLogger = logging.getLogger(loggerName)
    
    
    return manifestLogger


def getConfigFileContents(sConfigFilename):
    
    s3ConfigFolder_n_filename = f"{CONFIG_BUCKET_FLDR}{sConfigFilename}"
    manifestLogger.info(f"{s3ConfigFolder_n_filename=}")

    # Is config file in s3?         
    resp = s3_client.list_objects_v2(Bucket=XTR_BUCKET, Prefix=s3ConfigFolder_n_filename)
    
    if resp == None:
        ## Send Failure email	
        SUBJECT=f"CreateManifestFileDriver.py - Failed ({ENVNAME})"
        MSG=f"Config file {s3ConfigFolder_n_filename} is not in S3. Process failed. "
        sp_info = subprocess.check_output(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], text=True)
        manifestLogger.info(sp_info) 
        raise Exception(f"Config file {s3ConfigFolder_n_filename} is not in S3. Process failed.")        

    
    # Get config file from S3    
    ConfigFile = s3_client.get_object(Bucket=XTR_BUCKET, Key=s3ConfigFolder_n_filename)

    if ConfigFile == None:
        ## Send Failure email	
        SUBJECT=f"CreateManifestFileDriver.py - Failed ({ENVNAME})"
        MSG=f"Config file {s3ConfigFolder_n_filename} is not in S3. Process failed. "
        sp_info = subprocess.check_output(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], text=True)
        manifestLogger.info(sp_info)  
        raise Exception(f"Config file {s3ConfigFolder_n_filename} is not in S3. Process failed.")        


    # S3 Body is byte array. Convert byte array to utf-8 string. Splitlines recognizes "\r\n" as end-of-record markers     
    lstConfigRecs = ConfigFile["Body"].read().decode('utf-8').splitlines()
    manifestLogger.info(f"{lstConfigRecs=}") 

    return lstConfigRecs
    
    
def findDOJFOIAJiraTicketMatch(lstConfigExtKeys, p_S3ExtFilename):

    MatchingKey=""

    # Ex. p_ExtType = DOJ_ANTI_TRUST, FOIA_PISTORINO
    #manifestLogger.info(f"{p_ExtType=}")
    manifestLogger.info(f"{p_S3ExtFilename=}")

    for configExtKey in lstConfigExtKeys:
        manifestLogger.info(f"{configExtKey=}")
        
        # is this a DOJ or FOIA configuration? No --> go to next config record
        #idx = configExtKey.find(p_ExtType)
        #if idx == -1:
        #    continue
            
        # does the config key match the filename? --> Ex. DOJ_ANTI_TRUST 
        keyMatch_idx =  p_S3ExtFilename.find(configExtKey)
        if keyMatch_idx != -1:
            MatchingKey=f"{configExtKey}"
            break            

	
    manifestLogger.info(f"{MatchingKey=}")
    return MatchingKey


def ProcessFiles2IncludeInManifestFile(s3ManifestFilesFolder, sModelManifestFilename, Files2IncludeInManifest, jiraURL, RecipientEmails):


    ############################################
    # Variables and constants
    ############################################
    # 32212254720-30GB  42949672960-40GB  53687091200-50GB
    iMaxSizeAllFiles = 42949672960  
    iTotSizeAllFiles = 0

    iMaxNOFFiles = 35
    iTotNOFFiles = 0

    iMaxManifestFileBytes = 8000
    iNOFManifestFileBytes = 0


    ######################################################
    # Create Group Lists to ensure:
    #    1) Total NOF files not exceeded
    #    2) Total size of files in Manifest file is < iMaxSizeAllFiles (40 or 50 GB)
    ######################################################
    group_filename_lists = []
    cur_filename_list = []

    for File2Include in Files2IncludeInManifest:

        manifestLogger.info(f"{File2Include=}")
        
        # if a single file is larger than max, we can end up with an empty list of files to include in a manifest file
        if (File2Include['Size']) > iMaxSizeAllFiles:
             raise Exception(f"File {File2Include['Key']} is too big to place in manifest file. Create a smaller sized file.")

        
        if (  ((iTotNOFFiles + 1)  > iMaxNOFFiles) or 
            
              ((iTotSizeAllFiles + File2Include['Size']) > iMaxSizeAllFiles) ):

            manifestLogger.info("")
            manifestLogger.info("New Group")
            manifestLogger.info(f"{cur_filename_list=}")

            manifestLogger.info(f"{iTotNOFFiles=}")
            manifestLogger.info(f"{iTotSizeAllFiles=}")
            manifestLogger.info(f"{iNOFManifestFileBytes=}")
            
            group_filename_lists.append(cur_filename_list)
            cur_filename_list = []

            iTotNOFFiles = 0
            iTotSizeAllFiles = 0

            iNOFManifestFileBytes = 0


        # Add filename with path to filename_list
        #####cur_filename_list.append(os.path.basename(File2Include['Key']))
        cur_filename_list.append(File2Include['Key'])

        iTotNOFFiles += 1
        iTotSizeAllFiles +=  File2Include['Size']

        #print(f'{iTotSizeAllFiles=}')
        #manifestLogger.info(f"{File2Include['Size']=}")

        iNOFManifestFileBytes += len(File2Include['Key']) + 70


    ##################################################
    # Add remainder to group
    ##################################################
    if len(cur_filename_list) > 0:
        manifestLogger.info("")
        manifestLogger.info(f"remainder->{iTotNOFFiles=}")
        group_filename_lists.append(cur_filename_list)    

        manifestLogger.info(f"{iTotNOFFiles=}")
        manifestLogger.info(f"{iTotSizeAllFiles=}")
        manifestLogger.info(f"{iNOFManifestFileBytes=}")


    ##################################################
    # Create a manifest file for each group
    ##################################################
    print("\nBefore group_filenames process")
    
    NOFGroups = len(group_filename_lists)
    manifestLogger.info(f"{NOFGroups=}")
    
    for idx, filename_list in enumerate(group_filename_lists, start=1):
        #print ("\n")
        manifestLogger.info(f"{idx=}")
        manifestLogger.info(f"{len(filename_list)=}")
        manifestLogger.info(filename_list)
        
        # If NOF groups = 1 --> no literal  else add literal
        if NOFGroups == 1:
            idx_lit = ""
        else:    
            idx_lit = f"-{idx}"

        BuildManifestFile(s3ManifestFilesFolder, sModelManifestFilename, filename_list, idx_lit, jiraURL, RecipientEmails)


    
def BuildManifestFile(s3ManifestFilesFolder, sModelManifestFilename, lstFileNames, idx_lit, jiraURL, RecipientEmails):

   
    try:    

        ###############################################
        # process parameters
        ###############################################
        nofFiles = len(lstFileNames)
        manifestLogger.info(f"nofFiles: {nofFiles}")

        # Ex. dataRecepientEmails = "pbaranoski@apprioinc.com,jturner@apprioinc.com,SGayam@apprioinc.com"
        dataRecepientEmails = RecipientEmails

        # Ex. sOutFilePathAndName="/app/IDRC/XTR/CMS/data/OFM_PDE_Manifest_20240102.150137-0.json"
        sManifestFilename = sModelManifestFilename.replace(".json",f"{idx_lit}.json")
        ######manifestLogger.info(f"sOutFilePathAndName={sOutFilePathAndName}")

        # IDRBI-99999-20210126-165003-idx
        tmstmpDataReqID = datetime.today().strftime('%Y%m%d-%H%M%S')

        jiraTicket = jiraURL
        URLParts = jiraTicket.split("/")
        dataRequestID = f"{URLParts[len(URLParts) - 1]}-{tmstmpDataReqID}{idx_lit}"
        manifestLogger.info(f"{dataRequestID=}")

        
        ###############################################
        # Get Environment variables DDOM Contact Info 
        ###############################################
        DDOM_CONTACT_NM = os.getenv('DDOM_CONTACT_NM')
        DDOM_CONTACT_PHNE_NUM = os.getenv('DDOM_CONTACT_PHNE_NUM')
        DDOM_CONTACT_EMAIL = os.getenv('DDOM_CONTACT_EMAIL')

        ###############################################
        # Build List of Filename Dictionary items
        ###############################################
        manifestLogger.info("Build S3 Filename Dictionary items")
        
        # "xtr/FOIA/FOIA_PISTORINO_EXTRACT_CAR_Y2025MARW4_20250725.131535.txt.gz"
    
        lstDictS3Filenames = [{"fileName": os.path.basename(sFileNameNPath), "fileLocation": os.path.dirname(sFileNameNPath)+"/" } for sFileNameNPath in lstFileNames]    
        
        manifestLogger.debug(lstDictS3Filenames)

        ###############################################
        # Build Share Detail Dictionary
        ###############################################
        dctShareDetails = { "dataRequestID" : dataRequestID,
                            "shareDuration" : "30",
                            "dataRecipientEmails" : dataRecepientEmails, 
                            "totalNumberOfFiles"  : str(nofFiles),
                            "jiraTicket" : jiraTicket
        }

        ###############################################
        # Build Contact Info Dictionary
        ###############################################
        dctContactInfo = {
            "fullName": DDOM_CONTACT_NM, 
            "phoneNumber": DDOM_CONTACT_PHNE_NUM,
            "email": DDOM_CONTACT_EMAIL
        }

        ###############################################
        # Build Manifest file Dictionary
        ###############################################
        dctManifest = {"method":"POST",
                "deleteReason":"",
                "fileInformation": lstDictS3Filenames,
                "shareDetails": dctShareDetails,
                "requestorContactInfo": dctContactInfo,
                "comments" : ""  
        }  

        ###############################################
        # Write out manifest json file to S3
        ###############################################
        manifestLogger.info("Convert dictionary to json format")

        json_obj = json.dumps(dctManifest, indent=4)
        manifestLogger.info(f"{json_obj=}")

        destKey = s3ManifestFilesFolder + sManifestFilename
        manifestLogger.info(f"{destKey=}")
        
        manifestLogger.info(f"Put file {destKey} into S3")
        resp = s3_client.put_object(Bucket=XTR_BUCKET, Key=destKey, Body=json_obj)

        manifestLogger.debug(f"{resp=}")
        
        if resp == None:
            ## Send Failure email	
            SUBJECT=f"CreateManifestFileDriver.py - Failed ({ENVNAME})"
            MSG=f"Put manifest file failed. "
            sp_info = subprocess.check_output(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], text=True)
            manifestLogger.info(sp_info)        
            raise Exception(f"Put manifest file {destKey} failed.")  

    except Exception as e:
        manifestLogger.error("Exception occured in CreateManifestFileDriver.py.")
        manifestLogger.error(e)
        raise


def main_processing_loop():
    pass


def createManifestFile(bucket=None, s3folder=None, runToken=None, BoxEmails=None, Manifest_folder=None, Ext_Type=None ):

    try:    

        ##########################################
        # Set Timestamp for log file and extract filenames
        ##########################################
        global TMSTMP
        global LOGNAME


        #TMSTMP = If TMSTMP value set by caller via export --> use that value. 
        #         Else use the timestamp created in this script        
        try:
            TMSTMP = os.environ["TMSTMP"]

        except KeyError:
            # if environment variable doesn't exist --> create it.
            TMSTMP = datetime.now().strftime('%Y%m%d.%H%M%S')
            os.environ["TMSTMP"] = TMSTMP        
        
        print(f"{TMSTMP=}")

        LOGNAME = f"{LOG_DIR}CreateManifestFile_{TMSTMP}.log"
        
        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        setManifestLogging(LOGNAME)

        manifestLogger.info("################################### ")
        manifestLogger.info(f"\nCreateManifestFileDriver.py started at {TMSTMP} ")

        ###########################################################
        # Set working directory to scripts/run directory.
        # This is so subprocess calls will work from RunDeck.  
        ###########################################################
        os.chdir(RUNDIR)
        pwd = os.getcwd()
        manifestLogger.info(f"{pwd=}")

        #######################################################
        # Get parameters.
        #######################################################
        manifestLogger.info(f"{bucket=}")  
        manifestLogger.info(f"{s3folder=}")  
        manifestLogger.info(f"{runToken=}")  
        manifestLogger.info(f"{BoxEmails=}")  
        manifestLogger.info(f"{Manifest_folder=}")  
        manifestLogger.info(f"{Ext_Type=}")  
        
        global S3Bucket
        S3Bucket = bucket
        S3BucketFldr = s3folder
        S3FilenameTmstmp = runToken
        RecipientEmails = BoxEmails
        MANIFEST_FOLDER_OVERRIDE = Manifest_folder
        ExtractTypeOverride = Ext_Type
        
         
        #############################################################
        # Determine if using ExtractType override --> files named differently 
        #  from S3 folder.
        # Ex. S3BucketFldr = "/xtr/DEV/FOIA/" --> "FOIA"
        #############################################################
        manifestLogger.info(f"Determine Extract Type")
        
        if ExtractTypeOverride == None:
            # Remove last slash for "split" to work properly
            lstNodes = S3BucketFldr[ : -1].split("/")
            # get last node  Ex. xtr/DEV/OFM_PDE --> OFM_PDE  
            ExtractType = lstNodes[-1]

        else:
            manifestLogger.info("Using Extract Type Override parameter: {ExtractTypeOverride}") 
            ExtractType=f"{ExtractTypeOverride}"

        manifestLogger.info(f"{ExtractType=}")

        
        #############################################################
        # Get file prefix to limit NOF items returned from S3 bucket
        # NOTE: This will exclude looking for files in archive folder.
        #############################################################
        file_prefix = ExtractType.split("_")[0]
        
        if file_prefix == "OPMHI":
            file_prefix = "FEHB"
        if file_prefix == "VA":
            file_prefix = "MOA"
        
        manifestLogger.info(f"{file_prefix=}")
        

        #############################################################
        # Global constants  
        #############################################################
        JIRA_MAPPING_FILE = "JIRA_Extract_Mappings.txt"
        MANIFEST_CONFIG_FILE = "MANIFEST_FILE_PROCESS_CONFIG.txt"
       
        manifestLogger.info(f"{CONFIG_BUCKET_FLDR}")        
        manifestLogger.info(f"{MANIFEST_CONFIG_FILE=}")
        manifestLogger.info(f"{JIRA_MAPPING_FILE=}")


        #############################################################
        # Get S3 reference
        #############################################################
        global s3_client
        #s3_client = boto3.resource('s3')
        s3_client = boto3.client("s3")

        ##################################################################
        # Get list of S3 files to include in manifest file with file size
        ##################################################################
        manifestLogger.info("")
        manifestLogger.info("Get list of S3 files to include in manifest file(s)" )
        
        # Get objects in s3 folder         
        resp = s3_client.list_objects_v2(Bucket=S3Bucket, Prefix=S3BucketFldr + file_prefix)
        manifestLogger.debug(f"{resp=}")
        
        if resp == None or resp['IsTruncated']:
            ## Send Failure email	
            SUBJECT=f"CreateManifestFileDriver.py - Failed ({ENVNAME})"
            MSG=f"Get List of S3 objects for folder {S3BucketFldr} failed. "
            sp_info = subprocess.check_output(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], text=True)
            manifestLogger.info(sp_info)        
            raise Exception(f"Get List of S3 objects for folder {S3BucketFldr} failed.")  
       
        # Get the filenames with the run token (filename timestamp)   
        #lstFiles2IncludeInManifest =  [ x['Key']  for x in resp['Contents'] if str(x['Key']).find(S3FilenameTmstmp) != -1 ]
        lstFiles2IncludeInManifest =  [ {"Key": x['Key'], "Size": x['Size']}  for x in resp['Contents'] if str(x['Key']).find(S3FilenameTmstmp) != -1 ]
        
        manifestLogger.info("")
        manifestLogger.info(f"{lstFiles2IncludeInManifest=}")
 
 
        # List should include at least one file
        if len(lstFiles2IncludeInManifest) == 0:
            SUBJECT=f"CreateManifestFileDriver.py - Failed ({ENVNAME})"
            MSG=f"No files found to include in manifest file - failed. "
            sp_info = subprocess.check_output(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], text=True)
            manifestLogger.info(sp_info)        
            raise Exception(f"No files found to include in manifest file - failed.")  


        #############################################################
        # Retrieve config file JIRA_Extract_Mappings.txt contents
        #############################################################
        manifestLogger.info("")
        manifestLogger.info(f"Retrieve config file {JIRA_MAPPING_FILE} contents" )
        lstConfigRecs = getConfigFileContents(JIRA_MAPPING_FILE)
        
        manifestLogger.info("")
        manifestLogger.info(f"{lstConfigRecs=}")
        
        lstExtKeys=[configRec.split("=")[0] for configRec in lstConfigRecs]
        
        manifestLogger.info("")
        manifestLogger.info(f"{lstExtKeys=}")

        #############################################################
        # Extract JIRA URL for Extract type
        # NOTE: Perform "exception" logic for DOJ or FOIA JIRA tickets.
        #       Match each JIRA_MAPPING_FILE DOJ-JIRA-ticket entry to the filename
        #############################################################
        if  ExtractType == "DOJ" or ExtractType == "FOIA":
            manifestLogger.info("Look for DOJ Jira ticket entry")
            S3ExtractFilename = lstFiles2IncludeInManifest[0]['Key']
            MatchingKey = findDOJFOIAJiraTicketMatch(lstExtKeys, S3ExtractFilename)
            if MatchingKey == "":
                manifestLogger.info(f"No MatchingKey found for filename {S3ExtractFilename} on JIRA_MAPPING_FILE config file - Failed")
                raise Exception(f"No MatchingKey found for filename {S3ExtractFilename} on JIRA_MAPPING_FILE config file - Failed") 	

        else:
            manifestLogger.info("Look for non-DOJ-FOIA Jira ticket entry")
            MatchingKey = ExtractType


        ####################################################################
        # Get JIRA ticket #
        ####################################################################
        manifestLogger.info(f"Find matching entry in JIRA_MAPPING_FILE for {MatchingKey}")   

        jiraEntry = ""
        jiraURL = ""
        
        # Look for the matching entry in JIRA config file to get the JIRA ticket number
        for configRec in lstConfigRecs:
            
            if configRec.strip() == "":
                continue
                
            configKey = configRec.split("=")[0]
            configValue = configRec.split("=")[1]
            if configKey == MatchingKey:
                jiraEntry = configRec
                jiraURL = configValue
                
                break

        # Is key missing from JIRA config file?
        if jiraEntry == "":
            manifestLogger.info("")
            manifestLogger.info(f"{JIRA_MAPPING_FILE} missing Extract Type {MatchingKey} mapping.")

            raise Exception(f"{JIRA_MAPPING_FILE} missing Extract Type {MatchingKey} mapping.") 	

        
        manifestLogger.info(f"{jiraEntry=}")
        manifestLogger.info(f"{jiraURL=}")
        
        
        #############################################################
        # Create manifest file filename
        #############################################################
        # Ex. NYSPAP_Manifest_20221006.093854.json
        ManifestFilename = f"{MatchingKey}_Manifest_{S3FilenameTmstmp}.json"
        manifestLogger.info(f"{ManifestFilename=}")


        #############################################################
        # Retrieve Manifest config file contents - Look for possible 
        #    Manifest_Files_Hold folder indicator
        #############################################################
        manifestLogger.info("")
        manifestLogger.info(f"Retrieve config file {MANIFEST_CONFIG_FILE} contents" )
        lstManifestConfigRecs = getConfigFileContents(MANIFEST_CONFIG_FILE)

        manifestLogger.info("")
        manifestLogger.info(f"{lstManifestConfigRecs=}")

        # Default value
        HoldManifestFile = "N"
        
        # Look for the matching entry in JIRA config file to get the JIRA ticket number
        if  ExtractType == "DOJ" or ExtractType == "FOIA":
            HoldManifestFile = "Y"
        else:
            # see if there is a hold for the extract type in the config file
            for ManifestConfigRec in lstManifestConfigRecs:
                manifestLogger.debug(f"{ManifestConfigRec=}")
                
                if ManifestConfigRec.strip() == "":
                    continue
                
                HoldKey = ManifestConfigRec.split("=")[0]
                HoldValue = ManifestConfigRec.split("=")[1]
                if HoldKey == f"{MatchingKey}.HOLD":
                    HoldManifestFile = HoldValue
                    manifestLogger.info(f"Using S3 staging/hold override from {MANIFEST_CONFIG_FILE} configuration file.")
                    manifestLogger.info(f"Override Hold Manifest File value is {HoldValue}")
                    
                    break

        
        manifestLogger.info("")
        manifestLogger.info(f"Manifest Hold value to use is {HoldManifestFile}")


        #######################################################
        # Determine if using override manifest folder
        #######################################################
        if MANIFEST_FOLDER_OVERRIDE != None:
            manifestLogger.info("Using override manifest bucket value")
            S3MANIFEST_FOLDER_TO_USE = MANIFEST_FOLDER_OVERRIDE
        elif HoldManifestFile == "Y":
            manifestLogger.info("Using default manifest bucket value")
            S3MANIFEST_FOLDER_TO_USE = MANIFEST_HOLD_BUCKET_FLDR
        else:
            manifestLogger.info("Using default manifest bucket value")
            S3MANIFEST_FOLDER_TO_USE = MANIFEST_BUCKET_FLDR
 
        manifestLogger.info(f"{MANIFEST_BUCKET_FLDR=}")
        manifestLogger.info(f"{MANIFEST_HOLD_BUCKET_FLDR=}")
        manifestLogger.info(f"{S3MANIFEST_FOLDER_TO_USE=}")


        ####################################################################
        # Process files 2 Include in manifest File(s)
        ####################################################################
        ProcessFiles2IncludeInManifestFile(S3MANIFEST_FOLDER_TO_USE, ManifestFilename, lstFiles2IncludeInManifest, jiraURL, RecipientEmails)

        
        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        manifestLogger.info("Script CreateManifestFileDriver.py completed successfully.")
        manifestLogger.info(f"\nEnded at {TMSTMP}" )
        
        return 0


    except Exception as e:
        print (f"Exception occured in CreateManifestFileDriver.py\n {e}")

        manifestLogger.error("Exception occured in CreateManifestFileDriver.py.")
        manifestLogger.error(e)
        
        # re-raise error for caller to catch
        raise  


if __name__ == "__main__":
    
    #bucket=None, s3folder=None, runToken=None, BoxEmails=None, Manifest_folder=None, Ext_Type=None
    
    try:    
        parser = argparse.ArgumentParser(description="S3 file combiner")
        parser.add_argument("--bucket", help="base bucket to use")
        parser.add_argument("--s3folder", help="S3 folder whose contents should be combined")
        parser.add_argument("--runToken", help="timestamp that all files to include have")
        parser.add_argument("--BoxEmails", help="output location for resulting merged files, relative to the specified base bucket")
        parser.add_argument("--Manifest_folder", type=int, help="max filesize of the concatenated files in bytes")
        parser.add_argument("--Ext_Type", type=int, help="max filesize of the concatenated files in bytes")
        
        args = parser.parse_args()
        
       
    except Exception as e:
        logging.error("Exception occured in combinedS3Files.py.")
        print(e)

        sys.exit(12) 
    
    createManifestFile(bucket=args.bucket, s3folder=args.s3Folder, runToken=None, BoxEmails=None, Manifest_folder=None, Ext_Type=None)