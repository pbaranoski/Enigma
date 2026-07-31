############################################################################################################
# Name:  CreateManifestSFTPFileDriver.py
#
# Desc: Create Manifest file required for transfers of Extract files to Outside Consumers using SFTP
#
# Execute:
#        import CreateManifestSFTPFileDriver as CreMan
#        
#        CreMan.createManifestFile(s3folder=$1, sRunTimeStamp=$2, sRecipEmails=$3, ManifestFldr=$4, ManifestFileHLQ=$5, SFTPDestFldr=$6 )     
#
# $1 = S3 folder_name         Ex1: s3folder=xtr/DEV/MNUPAnnual/  
#                             Ex2: s3folder=xtr/MNUPAnnual/
# $2 = sRunTimeStamp --> S3 filename timestamp  Ex:  R240325.T131201   
# $3 = SFTP recipient email addresses (comma delimited)  
# $4 = S3 Destination manifest_files folder (SFTP - manifest_files_ssa_sftp/) or manifest_miles/SSA
# $5 = Manifest Filename HLQ (Beginning Nodes) --> Ex. MNUP_ANNUAL_
# $6 = SFTPDestFldr --> Ex. SSAMNUP
# 
# $1 = Where extract file lives in S3
# $2 = How to indentify all extract files to be included in the manifest file (e.g. all files with same "timestamp")
# $3 = The SFTP recipient email addresses
# $4 = The S3 folder where to place the manifest file
# $5 = The HLQ for the name of the manifest file itself
# $6 = The dataRequestID value in the manifest file. The folder name at the destination.
# 
#
# 2024-30-25 Paul Baranoski   Created script.
# 2026-07-29 Paul Baranoski   Convert CreateManifestSFTPFile.sh from bash to python as CreateManifestSFTPFileDriver.py. 	
############################################################################################################

import boto3 
import os
import json

#import datetime
from datetime import datetime

# Our common module with variable constants
from SET_XTR_ENV import *

import LoggerStandard as EnigmaLog
from CommonFunctions import *

LOG_DIR = "/app/IDRC/XTR/CMS/logs/"
DATADIR = "/app/IDRC/XTR/CMS/data/"


def BuildManifestFile(s3Folder, lstFilenames, sRecipEmails, sOutfilePathAndName, SFTPDestFldr):


    try:    

        # sRecipEmails is a comma-delimited string of email addresses: "pbaranoski@apprioinc.com,jturner@apprioinc.com"
        
        ###############################################
        # Display parameters
        ###############################################
        manifestLogger.info(f"{s3Folder=}")
        manifestLogger.info(f"{lstFilenames=}")
        manifestLogger.info(f"{sRecipEmails=}")
        manifestLogger.info(f"{sOutfilePathAndName=}")
        manifestLogger.info(f"{SFTPDestFldr=}")

        ###############################################
        # Get NOF Files
        ###############################################
        nofFiles = len(lstFilenames)
        manifestLogger.info(f"{nofFiles=}")
        
        ###############################################
        # Get Environment variables DDOM Contact Info 
        ###############################################
        DDOM_CONTACT_NM=os.getenv('DDOM_CONTACT_NM')
        DDOM_CONTACT_PHNE_NUM=os.getenv('DDOM_CONTACT_PHNE_NUM')
        DDOM_CONTACT_EMAIL=os.getenv('DDOM_CONTACT_EMAIL')

        ###############################################
        # Build List of Filename Dictionary items
        ###############################################
        manifestLogger.info("Build S3 Filename Dictionary items")
        lstDictS3Filenames = [{"fileName": sFilename, "fileLocation": s3Folder} for sFilename in lstFilenames] 
        
        manifestLogger.info(lstDictS3Filenames)

        ###############################################
        # Build Share Detail Dictionary
        ###############################################
        dctShareDetails = { "dataRequestID" : SFTPDestFldr,
                            "shareDuration" : "5",
                            "dataRecipientEmails" : sRecipEmails, 
                            "totalNumberOfFiles"  : str(nofFiles),
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
        # Write out manifest json file
        ###############################################
        manifestLogger.info("Convert dictionary to json format")

        json_obj = json.dumps(dctManifest, indent=4)
        manifestLogger.info(json_obj)

        manifestLogger.info("")
        manifestLogger.info("Write manifest file")
        with open(sOutfilePathAndName, "w+") as manFile:
            manFile.writelines(json_obj)


    except Exception as e:
        manifestLogger.error("Exception occured in BuildManifestFile function. {e}")

        # re-raise error
        raise


def createManifestFile(s3Folder=None, sRunTimeStamp=None, sRecipEmails=None, ManifestFldr=None, ManifestFileHLQ=None, SFTPDestFldr=None ):

    try:    

        ##########################################
        # Set Timestamp for log file and extract filenames
        ##########################################
        global TMSTMP
        global LOGNAME
        global manifestLogger


        #TMSTMP = If TMSTMP value set by caller via export --> use that value. 
        #         Else use the timestamp created in this script  
        TMSTMP = os.getenv("TMSTMP",datetime.now().strftime('%Y%m%d.%H%M%S'))
        os.environ["TMSTMP"] = TMSTMP 
         
        print(f"{TMSTMP=}")

        LOGNAME = f"{LOG_DIR}CreateManifestSFTPFile_{TMSTMP}.log"
        
        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        manifestLogger = EnigmaLog.setLogging(LOGNAME)

        manifestLogger.info("################################### ")
        manifestLogger.info(f"\nCreateManifestSFTPFileDriver.py started at {TMSTMP} ")


        #######################################################
        # Get parameters.
        #######################################################
        manifestLogger.info("")
        manifestLogger.info(f"{s3Folder=}")  
        manifestLogger.info(f"{sRunTimeStamp=}")  
        manifestLogger.info(f"{sRecipEmails=}")  
        manifestLogger.info(f"{ManifestFldr=}")  
        manifestLogger.info(f"{ManifestFileHLQ=}")  
        manifestLogger.info(f"{SFTPDestFldr=}")  
        

        #############################################################
        # Retrieve "TESTING" environment variable (Y or N)
        #############################################################
        swTESTING = os.getenv("TESTING","N")
            
        manifestLogger.info("")
        manifestLogger.info(f"Environment variable --> TESTING={swTESTING}")  
        
        # Set Manifest File folder to use based on if in TESTING mode.
        if swTESTING == "Y":
            MANIFEST_FILE_FLDR_2_USE = MANIFEST_HOLD_BUCKET_FLDR
        else:    
            MANIFEST_FILE_FLDR_2_USE = ManifestFldr

        #############################################################
        # Get S3 reference
        #############################################################
        global s3_client
        s3_resource = boto3.resource('s3')
        s3_client = boto3.client("s3")

        #############################################################
        # Get list of S3 files to include in manifest.
        #############################################################
        manifestLogger.info("")
        manifestLogger.info("Get list of S3 files to include in manifest file: ")
        
        lstFileKeys4ManifestFile = getExtFiles4RequestList(s3_resource, XTR_BUCKET, s3Folder, sRunTimeStamp)

        # Get NOF files to include in manifest file
        nofFiles = len(lstFileKeys4ManifestFile)          

        manifestLogger.info(f"{nofFiles=}")
        manifestLogger.info("\n%s","\n".join(lstFileKeys4ManifestFile))


        #############################################################
        # if no S3 files found --> return to caller
        #############################################################
        if nofFiles == 0:
            SUBJECT = f"CreateManifestFileDriver.py - Failed ({ENVNAME})"
            MSG = f"No files found to include in manifest file - failed. "

            try:
                sendEmail(CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG)

            except Exception as e:    
                manifestLogger.error(f"Calling sendEmail.py failed with error: {e}") 

                # Raise exception  
                raise Exception(f"No files found to include in manifest file - failed.")
            

        #############################################################
        # Convert list of keys to list of filenames
        #############################################################
        lstFilenames4ManifestFile = getFilenamesFromS3Keys(lstFileKeys4ManifestFile, s3Folder)


        #############################################################
        # Build manifest filename
        #############################################################
        # Ex. NYSPAP_Manifest_20221006.093854.json
        sManifestFilename = f"{ManifestFileHLQ}_Manifest_{sRunTimeStamp}.json"
        manifestLogger.info(f"{sManifestFilename=}")

        sManifestPathAndFilename = f"{DATADIR}{sManifestFilename}"
 
        #############################################################
        # Build manifest file.
        #############################################################
        manifestLogger.info("")
        manifestLogger.info("Build manifest file")

        BuildManifestFile(s3Folder, lstFilenames4ManifestFile, sRecipEmails, sManifestPathAndFilename, SFTPDestFldr)


        #################################################################################
        # Move manifest file to S3 manifest folder. 
        #################################################################################
        manifestLogger.info("")
        manifestLogger.info(f"Copy manifest file to manifest file folder to use: {XTR_BUCKET}/{MANIFEST_FILE_FLDR_2_USE}")
        
        s3UploadFile(s3_client, sManifestPathAndFilename, XTR_BUCKET, f"{MANIFEST_FILE_FLDR_2_USE}{sManifestFilename}")

	
        #################################################################################
        # Remove manifest file from Linux data directory 
        #################################################################################
        manifestLogger.info("")
        manifestLogger.info("Remove Manifest file from Linux data directory.")

        deleteFileFromLinux(sManifestPathAndFilename)	


        ####################################################################
        # End of Processing
        ####################################################################          
        manifestLogger.info("Script CreateManifestFileDriver.py completed successfully.")
        manifestLogger.info(f"\nEnded at {datetime.now().strftime('%Y%m%d.%H%M%S')}" )
        
        return 0


    except Exception as e:
        manifestLogger.error(f"Exception occured in CreateManifestSFTPFileDriver.py\n {e}")

        # re-raise error for caller to catch
        raise 

if __name__ == "__main__":

    # This module is designed to only be used as an import module.
    pass

