######################################################################################
# Name: CombineS3FilesDriver.py
# Desc: Combine/concatenate S3 Files into single file.
#
# Execute as python3 CombineS3FilesDriver.py $1 $2 
#
# Ex.1     python3 CombineS3FilesDriver.py "aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod/xtr/DEV/Blbtn/" "blbtn_clm_ext_20260108.093357.txt.gz"             
# Ex.2     python3 CombineS3FilesDriver.py "aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod/xtr/DEV/Blbtn/" "blbtn_clm_ext_20260108.093933.txt.gz"             
#
# $1 = S3 bucket/folder_name  Ex1: bucket=aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod/xtr/DEV/Blbtn/  
#                             Ex2: bucket=aws-hhs-cms-eadg-bia-ddom-extracts/xtr/Blbtn/
# $2 = S3 Combined filename   Ex:  PartB_Carrier_FINAL_2021_QTR1_20220922.084321.csv.gz
#
# Created: Paul Baranoski  09/22/2022
# Modified: 
# 
# Paul Baranoski 2023-06-05 Increase max filesize from 50GB to 60GB. This was done as
#                           quick fix since Josh's FFS extract generated 2 combine groups with file suffixes,
#                           and we don't yet have a solution on how to create EFT files from these files.
# Paul Baranoski 2023-06-07 Set the max filesize back to 50GB.
# Paul Baranoski 2023-06-16 Added timestamp to temp file so that each caller will have unique temp file, 
#                           and prevent overlay of file by another caller.
# Paul Baranoski 2023-09-14 Change logic to delete part files rather than move them to an archive folder. 
# Paul Baranoski 2025-03-28 Modify logic to bypass call to python combineS3Files.py module when NOF_PARTS_FILES = 0.
#                           This was done to avoid critical error in combineS3Files.py which causes parent shell script to fail.
# Paul Baranoski 2026-03-17 Convert bash script to python.
######################################################################################

import boto3 
import logging
import sys
import argparse
import os
import re

#import datetime
from datetime import datetime
from datetime import date,timedelta

# Our import modules
from SET_XTR_ENV import *

import LoggerStandard as EnigmaLog
from CommonFunctions import * 
import combineS3Files as concatS3Files

LOG_DIR = "/app/IDRC/XTR/CMS/logs/"
RUNDIR = "/app/IDRC/XTR/CMS/scripts/run/"


def main_processing_loop():
    pass


def combineS3Files(s3BucketAndFldr=None, s3CombinedFilename=None):


    try:    

        ##########################################
        # Set Timestamp for log file and extract filenames
        ##########################################
        global TMSTMP
        global LOGNAME
        global combineLogger


        #TMSTMP = If TMSTMP value set by caller via export --> use that value. 
        #         Else use the timestamp created in this script  
        TMSTMP = os.getenv("TMSTMP",datetime.now().strftime('%Y%m%d.%H%M%S'))
        os.environ["TMSTMP"] = TMSTMP 
         
        print(f"{TMSTMP=}")

        LOGNAME = f"{LOG_DIR}CombineS3Files_{TMSTMP}.log"
        
        ##########################################
        # Establish log file
        # NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
        ##########################################
        combineLogger = EnigmaLog.setLogging(LOGNAME)

        combineLogger.info("################################### ")
        combineLogger.info(f"\nCombineS3FilesDriver.py started at {TMSTMP} ")

        # Establish logger with CommonFunctions module.
        setCommonFunctionLogger(combineLogger)

        ###########################################################
        # Set working directory to scripts/run directory.
        # This is so subprocess calls will work from RunDeck.  
        ###########################################################
        os.chdir(RUNDIR)
        pwd = os.getcwd()
        combineLogger.info(f"{pwd=}")
       
        #############################################################
        # Display parameters passed to script 
        #############################################################
        combineLogger.info(f"{s3BucketAndFldr=}")  
        combineLogger.info(f"{s3CombinedFilename=}")  

        #############################################################
        # Display S3Bucket value
        #
        # aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod/xtr/DEV/Blbtn/ --> aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod
        # NOTE: We will use XTR_BUCKET variable instead of parsing s3BucketAndFolder
        #############################################################
        s3Bucket = XTR_BUCKET
        
        combineLogger.info("")
        combineLogger.info(f"{s3Bucket=}")
        
        #############################################################
        # Extract the S3 FolderName from the S3BucketAndFldr
        #
        # Ex. aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod/xtr/DEV/Blbtn/ -->  xtr/DEV/Blbtn/
        #############################################################
        s3BktFldr = s3BucketAndFldr.replace(rf"{XTR_BUCKET}/","")
        
        combineLogger.info(f"{s3BktFldr=}")

        ##################################################################
        # FilePrefix --> "I want to concatenate files that match this prefix/"wildcard "
        #
        # bash command --> S3FilePrefix=`echo ${combinedFilename} | cut -d. -f1-2`
        #
        # !!!! NOTE: Filename will look like this: "PartB_Carrier_FINAL_2021_QTR1_20220922.084321.csv.gz"
        #               and we want a file prefix: "PartB_Carrier_FINAL_2021_QTR1_20220922.084321"
        #
        #      Ex.2                                "FEHB_CMS_HHA_20260401_20260630_20260722.txt.gz"   
        #                                          "FEHB_CMS_HHA_20260401_20260630_20260722" 
        #
        # NOTE: I am assuming that all filenames will contain a timestamp at the end
        #       of the filename AND we are not interested in anything after the time 
        #       component in identifying the prefix/search criteria for files to concatenate.
        #
        #################################################################
        # In essence --> we are removing the extension
        objMatch = re.search("^[a-zA-z0-9_-]+\.", s3CombinedFilename)
        
        if objMatch is None:
            combineLogger.error(f"s3CombinedFilename ({s3CombinedFilename}) is not a valid filename where parts files can be combined")        
            raise Exception(f"s3CombinedFilename ({s3CombinedFilename}) is not a valid filename where parts files can be combined")
            
        else: 
            # Remove ending period.
            s3FilenamePrefix = objMatch.group().replace(".","")

        combineLogger.info(f"{s3FilenamePrefix=}")

        #####################################################
        # Set filesize of each concatenation
        # 50 GB
        #####################################################
        #filesize=5368709120  -- 5GB
        #filesize=10737418240 -- 10GB
        #filesize=21474836480 -- 20GB
        #filesize=26843545600 -- 25GB
        sFilesize="53687091200"  
        #filesize=64424509440 -- 60GB


        #############################################################
        # Get S3 references
        #############################################################
        combineLogger.info("")
        combineLogger.info(f"Get s3 Client object")
        
        global s3_client
        s3_resource = boto3.resource('s3')
        s3_client = boto3.client("s3")
        
        
        ##################################################################
        # NOTE: When Extract allows multiple files, there can be one S3 file
        #       that will have suffix entension "_0_0_0" ). So, still want to 
        #       create concatenated file without suffix.          
        #
        # NOTE: Get count of S3 files w/timestamp that  
        #       have suffix (e.g. "_0_0_0." or "_31_0_7.") in filename.
        #       There should be at least one.
        #
        # NOTE: there can be a single "non-parts" file which means we
        #       want to end gracefully since there are no files to concatenate.
        ##################################################################
        combineLogger.info("")
        combineLogger.info(f"Get S3 Parts filenames for {s3FilenamePrefix} ")

        # get list of potential "parts" files. This list may contain a single non-parts file.
        lstS3Keys4Prefix = getS3FileKeysList(s3_resource, s3Bucket, s3BktFldr, s3FilenamePrefix)
        NOF_PARTS_FILES = len(lstS3Keys4Prefix)
        
        if NOF_PARTS_FILES > 0:
            # filter list further to only contain "parts" files ("_00_00_00.") like "00_00_00.txt.gz"
            PartsFilePattern = re.compile("_[0-9]{1,2}_[0-9]{1,2}_[0-9]{1,2}\.")
            lstPartsFileKeys = [f for f in lstS3Keys4Prefix if PartsFilePattern.search(f) ]
            
            NOF_PARTS_FILES = len(lstPartsFileKeys)

        # Display NOF "parts" files.
        combineLogger.info(f"{NOF_PARTS_FILES}")


        ##################################################################
        # ONLY Run python concatenation program when there are parts files
        ##################################################################
        if NOF_PARTS_FILES == 0:
            combineLogger.info(f"NOF S3 PARTS FILES=0. No combining of S3 files is needed. Call to concatenate parts files is bypassed.")

        elif NOF_PARTS_FILES > 0:
            # Delete any residual combined/concatenated file before starting concatenation
            combineLogger.info(f"Deleting S3 file if exists={s3Bucket}/{s3BktFldr}{s3CombinedFilename}")

            # No exception is thrown if file_key does not exist
            s3_client.delete_object(Bucket=s3Bucket, Key=f"{s3BktFldr}{s3CombinedFilename}")
                

            ##################################################################
            # Run python concatenation program
            ##################################################################
            combineLogger.info("")
            combineLogger.info("Start Combining S3 files ")

            combineLogger.info("")
            combineLogger.info("Run python program combineS3Files.py")	

            try:
                #sp_info = subprocess.run(['python3', 'combineS3Files.py', '--bucket', s3Bucket, '--folder', s3BktFldr, '--prefix', s3FilenamePrefix, '--output', f"{s3BktFldr}{s3CombinedFilename}", '--filesize', sFilesize ], capture_output=True, text=True, check=True )
                #write_sp_info_2_log(sp_info)
                
                concatS3Files.setModuleLogger(combineLogger)
                concatS3Files.S3FileConcatenation(bucket=s3Bucket, folder=s3BktFldr, prefix=s3FilenamePrefix, output=f"{s3BktFldr}{s3CombinedFilename}", filesize=sFilesize) 

            except Exception as e:
                combineLogger.error(f"Calling combineS3Files.py failed with error: (e)")

            #except subprocess.CalledProcessError as e:
            #    combineLogger.error(f"Calling combineS3Files.py failed with return code {e.returncode}")
            #    combineLogger.error(e.stdout)
            #    combineLogger.error(e.stderr)

                # re-raise error
                raise

            #############################################################
            # Delete S3 Parts Files 
            #############################################################
            combineLogger.info("") 
            combineLogger.info("Deleting S3 Parts Files.")

            # combinedFilename     --> SAFENC_INP_FINAL_Y22QTR1_20230608.153723.txt.gz
            # parts filenames like --> SAFENC_INP_FINAL_Y22QTR1_20230608.153723.txt.gz_0_0_0.csv.gz

            lstS3Keys2Delete = []
            
            for s3PartsFilenameKey in lstPartsFileKeys:
                #s3_client.delete_object(Bucket=s3Bucket, Key=s3PartsFilenameKey)
                lstS3Keys2Delete.append({'Key': s3PartsFilenameKey})

            # Mass delete of objects. This is faster than deleting one at a time.
            if lstS3Keys2Delete:
                combineLogger.info("s3 Keys to Delete: \n%s\n", "\n".join(lstPartsFileKeys))
                s3_client.delete_objects(Bucket=s3Bucket, Delete={'Objects': lstS3Keys2Delete})


        #############################################################
        # End-if
        #############################################################

       
        ####################################################################
        # End of Processing
        # NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
        ####################################################################          
        # Need these messages for Dashboard
        combineLogger.info("CombineS3FilesDriver.py completed successfully.")
        combineLogger.info(f"\nEnded at {TMSTMP}" )


    except Exception as e:
        print (f"Exception occured in CombineS3FilesDriver.py\n {e}")

        combineLogger.error("Exception occured in CombineS3FilesDriver.py.")
        combineLogger.error("\n%s", str(e))
        
        # Send Failure email	
        SUBJECT=f"CombineS3FilesDriver.py - Failed ({ENVNAME})"
        MSG=str(e)
        sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
        write_sp_info_2_log(sp_info) 

        # re-raise error
        raise
        #sys.exit(12)


if __name__ == "__main__":
    
    main_processing_loop()