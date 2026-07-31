
import boto3 

# Our common module with variable constants
from SET_XTR_ENV import *

# Our include members
import LoggerStandard as EnigmaLog
from CommonFunctions import *

# functions for encrypting/decrypting files using gpg
import CommonFunctionsGPG as GPGFunctions


DATA_DIR = "/app/IDRC/XTR/CMS/data/"
LOG_DIR = "/app/IDRC/XTR/CMS/logs/"
RUNDIR = "/app/IDRC/XTR/CMS/scripts/run/"

OPMHI_HHA_BUCKET = rf"{XTR_BUCKET}/{OPMHI_HHA_BUCKET_FLDR}"
OPMHI_ENRLMNT_BUCKET = rf"{XTR_BUCKET}/{OPMHI_ENRLMNT_BUCKET_FLDR}"


EXT_FILENAME = "FEHB_CMS_ENR_20260101_20260331_20260615.gz"
#EXT_FILENAME = "FEHB_CMS_HHA_20260101_20260331_20260615.gz"

CUR_DATE = "20260615"

TMSTMP = datetime.now().strftime('%Y%m%d.%H%M%S')
print(f"{TMSTMP=}")
        
LOGNAME = f"{LOG_DIR}TESTING_OPMHI_TEST_GPG_{TMSTMP}.log"

##########################################
# Establish log file
# NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
##########################################
global rootLogger
rootLogger = EnigmaLog.setLogging(LOGNAME)
rootLogger.info(f"\nTESTING_OPMHI_TEST_GPG.py started at {TMSTMP}")

# Establish logger with CommonFunctions module.
setCommonFunctionLogger(rootLogger) 

# Establish logger with CommonFunctionsGPG module.        
GPGFunctions.setCommonFunctionLogger(rootLogger) 



global s3_client
s3_resource = boto3.resource('s3')
s3_client = boto3.client("s3")

        
#############################################################
# GPG encrypt file
#############################################################
rootLogger.info("Get Secret Key. ")
EncryptKey = GPGFunctions.get_secret(OPMHI_CLAIMS_ENCRYPT_KEY_SECRET_NAME, REGION)

rootLogger.info("Import gpg Key. ")
gnupg_home = GPGFunctions.import_gpg_key(EncryptKey)

rootLogger.info("get recipient. ")
recipient = GPGFunctions.get_key_fingerprint(gnupg_home)
rootLogger.info(f"{recipient=}")

ext_fileNPath = f"{DATA_DIR}{EXT_FILENAME}"
gpg_fileNPath = f"{DATA_DIR}{EXT_FILENAME}.gpg"

rootLogger.info("encrypt file ")
GPGFunctions.encrypt_file(gnupg_home, ext_fileNPath, gpg_fileNPath, recipient)
		

#############################################################
# Upload encrypted extract file to S3
#############################################################
#s3UploadFile(s3_client, f"{DATA_DIR}{EXT_FILENAME}.gpg", XTR_BUCKET, f"{OPMHI_HHA_BUCKET_FLDR}{EXT_FILENAME}.gpg")	
s3UploadFile(s3_client, f"{DATA_DIR}{EXT_FILENAME}.gpg", XTR_BUCKET, f"{OPMHI_ENRLMNT_BUCKET_FLDR}{EXT_FILENAME}.gpg")

################################################
# $1 = bucket/folder where file(s) referenced in manifest files are located 
# $2 = timestamp of file(s) to include (how to find file in folder)
# $3 = Manifest file email addresses
# $4 = where to place manifest file (TESTING - hold folder)
# $5 = HLQ of manifest .json filename
# $6 = the dataRequestID = Destination folder name
################################################
MANIFEST_BUCKET_TO_USE = f"{XTR_BUCKET}/{MANIFEST_HOLD_BUCKET_FLDR}"

#MANIFEST_FILE_HLQ = "OPMHI_HHA"
#DEST_FLDR = f"CLAIMS_{TMSTMP[:6]}"
# Ex. CLAIMS_YYYYMM

        
MANIFEST_FILE_HLQ = "OPMHI_ENRLMNT"
DEST_FLDR = "ENRLMENT"
#DEST_FLDR = f"ENRLMENT_{TMSTMP[:6]}"

try:
    #sp_info = subprocess.run(['bash', 'CreateManifestSFTPFile.sh', OPMHI_HHA_BUCKET, CUR_DATE, OPMHI_BOX_RECIPIENT, MANIFEST_BUCKET_TO_USE, MANIFEST_FILE_HLQ, DEST_FLDR ], capture_output=True, text=True, check=True)
    sp_info = subprocess.run(['bash', 'CreateManifestSFTPFile.sh', OPMHI_ENRLMNT_BUCKET, CUR_DATE, OPMHI_BOX_RECIPIENT, MANIFEST_BUCKET_TO_USE, MANIFEST_FILE_HLQ, DEST_FLDR ], capture_output=True, text=True, check=True)

    write_sp_info_2_log(sp_info)
    
except subprocess.CalledProcessError as e:
    rootLogger.error(f"sendEmail.py failed with return code {e.returncode}")
    rootLogger.error("\n%s", e.stdout)
    rootLogger.error("\n%s", e.stderr)

    SUBJECT=f"Create Manifest file in TESTING_OPMHI_TEST_GPG.py - Failed ({ENVNAME})"
    MSG=f"Create Manifest file in TESTING_OPMHI_TEST_GPG.py  has failed."
    
    sp_info = subprocess.run(['python3', 'sendEmail.py', CMS_EMAIL_SENDER, ENIGMA_EMAIL_FAILURE_RECIPIENT, SUBJECT, MSG], capture_output=True, text=True, check=True)
    write_sp_info_2_log(sp_info)

    # re-raise exception
    raise


####################################################################
# End of Processing
# NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
####################################################################          
# Need these messages for Dashboard
rootLogger.info("Script TESTING_OPMHI_TEST_GPG.py completed successfully.")
rootLogger.info(f"\nEnded at {TMSTMP}" )
