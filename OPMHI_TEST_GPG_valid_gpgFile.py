
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


encrypted_file = "FEHB_CMS_CAR_20260401_20260630_20260714.txt.gz.gpg"
#encrypted_file = "FEHB_CMS_DME_20260401_20260630_20260714.txt.gz.gpg"

CUR_DATE = "20260615"

TMSTMP = datetime.now().strftime('%Y%m%d.%H%M%S')
print(f"{TMSTMP=}")
        
LOGNAME = f"{LOG_DIR}TESTING_OPMHI_TEST_GPG_ValidGPGFile_{TMSTMP}.log"

##########################################
# Establish log file
# NOTE: the \n before "started at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it        
##########################################
global rootLogger
rootLogger = EnigmaLog.setLogging(LOGNAME)
rootLogger.info(f"\nTESTING_OPMHI_TEST_GPG_validGPGFile.py started at {TMSTMP}")

# Establish logger with CommonFunctions module.
setCommonFunctionLogger(rootLogger) 

# Establish logger with CommonFunctionsGPG module.        
GPGFunctions.setCommonFunctionLogger(rootLogger) 

        
#############################################################
# GPG encrypt file
#############################################################
rootLogger.info("Get Secret Key. ")
EncryptKey = GPGFunctions.get_secret(OPMHI_CLAIMS_ENCRYPT_KEY_SECRET_NAME, REGION)

rootLogger.info("Import gpg Key. ")
gnupg_home = GPGFunctions.import_gpg_key(EncryptKey)

rootLogger.info("List packets. ")
GPGFunctions.list_packets(gnupg_home, encrypted_file)
   



####################################################################
# End of Processing
# NOTE: the \n before "Ended at" line is to ensure that this information is on a separate line, left-justified without any other logging info preceding it.        
####################################################################          
# Need these messages for Dashboard
rootLogger.info("Script TESTING_OPMHI_TEST_GPG_validGPGFile.py completed successfully.")
rootLogger.info(f"\nEnded at {TMSTMP}" )
