######################################################################################
# Name: SET_XTR_ENV_v2.py
##
# Desc: This python module sets environment variables needed by apps and 
#       SF databasename connectivity.
#
#####################################################################################
#  Modification: 
#                                                                                                                       
# 07/09/2025   Paul Baranoski    Created module for "serverless" processing. 
# 07/11/2025   Paul Baranoski    Added Sean's AB prod emails addresses. 
# 07/17/2025   Paul Baranoski    Updated comments. 
# 08/05/2025   Paul Baranoski    Added DDOM to all prod success emails.     
# 08/22/2025   Viren Khanna      Added Medpac Box Recipient
# 09/11/2025   Paul Baranoski    Add DoD_NPI bucket constants. Add DOD_NPI_EMAIL_SUCCESS_RECIPIENT and DOD_NPI_BOX_RECIPIENT Constants.
# 09/12/2025   Viren Khanna      Updated Box Recipient email with Emma's email
# 09/25/2025   Paul Barnaoski    Trying to determine the cause of this error: 
#                                *** Failure under get_secret_as_env : ClientError : An error occurred (AccessDeniedException) 
#                                   when calling the GetSecretValue operation: User: arn:aws:sts::772614087260:assumed-role/idrc-infa-om-etl-ddom-role/AssumeRoleSession 
#                                   is not authorized to perform: secretsmanager:GetSecretValue on resource: idrc/snowflake/bia/idrc_tst_bia_xtr_etl 
#                                   because no identity-based policy allows the secretsmanager:GetSecretValue action
#
#                                This error occurred when running extracts with python driver vs shell script driver script. The only difference is the 
#                                shell script uses SET_XTR_ENV.sh while python driver uses SET_XTR_ENV.py to set environment variables used by snowconvert_helpers python pkg.
#                                When looking at the contents of /app/INFA/Config/setToolsEnv.ksh this module has a different value for 
#                                environment variable IDRC_DATALAKE_AWS_ACCT on each of the 4 DEI v4 servers. Updated code to set the proper value for this variable
#                                depending on environment.  
# 10/28/2025   Paul Baranoski    Remove Josh email from PTD Duals Daily and Monthly SUCCESS Email constant.   
# 10/30/2025   Paul Baranoski    OPMHI_EMAIL_SUCCESS_RECIPIENT to OPMHI_BOX_RECIPIENT.
#                                Change OPMHI_EMAIL_FAILURE_RECIPIENT to ENIGMA_EMAIL_FAILURE_RECIPIENT. 
# 11/21/2025   Paul Baranoski    Add new constant FMR_BOX_RECIPIENT.
# 11/25/2025   Paul Baranoski    Updated list of OFM Box recipients per Mikia Burris email to Monica.
# 12/22/2025   Paul Baranoski    Added NYSPAP Box Recipient.
# 01/14/2026   Paul Baranoski    Added logic to retrieve "TESTING" environment variable and set variable swTESTING to its value if it exists, 
#                                or to default value of "N". Also, set variable "TESTLOG" value based on the swTESTING variable's value which
#                                will be used by extract programs. 
# 01/15/2026   Paul Baranoski    Add TESTEMAIL variable whose verbiage will appear in TESTING emails.
# 01/28/2026   Paul Baranoski    Add Shine as Box recipient to all STS reports.
# 03/26/2026   Paul Baranoski    Remove LINDA.KING@CMS.HHS.GOV from email distros. She's leaving CMS.
# 04/21/2026   Paul Baranoski    Replace BIT_DDOM_PO@cms.hhs.gov with BIT_DDOM_PO@cms.hhs.gov.
# 05/12/2026   Paul Baranoski    Add Kenneth.Wilkins@cms.hhs.gov to PART_AB_EMAIL_SUCCESS_RECIPIENT.
# 05/18/2026   Paul Baranoski    Add GPG encrypt/decrypt constants for OPMHI.
#####################################################################################

import os
import sys
import platform

##########################################
# Determine environment based on hostname
##########################################
#uname -a | awk '{print $2}' | cut -c1
 
hostname = platform.node()
print(f"{hostname=}")

ENV_IND = hostname[:1]
if ENV_IND == 'd':
    ENVNAME = "DEV"
elif ENV_IND == 't':    
    ENVNAME = "TST"    
elif ENV_IND == 'i':    
    ENVNAME = "IMPL"   
elif ENV_IND == 'p':    
    ENVNAME = "PRD"  
else:    
    ENVNAME = "UNK" 
    
print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
print(f"Current Environment is : {ENVNAME}")
print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")    

os.environ["ENVNAME"] = ENVNAME


#########################################################################
# Rest of SET_XTR_ENV.sh
#########################################################################
if  ENVNAME == 'DEV' or  ENVNAME == 'TST' or ENVNAME == 'IMPL' or  ENVNAME == 'PRD':
    if  ENVNAME == 'DEV' or ENVNAME == 'TST' or ENVNAME == 'IMPL': 
        SF_ETL_WHSE = "NP"
    else:
        SF_ETL_WHSE = "P"
   
else:
    print("Environment should be either DEV or TST or IMPL or PRD !")
    sys.exit(12)

# Snowflake warehouse variables
os.environ["sf_etl_warehouse"] = f"BIA_{SF_ETL_WHSE}_ETL_WKLD"
os.environ["sf_xtr_warehouse"] = f"BIA_{SF_ETL_WHSE}_XTR_WKLD"

#os.environ["CMN_UTIL"] = "/app/IDRC/COMMON/CMS/scripts/util"
os.environ["CMN_UTIL"] = "/app/IDRC/XTR/CMS/scripts/run"

######################################
#  Configures Additional Software   #
######################################
if  ENVNAME == 'DEV':
    os.environ["IDRC_DATALAKE_AWS_ACCT"] = "772614087260"
elif ENVNAME == 'TST':
    os.environ["IDRC_DATALAKE_AWS_ACCT"] = "232722229861"
elif ENVNAME == 'IMPL':
    os.environ["IDRC_DATALAKE_AWS_ACCT"] = "291492156955"
elif ENVNAME == 'PRD':
    os.environ["IDRC_DATALAKE_AWS_ACCT"] = "148550782651"
 

# Python Interpreter
PYTHON_COMMAND = "python3"
os.environ["PYTHON_COMMAND"] = "python3"

	
#######################################################
# SET Bucket and SMTP env variables
#######################################################
if ENVNAME == 'PRD':
    #os.environ["bucket=aws-cms-oit-bit-ddom-extracts/xtr/
    SMTP_SERVER = "cloud-smtp-prod.bitaws.local"
    XTR_BUCKET  = "aws-hhs-cms-eadg-bia-ddom-extracts"
    bucket_fldr = f"xtr/"
    
else:
    #os.environ["bucket"] = f"aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod/xtr/{ENVNAME}/"
    SMTP_SERVER = "cloud-smtp-nonprod.bitaws.local"
    XTR_BUCKET  = "aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod"
    bucket_fldr = f"xtr/{ENVNAME}/"
   
os.environ["SMTP_SERVER"] = SMTP_SERVER
 
# Define Bucket Folder Constants for Boto calls
ASC_PTB_BUCKET_FLDR = f"{bucket_fldr}ASC_PTB/"
BLBTN_BUCKET_FLDR = f"{bucket_fldr}Blbtn/"
CALENDAR_BUCKET_FLDR = f"{bucket_fldr}CALENDAR/"
CONFIG_BUCKET_FLDR = f"{bucket_fldr}config/"

DASHBOARD_BUCKET_FLDR = f"{bucket_fldr}Dashboard/"
DEMO_FNDR_BUCKET_FLDR = f"{bucket_fldr}DemoFndr/"
DDOM_BUCKET_FLDR = f"{bucket_fldr}DDOM/"
DOD_NPI_BUCKET_FLDR = f"{bucket_fldr}DoD_NPI/"
DOJ_BUCKET_FLDR = f"{bucket_fldr}DOJ/"
DSH_BUCKET_FLDR = f"{bucket_fldr}DSH/"
DUALS_MedAdv_BUCKET_FLDR = f"{bucket_fldr}DUALS_MedAdv/"

EFT_FILEST_BUCKET_FLDR = f"{bucket_fldr}EFT_Files/"
FINDER_FILE_BUCKET_FLDR = f"{bucket_fldr}Finder_Files/"
FINDER_FILE_SSA_BUCKET_FLDR = f"{bucket_fldr}Finder_Files_SSA/"
FMR_BUCKET_FLDR = f"{bucket_fldr}FMR/"
FOIA_BUCKET_FLDR = f"{bucket_fldr}FOIA/"

GITHUB_BUCKET_FLDR = f"{bucket_fldr}GITHUB/"

HCPP_BUCKET_FLDR = f"{bucket_fldr}HCPP/"
HOS_BUCKET_FLDR = f"{bucket_fldr}HOS/"

MANIFEST_BUCKET_FLDR = f"{bucket_fldr}manifest_files/"
MANIFEST_ARCHIVE_BUCKET_FLDR = f"{bucket_fldr}manifest_files_archive/"
MANIFEST_HOLD_BUCKET_FLDR = f"{bucket_fldr}manifest_files_hold/"
MANIFEST_SSA_BUCKET_FLDR = f"{bucket_fldr}manifest_files/SSA/"
MANIFEST_VA_MAC_BUCKET_FLDR = f"{bucket_fldr}manifest_files/VA/MAC/"
MANIFEST_VA_PBM_BUCKET_FLDR = f"{bucket_fldr}manifest_files/VA/PBM/"

MEDPAC_BUCKET_FLDR = f"{bucket_fldr}MEDPAC_HOS/"
MEDPAR_BAYSTATE_BUCKET_FLDR = f"{bucket_fldr}MEDPAR_BAYSTATE/"
MNUP_BUCKET_FLDR = f"{bucket_fldr}MNUPAnnual/"
MNUP_MONTHLY_BUCKET_FLDR = f"{bucket_fldr}MNUPMonthly/"
NYSPAP_BUCKET_FLDR = f"{bucket_fldr}NYSPAP/"	

OFM_PDE_BUCKET_FLDR = f"{bucket_fldr}OFM_PDE/"
OPMHI_HHA_BUCKET_FLDR = f"{bucket_fldr}OPMHI_HHA/"
OPMHI_SNF_BUCKET_FLDR = f"{bucket_fldr}OPMHI_SNF/"
OPMHI_INP_BUCKET_FLDR = f"{bucket_fldr}OPMHI_INP/"
OPMHI_OPT_BUCKET_FLDR = f"{bucket_fldr}OPMHI_OPT/"
OPMHI_HSP_BUCKET_FLDR = f"{bucket_fldr}OPMHI_HSP/"
OPMHI_CAR_BUCKET_FLDR = f"{bucket_fldr}OPMHI_CAR/"
OPMHI_DME_BUCKET_FLDR = f"{bucket_fldr}OPMHI_DME/"
OPMHI_ENRLMNT_BUCKET_FLDR = f"{bucket_fldr}OPMHI_ENRLMNT/"
OPMHI_PDE_BUCKET_FLDR = f"{bucket_fldr}OPMHI_PDE/"

PAC_BUCKET_FLDR = f"{bucket_fldr}PAC/"	
PARTAB_BUCKET_FLDR = f"{bucket_fldr}PartAB_Extract/"	
PECOS_BUCKET_FLDR = f"{bucket_fldr}PECOS/"
PHYZIP_BUCKET_FLDR = f"{bucket_fldr}PHYZIP/"
PSA_BUCKET_FLDR = f"{bucket_fldr}PSA/"
PSPS_BUCKET_FLDR = f"{bucket_fldr}PSPS/"
PSPSNPI_BUCKET_FLDR = f"{bucket_fldr}PSPS_NPI/"

PTB_CARR_BUCKET_FLDR = f"{bucket_fldr}PTBCarrier/"
PTDDUALMNTH_BUCKET_FLDR = f"{bucket_fldr}PTDDualMnth/"	
PTDDUALDAILY_BUCKET_FLDR = f"{bucket_fldr}PTDDualDaily/"
PTDDUALHIST_BUCKET_FLDR = f"{bucket_fldr}PTDDualHstr/"

RAND_FFSPTAB_BUCKET_FLDR = f"{bucket_fldr}RAND_FFSPTAB/"
RAND_PDE_BUCKET_FLDR = f"{bucket_fldr}RAND_PDE/"

SEER_BUCKET_FLDR = f"{bucket_fldr}SEER/"

SFTP_BUCKET_FLDR = f"{bucket_fldr}SFTP_Files/"
SFTP_FOLDER = "SFTP_Files/"
SRTR_ENC_BUCKET_FLDR = f"{bucket_fldr}SRTR_ENCPTAB/"
SRTR_FFS_BUCKET_FLDR = f"{bucket_fldr}SRTR_FFSPTAB/"
SRTR_PDE_BUCKET_FLDR = f"{bucket_fldr}SRTR_PDE/"
SRTR_ENRLMNT_BUCKET_FLDR = f"{bucket_fldr}SRTR_ENRLMNT/"

STS_HHA_BUCKET_FLDR = f"{bucket_fldr}STS_HHA/"
STS_HHA_REV_CTR_BUCKET_FLDR = f"{bucket_fldr}STS_HHA_REV_CTR/"
STS_MED_INS_BUCKET_FLDR = f"{bucket_fldr}STS_MED_INS/"
STS_MED_INS_MN_BUCKET_FLDR = f"{bucket_fldr}STS_MED_INS_MN/"
STS_SNF_BUCKET_FLDR = f"{bucket_fldr}STS_SNF/"
STS_PTA_BPYMTS_BUCKET_FLDR = f"{bucket_fldr}STS_PTA_BPYMTS/"
STS_PTA_BPYMTS_MN_BUCKET_FLDR = f"{bucket_fldr}STS_PTA_BPYMTS_MN/"
STS_HHA_FACILITY_BUCKET_FLDR = f"{bucket_fldr}STS_HHA_FACILITY/"
STS_HOS_FACILITY_BUCKET_FLDR = f"{bucket_fldr}STS_HOS_FACILITY/"


SAF_PDE_BUCKET_FLDR = f"{bucket_fldr}SAF_PDE/"
SAFENC_HHA_BUCKET_FLDR = f"{bucket_fldr}SAFENC_HHA/"
SAFENC_SNF_BUCKET_FLDR = f"{bucket_fldr}SAFENC_SNF/"
SAFENC_INP_BUCKET_FLDR = f"{bucket_fldr}SAFENC_INP/"
SAFENC_OPT_BUCKET_FLDR = f"{bucket_fldr}SAFENC_OPT/"
SAFENC_CAR_BUCKET_FLDR = f"{bucket_fldr}SAFENC_CAR/"
SAFENC_DME_BUCKET_FLDR = f"{bucket_fldr}SAFENC_DME/"

TRICARE_BUCKET_FLDR = f"{bucket_fldr}TRICARE/"

VAPTD_BUCKET_FLDR = f"{bucket_fldr}VA_PTD/"
VARTN_BUCKET_FLDR = f"{bucket_fldr}VA_RTRN/"


############################################
# DDOM manifest file info
############################################
os.environ["DDOM_CONTACT_NM"] = "Edward Belle"
os.environ["DDOM_CONTACT_PHNE_NUM"] = "443-764-4548"
os.environ["DDOM_CONTACT_EMAIL"] = "edward.belle@cms.hhs.gov"


########################################################################
# To support conversion from bash to python TESTING in production.
########################################################################
swTESTING = os.getenv("TESTING","N") 

if swTESTING == 'Y':
    print("TESTING")
    TESTLOG = "TESTING_"
    TESTEMAIL = "-TESTING"
else:
    print("NOT TESTING")
    TESTLOG = ""
    TESTEMAIL = ""


########################################################################
# gpg enctyption/decryption Constants
########################################################################
if ENVNAME == 'DEV' or ENVNAME == 'TST' or ENVNAME == 'IMPL':

    # for gpg enctyption/decryption
    OPMHI_ENCRYPT_KEY_SECRET_NAME  = "np-opm-extract-public-key"
    OPMHI_ENROLL_ENCRYPT_KEY_SECRET_NAME = "np-opm-extract-public-key"
    OPMHI_DECRYPT_KEY_SECRET_NAME  = "np-opm-private-key"
    OPMHI_DECRYPT_PASSPHRASE = "gpgprivatekey04222026"
    REGION = "us-east-1"

else:
    
    # for gpg enctyption/decryption
    OPMHI_ENCRYPT_KEY_SECRET_NAME = "np-opm-extract-public-key"
    OPMHI_ENROLL_ENCRYPT_KEY_SECRET_NAME = "np-opm-extract-public-key"
    OPMHI_DECRYPT_KEY_SECRET_NAME  = "np-opm-private-key"
    OPMHI_DECRYPT_PASSPHRASE = "gpgprivatekey04222026"
    REGION = "us-east-1"
    
    
########################################################################
# Email Receipients
########################################################################
CMS_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"

if ENVNAME == 'DEV' or ENVNAME == 'TST' or ENVNAME == 'IMPL' or swTESTING == "Y":

    ASC_PTB_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    BLBTN_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    CALENDAR_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"

    DSH_EMAIL_SUCCESS_RECIPIENT = "bit-extractsupport@index-analytics.com"
    DSH_EMAIL_FAILURE_RECIPIENT = "bit-extractsupport@index-analytics.com"
    DSH_EMAIL_BCC = "bit-extractalerts@index-analytics.com"
    DSH_EMAIL_REPLY_MSG = "Note: Send inquiries to dshquestions@cms.hhs.gov "
    DSH_BOX_RECIPIENT = "bit-extractsupport@index-analytics.com"

    DASHBOARD_RPT_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"

    DEMO_FINDER_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"

    DOD_NPI_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    DOD_NPI_BOX_RECIPIENT = "bit-extractalerts@index-analytics.com"
    
    DOJ_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    DUALMEDADV_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"

    EFT_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"

    ENIGMA_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    ENIGMA_EMAIL_FAILURE_RECIPIENT = "bit-extractalerts@index-analytics.com"
    
    FMR_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    FMR_BOX_RECIPIENT = "bit-extractalerts@index-analytics.com"
    HCPP_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"

    HOS_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    HOS_EMAIL_BOX_RECIPIENT = "bit-extractalerts@index-analytics.com"

    MEDPAC_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    MEDPAR_BAYSTATE_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    MEDPAC_EMAIL_BOX_RECIPIENT = "bit-extractalerts@index-analytics.com"

    MNUP_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    MNUP_EMAIL_BOX_RECIPIENT = "jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov,Daniel.Lee2@cms.hhs.gov,olga.yablonovsky@ssa.gov"
    
    NYSPAP_EMAIL_SENDER = "BIA_SUPPORT@cms.hhs.gov"
    NYSPAP_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    NYSPAP_BOX_RECIPIENT = "bit-extractalerts@index-analytics.com"	

    OFM_PDE_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    
    OFM_PDE_BLAND_BOX_RECIPIENT = "bit-extractalerts@index-analytics.com"
    OFM_PDE_CGI_BOX_RECIPIENT = "bit-extractalerts@index-analytics.com"
    OFM_PDE_MHM_BOX_RECIPIENT = "bit-extractalerts@index-analytics.com"	
    OFM_PDE_DJLLC_BOX_RECIPIENT = "bit-extractalerts@index-analytics.com"	
    OFM_PDE_CONRAD_BOX_RECIPIENT = "bit-extractalerts@index-analytics.com"	
    OFM_PDE_UNKNOWN_BOX_RECIPIENT = "bit-extractalerts@index-analytics.com"	

    OPMHI_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    OPMHI_BOX_RECIPIENT = "bit-extractalerts@index-analytics.com"
    
    OPMHI_HIST_EMAIL_SUCCESS_RECIPIENT = "jturner-con@index-analytics.com"
    PAC_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"

    PART_AB_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"

    PARTB_CARRIER_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    PECOS_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"

    PHYZIP_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    PHYZIP_BOX_RECIPIENTS = "bit-extractalerts@index-analytics.com"

    PSA_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    PSPS_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    PSPS_HCPCS_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"

    PSPS_SUPPRESSION_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    PSPSNPI_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    PTDDUALMNTH_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    PTDDUALDAILY_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"

    RAND_FFS_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    RAND_FFS_BOX_RECIPIENTS = "bit-extractalerts@index-analytics.com"

    RAND_PDE_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    SAF_PDE_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"

    SAFENC_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    SAFENC_CAR_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"

    SEER_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    SEER_BOX_RECIPIENTS = "bit-extractalerts@index-analytics.com"
    SEER_EMAIL_BCC = "bit-extractalerts@index-analytics.com"	

    SRTR_FNDR_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    SRTR_ENC_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    SRTR_FFS_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    SRTR_PDE_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    SRTR_ENRLMNT_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"

    STS_HHA_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    STS_HHA_BOX_RECIPIENT = "bit-extractalerts@index-analytics.com"

    STS_HHA_REV_CTR_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    STS_HHA_REV_CTR_BOX_RECIPIENT = "bit-extractalerts@index-analytics.com"

    STS_MED_INS_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    STS_MED_INS_BOX_RECIPIENT = "bit-extractalerts@index-analytics.com"

    STS_MED_INS_MN_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    STS_MED_INS_MN_BOX_RECIPIENT = "bit-extractalerts@index-analytics.com"

    STS_SNF_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    STS_SNF_BOX_RECIPIENT = "bit-extractalerts@index-analytics.com"

    STS_PTA_BPYMTS_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    STS_PTA_BPYMTS_BOX_RECIPIENT = "bit-extractalerts@index-analytics.com"

    STS_PTA_BPYMTS_MN_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    STS_PTA_BPYMTS_MN_BOX_RECIPIENT = "bit-extractalerts@index-analytics.com"

    STS_HHA_FACILITY_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    STS_HHA_FACILITY_BOX_RECIPIENT = "bit-extractalerts@index-analytics.com"

    STS_HOS_FACILITY_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    STS_HOS_FACILITY_BOX_RECIPIENT = "bit-extractalerts@index-analytics.com"

    TRICARE_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"

    VAPTD_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    VAPTD_EMAIL_BOX_RECIPIENT = "bit-extractalerts@index-analytics.com"	

    VARTN_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    VARTN_EMAIL_BOX_RECIPIENT = "bit-extractalerts@index-analytics.com"
    #
    #
else:

    ASC_PTB_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"
    BLBTN_EMAIL_SUCCESS_RECIPIENT = "IDR_SOURCES@CMS.HHS.GOV,JEFF.BYRNES@REVELANTTECH.COM,DONOVAN.WADDEL@CMS.HHS.GOV,BIT_DDOM_PO@cms.hhs.gov,bit-extractsupport@index-analytics.com"
    CALENDAR_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"

    DASHBOARD_RPT_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"

    DSH_EMAIL_SUCCESS_RECIPIENT = "dshquestions@cms.hhs.gov"
    DSH_EMAIL_FAILURE_RECIPIENT = "dshquestions@cms.hhs.gov"
    DSH_EMAIL_BCC = "bit-extractalerts@index-analytics.com"
    DSH_EMAIL_REPLY_MSG = "Note: Send inquiries to dshquestions@cms.hhs.gov "
    DSH_BOX_RECIPIENT = "jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov"

    DEMO_FINDER_EMAIL_SUCCESS_RECIPIENT = "BIT_DDOM_PO@cms.hhs.gov,bit-extractsupport@index-analytics.com"
    DOD_NPI_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"
    DOD_NPI_BOX_RECIPIENT = "jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov"
    DOJ_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    DUALMEDADV_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"

    ENIGMA_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    ENIGMA_EMAIL_FAILURE_RECIPIENT = "bit-extractalerts@index-analytics.com"

    EFT_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    FMR_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.govv"
    FMR_BOX_RECIPIENT = "sukgoo.pak@palmettogba.com,nicholas.landry@palmettogba.com,dumbiri.stone@palmettogba.com,jagadeeshwar.pagidimarri@cms.hhs.gov,monica.algozer@cms.hhs.gov"

    HCPP_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"

    HOS_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"
    HOS_EMAIL_BOX_RECIPIENT = "jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,kimberly.demichele@cms.hhs.gov,alyssa.rosen@cms.hhs.gov,esjackson@rti.org,akandilov@rti.org,aakinseye@rti.org"

    MEDPAC_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"
    MEDPAR_BAYSTATE_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"
    MEDPAC_EMAIL_BOX_RECIPIENT = "monica.algozer@cms.hhs.gov,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,ssiford@acumenllc.com"

    MNUP_EMAIL_SUCCESS_RECIPIENT = "ssa.mnup.support@ssa.gov,BIT_DDOM_PO@cms.hhs.gov,bit-extractalerts@index-analytics.com"
    MNUP_EMAIL_BOX_RECIPIENT = "jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,Daniel.Lee2@cms.hhs.gov,olga.yablonovsky@ssa.gov,ssa.mnup.support@ssa.gov"

    NYSPAP_EMAIL_SUCCESS_RECIPIENT = "Robert.Palumbo@Primetherapeutics.com,bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"
    NYSPAP_BOX_RECIPIENT = "Robert.Palumbo@Primetherapeutics.com,monica.algozer@cms.hhs.gov,jagadeeshwar.pagidimarri@cms.hhs.gov"	
    
    OFM_PDE_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"

    ################################################################################################################
	#   Bland (CONT-2025-71859)
	#	  Katie Brabec (kbrabec@blandgovconsulting.com)
	#	  Courtney Hearn (chearn@blandgovconsulting.com) 
	#	  Alexander Abboud (aabboud@blandgovconsulting.com)
    # 
	#	Myers (CONT-2025-71863) (CGI)
	#	    Keith Sorensen (ksorensen@mslc.com) 
	#		Stephanie Gutcher (sgutcher@mslc.com) 
    #		Morgan Coughlin (mcoughlin@mslc.com) 
	#
	#   Davis Farr (CONT-2025-71862) (MHM)
    #	    Marc Davis (mdavis@davisfarr.com)
	#
    #   David-James (CONT-2025-71861) (DJLLC)
	#	    Michelle McConkey (michelle.mcconkey@djllc.com)
	#	    Christine Fleming (Christine.fleming@djllc.com) 
	#
	# Conrad contacts: Mattison Cano, mcano@conradllp.com, Sam Perera, sperera@conradllp.com)
    ################################################################################################################# 
    OFM_PDE_BLAND_BOX_RECIPIENT = "KBrabec@blandgovconsulting.com,chearn@blandgovconsulting.com,aabboud@blandgovconsulting.com,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov"
    OFM_PDE_CGI_BOX_RECIPIENT = "KSorensen@mslc.com,sgutcher@mslc.com,mcoughlin@mslc.com,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov"
    OFM_PDE_MHM_BOX_RECIPIENT = "MDavis@DavisFarr.com,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov"	
    OFM_PDE_DJLLC_BOX_RECIPIENT = "Michelle.McConkey@djllc.com,Christine.Fleming@djllc.com,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov"	
    OFM_PDE_CONRAD_BOX_RECIPIENT = "mcano@conradllp.com,sperera@conradllp.com,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov"	
    OFM_PDE_UNKNOWN_BOX_RECIPIENT = "jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov"	


    OPMHI_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,Joseph.Stewart@opm.gov,BIT_DDOM_PO@cms.hhs.gov"
    OPMHI_BOX_RECIPIENT = "Joseph.Stewart@opm.gov,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov"

    OPMHI_HIST_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"

    PAC_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"

    PART_AB_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov,Kenneth.Wilkins@cms.hhs.gov,Angela.Huynh@cms.hhs.gov,Floyd.Epps@cms.hhs.gov,Robert.Fox@cms.hhs.gov"

    #PARTB_CARRIER_EMAIL_SUCCESS_RECIPIENT = "GIFT.TEE@CMS.HHS.GOV,CHARLES.CAMPBELL@CMS.HHS.GOV,LARRY.CHAN@CMS.HHS.GOV,MICHAEL.SORACOE@CMS.HHS.GOV,REBECCA.ZELLER@CMS.HHS.GOV,BIT_DDOM_PO@cms.hhs.gov,bit-extractsupport@index-analytics.com"
    PARTB_CARRIER_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"

    PECOS_EMAIL_SUCCESS_RECIPIENT = "PRAVEEN.BOBBASANI@CGIFEDERAL.COM,BIT_DDOM_PO@cms.hhs.gov,bit-extractsupport@index-analytics.com"

    PHYZIP_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,Rebecca.Zeller@cms.hhs.gov,BIT_DDOM_PO@cms.hhs.gov"
    PHYZIP_BOX_RECIPIENTS = "Rebecca.Zeller@cms.hhs.gov,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov"

    PSA_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"

    PSPS_EMAIL_SUCCESS_RECIPIENT = "GIFT.TEE@CMS.HHS.GOV,CHARLES.CAMPBELL@CMS.HHS.GOV,MICHAEL.SORACOE@CMS.HHS.GOV,REBECCA.ZELLER@CMS.HHS.GOV,BIT_DDOM_PO@cms.hhs.gov,bit-extractsupport@index-analytics.com"
    PSPS_HCPCS_EMAIL_SUCCESS_RECIPIENT = "BIT_DDOM_PO@cms.hhs.gov,bit-extractsupport@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov,KEVIN.HODGES2@CMS.HHS.GOV"
    PSPS_SUPPRESSION_EMAIL_SUCCESS_RECIPIENT = "BIT_DDOM_PO@cms.hhs.gov,bit-extractsupport@index-analytics.com"

    PSPSNPI_SUCCESS_RECIPIENT = "bit-extractsupport@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"

    PTDDUALMNTH_EMAIL_SUCCESS_RECIPIENT = "Nicole.Perry@cms.hhs.gov,SDRC@ACUMENLLC.COM,BIT_DDOM_PO@cms.hhs.gov,bit-extractsupport@index-analytics.com"
    PTDDUALDAILY_EMAIL_SUCCESS_RECIPIENT = "Nicole.Perry@cms.hhs.gov,SDRC@ACUMENLLC.COM,BIT_DDOM_PO@cms.hhs.gov,bit-extractsupport@index-analytics.com"

    RAND_FFS_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"
    RAND_FFS_BOX_RECIPIENTS = "jdaly@rand.org,jlai@rand.org,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov"

    RAND_PDE_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"
    SAF_PDE_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"

    SAFENC_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"
    SAFENC_CAR_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"

    SEER_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"
    SEER_BOX_RECIPIENTS = "jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov"
    SEER_EMAIL_BCC = "bit-extractalerts@index-analytics.com"	

    SRTR_FNDR_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    SRTR_ENC_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    SRTR_ENRLMNT_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    SRTR_FFS_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    SRTR_PDE_EMAIL_SUCCESS_RECIPIENT = "it-extractalerts@index-analytics.com"

    STS_HHA_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"
    STS_HHA_BOX_RECIPIENT = "shine.jacob@cms.hhs.gov,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"

    STS_HHA_REV_CTR_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"
    STS_HHA_REV_CTR_BOX_RECIPIENT = "shine.jacob@cms.hhs.gov,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"

    STS_MED_INS_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"
    STS_MED_INS_BOX_RECIPIENT = "shine.jacob@cms.hhs.gov,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"

    STS_MED_INS_MN_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov,michelle.wilson@state.mn.us,kelsey.kannenberg@state.mn.us"
    STS_MED_INS_MN_BOX_RECIPIENT = "shine.jacob@cms.hhs.gov,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov,michelle.wilson@state.mn.us,kelsey.kannenberg@state.mn.us"

    STS_SNF_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"
    STS_SNF_BOX_RECIPIENT = "shine.jacob@cms.hhs.gov,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"

    STS_PTA_BPYMTS_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"
    STS_PTA_BPYMTS_BOX_RECIPIENT = "shine.jacob@cms.hhs.gov,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"

    STS_PTA_BPYMTS_MN_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov,michelle.wilson@state.mn.us,kelsey.kannenberg@state.mn.us"
    STS_PTA_BPYMTS_MN_BOX_RECIPIENT = "shine.jacob@cms.hhs.gov,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov,michelle.wilson@state.mn.us,kelsey.kannenberg@state.mn.us"

    STS_HHA_FACILITY_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"
    STS_HHA_FACILITY_BOX_RECIPIENT = "shine.jacob@cms.hhs.gov,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"

    STS_HOS_FACILITY_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"
    STS_HOS_FACILITY_BOX_RECIPIENT = "shine.jacob@cms.hhs.gov,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"


    TRICARE_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"

    VAPTD_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"
    VAPTD_EMAIL_BOX_RECIPIENT = "jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov,bit-extractalerts@index-analytics.com"

    VARTN_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"
    VARTN_EMAIL_BOX_RECIPIENT = "Monir.Hossain@va.gov"


