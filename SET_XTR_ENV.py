######################################################################################
# Name: SET_XTR_ENV.py
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
# 08/05/2025   Paul Baranoski       Added DDOM to all prod success emails.     
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
    INFA_ENV = "DEV"
elif ENV_IND == 't':    
    INFA_ENV = "TST"    
elif ENV_IND == 'i':    
    INFA_ENV = "IMPL"   
elif ENV_IND == 'p':    
    INFA_ENV = "PRD"  
else:    
    INFA_ENV = "UNK" 
print(f"{INFA_ENV=}")    
    
##########################################
# Env variables set from 2 source includes
##########################################
#source /app/INFA/Config/setToolsEnv.ksh
#source /app/INFA/Logon/idrc_xtr.logon

##########################################
# Configures System environment #
##########################################
os.environ["LC_ALL"] = "en_US.UTF-8"
os.environ["LANG"] = "en_US.UTF-8"
os.environ["NLS_LANG"] = "AMERICAN_AMERICA.AL32UTF8"

#########################################
#  Configures INFORMATICA environment   #
#########################################
##INFA_ENV = "DEV"
INFA_HOME = "/informatica/DEI/current"
DEI_HOME = "/informatica/DEI/current"
INFA_DEFAULT_DOMAIN = f"dom_DEI_{INFA_ENV}"
INFA_MRS = f"mrs_DEI_{INFA_ENV}"
INFA_DEI_MREP = INFA_MRS
INFA_DIS = f"dis_DEI_{INFA_ENV}"
JAVA_HOME = f"{INFA_HOME}/java"
JRE_HOME = f"{JAVA_HOME}/jre"
ODBCHOME = f"{INFA_HOME}/odbc"
ODBCINI = f"{ODBCHOME}/odbc.ini"
ACJVMCommandLineOptions = "-XX:GCTimeRatio=9 -Xmx1024M -XX:+HeapDumpOnOutOfMemoryError"

os.environ["INFA_ENV"] = INFA_ENV
os.environ["INFA_HOME"] = INFA_HOME
os.environ["DEI_HOME"] = DEI_HOME
os.environ["INFA_DEFAULT_DOMAIN"] = INFA_DEFAULT_DOMAIN
os.environ["INFA_MRS"] = INFA_MRS
os.environ["INFA_DEI_MREP"] = INFA_DEI_MREP
os.environ["INFA_DIS"] = INFA_DIS
os.environ["JAVA_HOME"] = JAVA_HOME
os.environ["JRE_HOME"] = JRE_HOME
os.environ["ODBCHOME"] = ODBCHOME
os.environ["ODBCINI"] = ODBCINI
os.environ["ACJVMCommandLineOptions"] = ACJVMCommandLineOptions

######################################
#   Configures ORACLE environment    #
######################################
ORACLE_HOME = "/oracle/product/19.3.0/client_1"
ORACLE_LIB = f"{ORACLE_HOME}/lib"

os.environ["ORACLE_HOME"] = ORACLE_HOME
os.environ["ORACLE_TERM"] = "xterm"
os.environ["ORACLE_LIB"] = ORACLE_LIB
os.environ["TNS_ADMIN"] = f"{ORACLE_HOME}network/admin"

######################################
#  Configures Additional Software   #
######################################

#AWS CONFIG#
os.environ["AWS_CONFIG_FILE"] = "/informatica/DEI/current/services/shared/hadoop/EMR_5.29/conf/aws.default"
os.environ["IDRC_DATALAKE_AWS_ACCT"] = "772614087260"


######################
# PATH Configuration #
######################
PATH = "{ODBCHOME}/bin:{ORACLE_HOME}/bin"
PATH += ":/usr/bin:/usr/local/bin:/usr/sbin:/usr/local/sbin:/usr/lib:/etc"
 
# Software Additions
PATH += f":{JAVA_HOME}/bin:{JRE_HOME}/bin:{INFA_HOME}/server:{INFA_HOME}/server/bin:{INFA_HOME}/tomcat/bin:{INFA_HOME}/services/shared/bin:{INFA_HOME}/isp/bin:{INFA_HOME}/externaljdbcjars"

#Python Virtual Environment#
#source /app/INFA/Config/pythonvenv/venvdev/bin/activate

VIRTUAL_ENV="/projects/INFA/Config/pythonvenv/venvdev"
os.environ["VIRTUAL_ENV"] = VIRTUAL_ENV

_OLD_VIRTUAL_PATH = "${PATH}"

PATH = f"{VIRTUAL_ENV}/bin:{PATH}"

os.environ["PATH"] = PATH

#################################
# LD_LIBRARY_PATH Configuration #
#################################
LD_LIBRARY_PATH = f"{ODBCHOME}/lib"
LD_LIBRARY_PATH += ":/usr/lib:/usr/local/lib:/usr/lib64:/usr/local/lib64"

# Software Additions
LD_LIBRARY_PATH += ":{ORACLE_LIB}:{ORACLE_HOME}/network/lib:{ORACLE_HOME}/jdbc/lib:{INFA_HOME}/server/bin:{INFA_HOME}/services/shared/bin:{ODBCHOME}"
LD_LIBRARY_PATH += ":{ODBCHOME}/lib:{ODBCHOME}/bin:{INFA_HOME}/isp/bin"

os.environ["LD_LIBRARY_PATH"] = LD_LIBRARY_PATH

# vt100 default set by JASS
os.environ["TERM"] = "vt100"


#########################################################################
# Rest of SET_XTR_ENV.sh
#########################################################################
ENVNAME = INFA_ENV
os.environ["ENVNAME"] = ENVNAME

if  ENVNAME == 'DEV' or  ENVNAME == 'TST' or ENVNAME == 'IMPL' or  ENVNAME == 'PRD':
    if  ENVNAME == 'DEV' or ENVNAME == 'TST' or ENVNAME == 'IMPL': 
        SF_ETL_WHSE = "NP"
    else:
        SF_ETL_WHSE = "P"
   
else:
    print("Environment should be either DEV or TST or IMPL or PRD !")
    sys.exit(12)

print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
print(f"Current Environment is : {ENVNAME}")
print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
#os.environ["ENVNAME_LOWER=$(echo $ENVNAME | tr "[:upper:]" "[:lower:]")
#os.environ["ENV_CHAR=$(echo $ENVNAME | cut -c1)

# Snowflake warehouse variables
os.environ["sf_etl_warehouse"] = f"BIA_{SF_ETL_WHSE}_ETL_WKLD"
os.environ["sf_xtr_warehouse"] = f"BIA_{SF_ETL_WHSE}_XTR_WKLD"


os.environ["CMN_UTIL"] = "/app/IDRC/COMMON/CMS/scripts/util"
os.environ["COMMON_UTIL_PATH"] = "/app/IDRC/COMMON/CMS/scripts/util"
os.environ["CMN_RUN"] = "/app/IDRC/COMMON/CMS/scripts/run"
os.environ["CMN_SOURCE"] = "/app/IDRC/COMMON/CMS/scripts/source"


ENVPATH = "/app/IDRC/XTR/CMS"
os.environ["ENVPATH"] = ENVPATH
os.environ["SCRIPTS"] = f"{ENVPATH}/scripts"
os.environ["UTIL"] = f"{ENVPATH}/scripts/util"
os.environ["SOURCE"] = f"{ENVPATH}/scripts/source"
os.environ["DDL"] = f"{ENVPATH}/scripts/ddl"
os.environ["RUN"] = f"{ENVPATH}/scripts/run"
os.environ["LOGONPATH"] = f"{ENVPATH}/scripts/logon"
os.environ["MAINLOG"] = f"{ENVPATH}/logs"
os.environ["LOG_PATH"] = f"{ENVPATH}/logs"
os.environ["CURRENT_LOG"] = f"{ENVPATH}/logs/current"


# Python Interpreter
PYTHON_COMMAND = "python3"
os.environ["PYTHON_COMMAND"] = "python3"

########################################################################
# Snowflake Parameters
########################################################################
os.environ["IDRC_DB"] = f"IDRC_{ENVNAME}"

os.environ["BIA_DB"] = f"BIA_{ENVNAME}"

os.environ["TEMP_SCHEMA"] = f"CMS_ETLTEMP_COMM_{ENVNAME}"
os.environ["LOG_SCHEMA"] = f"CMS_LOG_XTR_{ENVNAME}"

os.environ["QT_SCHEMA"] = f"CMS_QT_COMM_{ENVNAME}"
os.environ["SP_META_SCHEMA"] = f"CMS_LOG_XTR_{ENVNAME}"
os.environ["SP_SCHEMA"] = f"CMS_SP_COMM_{ENVNAME}"
os.environ["UTIL_SCHEMA"] = f"CMS_CMN_UTLTY_{ENVNAME}"
#

		
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
# Email Receipients
########################################################################
CMS_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"

if ENVNAME == 'DEV' or ENVNAME == 'TST' or ENVNAME == 'IMPL':

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
    DOJ_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    DUALMEDADV_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"

    EFT_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"

    ENIGMA_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    ENIGMA_EMAIL_FAILURE_RECIPIENT = "bit-extractalerts@index-analytics.com"
    
    FMR_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    HCPP_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"

    HOS_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    HOS_EMAIL_BOX_RECIPIENT = "bit-extractalerts@index-analytics.com"

    MEDPAC_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    MEDPAR_BAYSTATE_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"

    MNUP_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    MNUP_EMAIL_BOX_RECIPIENT = "jagadeeshwar.pagidimarri@cms.hhs.gov,monica.algozer@cms.hhs.gov,Daniel.Lee2@cms.hhs.gov,olga.yablonovsky@ssa.gov"
    
    NYSPAP_EMAIL_SENDER = "BIA_SUPPORT@cms.hhs.gov"
    NYSPAP_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"

    OFM_PDE_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    
    OFM_PDE_BLAND_BOX_RECIPIENT = "bit-extractalerts@index-analytics.com"
    OFM_PDE_CGI_BOX_RECIPIENT = "bit-extractalerts@index-analytics.com"
    OFM_PDE_MHM_BOX_RECIPIENT = "bit-extractalerts@index-analytics.com"	
    OFM_PDE_DJLLC_BOX_RECIPIENT = "bit-extractalerts@index-analytics.com"	
    OFM_PDE_CONRAD_BOX_RECIPIENT = "bit-extractalerts@index-analytics.com"	
    OFM_PDE_UNKNOWN_BOX_RECIPIENT = "bit-extractalerts@index-analytics.com"	

    OPMHI_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
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

    ASC_PTB_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,ddom-businessowners@index-analytics.com"
    BLBTN_EMAIL_SUCCESS_RECIPIENT = "IDR_SOURCES@CMS.HHS.GOV,JEFF.BYRNES@REVELANTTECH.COM,DONOVAN.WADDEL@CMS.HHS.GOV,ddom-businessowners@index-analytics.com,bit-extractsupport@index-analytics.com"
    CALENDAR_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,ddom-businessowners@index-analytics.com"

    DASHBOARD_RPT_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,ddom-businessowners@index-analytics.com"

    DSH_EMAIL_SUCCESS_RECIPIENT = "dshquestions@cms.hhs.gov"
    DSH_EMAIL_FAILURE_RECIPIENT = "dshquestions@cms.hhs.gov"
    DSH_EMAIL_BCC = "bit-extractalerts@index-analytics.com"
    DSH_EMAIL_REPLY_MSG = "Note: Send inquiries to dshquestions@cms.hhs.gov "
    DSH_BOX_RECIPIENT = "jagadeeshwar.pagidimarri@cms.hhs.gov,monica.algozer@cms.hhs.gov"

    DEMO_FINDER_EMAIL_SUCCESS_RECIPIENT = "ddom-businessowners@index-analytics.com,bit-extractsupport@index-analytics.com"
    DOJ_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    DUALMEDADV_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"

    ENIGMA_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    ENIGMA_EMAIL_FAILURE_RECIPIENT = "bit-extractalerts@index-analytics.com"

    EFT_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    FMR_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,ddom-businessowners@index-analytics.comv"
    HCPP_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,ddom-businessowners@index-analytics.com"

    HOS_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,ddom-businessowners@index-analytics.com"
    HOS_EMAIL_BOX_RECIPIENT = "jagadeeshwar.pagidimarri@cms.hhs.gov,kimberly.demichele@cms.hhs.gov,alyssa.rosen@cms.hhs.gov,esjackson@rti.org,akandilov@rti.org,aakinseye@rti.org"

    MEDPAC_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,ddom-businessowners@index-analytics.com"
    MEDPAR_BAYSTATE_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,ddom-businessowners@index-analytics.com"

    MNUP_EMAIL_SUCCESS_RECIPIENT = "ssa.mnup.support@ssa.gov,ddom-businessowners@index-analytics.com,bit-extractalerts@index-analytics.com"
    MNUP_EMAIL_BOX_RECIPIENT = "jagadeeshwar.pagidimarri@cms.hhs.gov,Daniel.Lee2@cms.hhs.gov,olga.yablonovsky@ssa.gov,ssa.mnup.support@ssa.gov"

    NYSPAP_EMAIL_SUCCESS_RECIPIENT = "Robert.Palumbo@Primetherapeutics.com,bit-extractalerts@index-analytics.com,ddom-businessowners@index-analytics.com"

    OFM_PDE_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,ddom-businessowners@index-analytics.com"

    ################################################################################################################
    # Conrad contacts: Mattison Cano, mcano@conradllp.com, Sam Perera, sperera@conradllp.com)
    #
    # MCO Audits
    #	Bland – Hilary Allen HAllen@blandgovconsulting.com, Tina Sturm TSturm@blandgovconsulting.com
    #	David James (DJLLC) – Michelle McConkey Michelle.McConkey@djllc.com, Christine Fleming Christine.Fleming@djllc.com
    #	Davis Farr (MHM) – Tritia Foster TFoster@DavisFarr.com, Marc Davis MDavis@DavisFarr.com
    #
    # OFA
    #	Bland – Katie Brabec KBrabec@blandgovconsulting.com
    #	Myers and Stauffer (CGI) – Keith Sorensen KSorensen@mslc.com, Stephanie Ruggeri SRuggeri@mslc.com
    #	Davis Farr (MHM) – Tritia Foster TFoster@DavisFarr.com, Marc Davis MDavis@DavisFarr.com
    #	David James (DJLLC) – Michelle McConkey Michelle.McConkey@djllc.com, Christine Fleming Christine.Fleming@djllc.com
    ################################################################################################################# 
    OFM_PDE_BLAND_BOX_RECIPIENT = "KBrabec@blandgovconsulting.com,HAllen@blandgovconsulting.com,TSturm@blandgovconsulting.com,jagadeeshwar.pagidimarri@cms.hhs.gov,monica.algozer@cms.hhs.gov"
    OFM_PDE_CGI_BOX_RECIPIENT = "KSorensen@mslc.com,SRuggeri@mslc.com,jagadeeshwar.pagidimarri@cms.hhs.gov,monica.algozer@cms.hhs.gov"
    OFM_PDE_MHM_BOX_RECIPIENT = "TFoster@DavisFarr.com,MDavis@DavisFarr.com,jagadeeshwar.pagidimarri@cms.hhs.gov,monica.algozer@cms.hhs.gov"	
    OFM_PDE_DJLLC_BOX_RECIPIENT = "Michelle.McConkey@djllc.com,Christine.Fleming@djllc.com,jagadeeshwar.pagidimarri@cms.hhs.gov,monica.algozer@cms.hhs.gov"	
    OFM_PDE_CONRAD_BOX_RECIPIENT = "mcano@conradllp.com,sperera@conradllp.com,jagadeeshwar.pagidimarri@cms.hhs.gov,monica.algozer@cms.hhs.gov"	
    OFM_PDE_UNKNOWN_BOX_RECIPIENT = "jagadeeshwar.pagidimarri@cms.hhs.gov,monica.algozer@cms.hhs.gov"	

    OPMHI_EMAIL_SUCCESS_RECIPIENT = "Joseph.Stewart@opm.gov,bit-extractalerts@index-analytics.com,ddom-businessowners@index-analytics.com"

    OPMHI_HIST_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"

    PAC_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"

    PART_AB_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,ddom-businessowners@index-analytics.com,Angela.Huynh@cms.hhs.gov,Floyd.Epps@cms.hhs.gov,Robert.Fox@cms.hhs.gov"

    #PARTB_CARRIER_EMAIL_SUCCESS_RECIPIENT = "GIFT.TEE@CMS.HHS.GOV,CHARLES.CAMPBELL@CMS.HHS.GOV,LARRY.CHAN@CMS.HHS.GOV,MICHAEL.SORACOE@CMS.HHS.GOV,REBECCA.ZELLER@CMS.HHS.GOV,ddom-businessowners@index-analytics.com,bit-extractsupport@index-analytics.com"
    PARTB_CARRIER_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,ddom-businessowners@index-analytics.com"

    PECOS_EMAIL_SUCCESS_RECIPIENT = "PRAVEEN.BOBBASANI@CGIFEDERAL.COM,ddom-businessowners@index-analytics.com,bit-extractsupport@index-analytics.com"

    PHYZIP_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,Rebecca.Zeller@cms.hhs.gov,ddom-businessowners@index-analytics.com"
    PHYZIP_BOX_RECIPIENTS = "Rebecca.Zeller@cms.hhs.gov,jagadeeshwar.pagidimarri@cms.hhs.gov,monica.algozer@cms.hhs.gov"

    PSA_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,ddom-businessowners@index-analytics.com"

    PSPS_EMAIL_SUCCESS_RECIPIENT = "GIFT.TEE@CMS.HHS.GOV,CHARLES.CAMPBELL@CMS.HHS.GOV,MICHAEL.SORACOE@CMS.HHS.GOV,REBECCA.ZELLER@CMS.HHS.GOV,ddom-businessowners@index-analytics.com,bit-extractsupport@index-analytics.com"
    PSPS_HCPCS_EMAIL_SUCCESS_RECIPIENT = "ddom-businessowners@index-analytics.com,bit-extractsupport@index-analytics.com,ddom-businessowners@index-analytics.com,KEVIN.HODGES2@CMS.HHS.GOV"
    PSPS_SUPPRESSION_EMAIL_SUCCESS_RECIPIENT = "ddom-businessowners@index-analytics.com,bit-extractsupport@index-analytics.com"

    PSPSNPI_SUCCESS_RECIPIENT = "bit-extractsupport@index-analytics.com,ddom-businessowners@index-analytics.com"

    PTDDUALMNTH_EMAIL_SUCCESS_RECIPIENT = "Nicole.Perry@cms.hhs.gov,LINDA.KING@CMS.HHS.GOV,SDRC@ACUMENLLC.COM,ddom-businessowners@index-analytics.com,bit-extractsupport@index-analytics.com,pbaranoski-con@index-analytics.com,jturner-con@index-analytics.com"
    PTDDUALDAILY_EMAIL_SUCCESS_RECIPIENT = "Nicole.Perry@cms.hhs.gov,LINDA.KING@CMS.HHS.GOV,SDRC@ACUMENLLC.COM,ddom-businessowners@index-analytics.com,bit-extractsupport@index-analytics.com,pbaranoski-con@index-analytics.com,jturner-con@index-analytics.com"

    RAND_FFS_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,ddom-businessowners@index-analytics.com"
    RAND_FFS_BOX_RECIPIENTS = "jdaly@rand.org,jlai@rand.org,Jagadeeshwar.Pagidimarri@cms.hhs.gov"

    RAND_PDE_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,ddom-businessowners@index-analytics.com"
    SAF_PDE_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,ddom-businessowners@index-analytics.com"

    SAFENC_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,ddom-businessowners@index-analytics.com"
    SAFENC_CAR_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,ddom-businessowners@index-analytics.com"

    SEER_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,ddom-businessowners@index-analytics.com"
    SEER_BOX_RECIPIENTS = "jagadeeshwar.pagidimarri@cms.hhs.gov,monica.algozer@cms.hhs.gov"
    SEER_EMAIL_BCC = "bit-extractalerts@index-analytics.com"	

    SRTR_FNDR_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    SRTR_ENC_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    SRTR_ENRLMNT_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    SRTR_FFS_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com"
    SRTR_PDE_EMAIL_SUCCESS_RECIPIENT = "it-extractalerts@index-analytics.com"

    STS_HHA_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,ddom-businessowners@index-analytics.com,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"
    STS_HHA_BOX_RECIPIENT = "jagadeeshwar.pagidimarri@cms.hhs.gov,monica.algozer@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"

    STS_HHA_REV_CTR_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,ddom-businessowners@index-analytics.com,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"
    STS_HHA_REV_CTR_BOX_RECIPIENT = "jagadeeshwar.pagidimarri@cms.hhs.gov,monica.algozer@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"

    STS_MED_INS_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,ddom-businessowners@index-analytics.com,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"
    STS_MED_INS_BOX_RECIPIENT = "jagadeeshwar.pagidimarri@cms.hhs.gov,monica.algozer@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"

    STS_MED_INS_MN_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,ddom-businessowners@index-analytics.com,michelle.wilson@state.mn.us,kelsey.kannenberg@state.mn.us"
    STS_MED_INS_MN_BOX_RECIPIENT = "jagadeeshwar.pagidimarri@cms.hhs.gov,monica.algozer@cms.hhs.gov,michelle.wilson@state.mn.us,kelsey.kannenberg@state.mn.us"

    STS_SNF_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,ddom-businessowners@index-analytics.com,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"
    STS_SNF_BOX_RECIPIENT = "jagadeeshwar.pagidimarri@cms.hhs.gov,monica.algozer@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"

    STS_PTA_BPYMTS_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,ddom-businessowners@index-analytics.com,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"
    STS_PTA_BPYMTS_BOX_RECIPIENT = "jagadeeshwar.pagidimarri@cms.hhs.gov,monica.algozer@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"

    STS_PTA_BPYMTS_MN_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,ddom-businessowners@index-analytics.com,michelle.wilson@state.mn.us,kelsey.kannenberg@state.mn.us"
    STS_PTA_BPYMTS_MN_BOX_RECIPIENT = "jagadeeshwar.pagidimarri@cms.hhs.gov,monica.algozer@cms.hhs.gov,michelle.wilson@state.mn.us,kelsey.kannenberg@state.mn.us"

    STS_HHA_FACILITY_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,ddom-businessowners@index-analytics.com,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"
    STS_HHA_FACILITY_BOX_RECIPIENT = "jagadeeshwar.pagidimarri@cms.hhs.gov,monica.algozer@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"

    STS_HOS_FACILITY_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,ddom-businessowners@index-analytics.com,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"
    STS_HOS_FACILITY_BOX_RECIPIENT = "jagadeeshwar.pagidimarri@cms.hhs.gov,monica.algozer@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"


    TRICARE_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,ddom-businessowners@index-analytics.com"

    VAPTD_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,ddom-businessowners@index-analytics.com"
    VAPTD_EMAIL_BOX_RECIPIENT = "jagadeeshwar.pagidimarri@cms.hhs.gov,monica.algozer@cms.hhs.gov,bit-extractalerts@index-analytics.com"

    VARTN_EMAIL_SUCCESS_RECIPIENT = "bit-extractalerts@index-analytics.com,ddom-businessowners@index-analytics.com"
    VARTN_EMAIL_BOX_RECIPIENT = "Monir.Hossain@va.gov"


