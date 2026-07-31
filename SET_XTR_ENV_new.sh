#!/usr/bin/sh
######################################################################################
# Name: SET_XTR_ENV.sh
##
# Desc: This script sets UNIX variables like databasename variables
#
# Created:      D Lieske    01-21-2012
#####################################################################################
#Modification:                                                                                                                        
# 01/04/2021   Sumathi Gayam        Changed for cloud migration       
# 03/25/2022   Paul Baranoski       Modified If statement for verifying the $ENVNAME variable.
##                                   Added code set SF_ETL_WHSE variable.
# 06/16/2022   Paul Baranoski       Added email recipients/sender variables. 
# 08/26/2022   Paul Baranoski       Commented out production email addresses and replaced with team members
#                                   until we actually go-live.   
# 09/02/2022   Paul Baranoski       Added new Extract S3 Bucket names. Added email variables for DEMO FINDER.                                                           
# 10/05/2022   Paul Baranoski       Added new S3 Bucket and Email addresses for NYSPAP.
# 11/01/2022   Paul Baranoski       Added DDOM info.
# 11/17/2022   Paul Baranoski       Add the PAC extract buckets and email addresses.
# 11/23/2022   Paul Baranoski       Add the MNUP extract buckets and email addresses.
# 12/02/2022   Paul Baranoski       Add CJ to NYSPAP email recipients for nonprd and prd.
# 12/06/2022   Paul Baranoski       Add new S3 Bucket and Email addresses for PTDDUALMNTH.
# 12/14/2022   Paul Baranoski       Update DDOM_CONTACT_PHNE_NUM. Modify NYSPAP_EMAIL_SUCCESS_RECIPIENT.
# 12/19/2022   Paul Baranoski       Update PSPS_EMAIL_SUCCESS_RECIPIENT to include Paul Lafornara and CJ's 
#                                   Email addresses for creating user documentation.
# 12/20/2022   Joshua Turner        Added VA Part D bucket and email addresses.   
# 12/21/2022   Sumathi Gayam        Defined PSPSNPI SUCCESS and FAILURE RECIPIENTS. 
# 12/22/2022   Paul Baranoski       Add Part D Daily Bucket and email addresses. 
# 01/05/2023   Paul Baranoski       Add Part D History Bucket.            
# 01/12/2023   Paul Baranoski       Modify Part D Dual History Bucket value  
# 01/13/2023   Paul Baranoski       Trying to solve a mystery.
# 01/18/2023   Joshua Turner        Add VA Return File variables    
# 01/18/2023   Viren Khanna	        Add FMR File Variables	  
# 01/23/2023   Paul Baranoski       Add ASC PTB Email and bucket values. 
# 01/30/2023   Joshua Turner        Add MEDPAC HOSPICE email and bucket values.  
# 02/06/2023   Paul Baranoski       Add HCPP email and bucket values. 
# 02/09/2023   Paul Baranoski       Add SRTR Encounter email and bucket values. 
# 02/15/2023   Paul Baranoski       Add SRTR FNDR File email values         
# 02/15/2023   Viren Khanna         Add SRTR PDE email and bucket values. 
# 02/27/2023   Sumathi Gayam        Add SRTR Enrollment email and bucket values.
# 03/02/2023   Joshua Turner        Add RAND FFS PTAB email and bucket values.  
# 03/06/2023   Viren Khanna         Add RAND PDE email and bucket values.  
# 03/08/2023   Paul Baranoski       Add S3 bucket/folder value for config folder.
# 03/24/2023   Paul Baranoski       Add S3 bucket/folder for OFM and HCPP
# 03/28/2023   Paul Baranoski       Removed duplicated HCCP_EMAIL entries
# 03/28/2023   Joshua Turner        Add S3 and email values for HOS
# 04/10/2023   Viren Khanna         Add SAF PDE email and bucket values. 
# 04/13/2023   Paul Baranoski       Add EFT email entries.
# 05/09/2023   Paul Baranoski       Update NYSPAP Production EMAIL recipients.
# 05/31/2023   Paul Baranoski       Add SAF Encounter email and bucket values.
# 06/01/2023   Joshua Turner        Add OPMHI email and bucket values
# 07/27/2023   Paul Baranoski       Updated PartD Duals production success email addresses.
# 07/28/2023   Paul Baranoski       Updated BlueButton production success email addresses. 
# 08/02/2023   Paul Baranoski       Remove Sumathi apprio email address.
#                                   Update prod email addresses for PECOS,PTB Carrier,ASC-PTB. 
# 08/21/2023   Paul Baranoski       Update apprio email addresses to index.  
# 08/24/2023   Paul Baranoski       Update OPMHI email addresses, specifically production for Box test with client.
# 08/30/2023   Paul Baranoski       Update PECOS and BlueButton production Success emails.
# 09/12/2023   Paul Baranoski       Add Tricare S3 bucket and email constants.
# 09/19/2023   Paul Baranoski       Change BIA_SUPPORT@cms.hhs.gov to bit-extractsupport@index-analytics.com per CJ.
#                                   Put extract email constants in alphabetical order to make it easier to find them.
#                                   Replace personal team emails with bit-extractalerts@index-analytics.com.
# 09/22/2023   Paul Baranoski       PSPS Email addresses. Comment out and replace with me, Josh, and Viren (for now).
# 09/22/2023   Paul Baranoski       Added bit-extractalerts@index-analytics.com back for EMAIL constants.
# 09/27/2023   Paul Baranoski       Add production email addresses for MNUP Annual.
# 09/27/2023   Joshua Turner        Adding OPM HI Part D bucket and email 
# 09/27/2023   Paul Baranoski       Fix export statements: "exprot OPMHI_HIST_EMAIL_SENDER" for DEV and PRD. 
# 10/05/2023   Joshua Turner        Adding MEDPBAR Bay State bucket and email vars
# 10/10/2023   Paul Baranoski       Add Manifest and Manifest Hold bucket constants.
# 10/17/2023   Paul Baranoski       Modified production OPMHI Email constant to include Jag (not sure how he disappeared).
# 10/20/2023   Paul Baranoski       Uncomment out PSPS production email constants.
# 10/26/2023   Joshua Turner        Added a new BOX recipient list for the HOS extract
# 11/06/2023   Paul Baranoski       Add new DOJ S3 bucket and DOJ email constants.
# 11/09/2023   Joshua Turner        Added a new BOX email recipient list for VA Return
# 11/28/2023   Paul Baranoski       Add export MANIFEST_SSA_BUCKET and MANIFEST_VA_BUCKET constants.
#                                   Add VAPTD_EMAIL_BOX_RECIPIENT constant.
# 12/04/2023   Paul Baranoski       Remove personal email address for PECOS/PROD email success constant.
# 12/07/2023   Paul Baranoski       Add PSA extract constants.
# 12/22/2023   Paul Baranoski       Add DDOM email constants to be used for manifest file report
#                                   Add MANIFEST_ARCHIVE_BUCKET constant 
# 01/10/2024   Paul Baranoski       Add/Modify manifest_files SSA and VA bucket constants 
# 01/16/2024   Paul Baranoski       Rename constant SFTP_FILES to SSA_RESP_BUCKET.
# 01/31/2024   Viren Khanna         Update end user Robert Palumbo's email address
# 02/12/2024   Paul Baranoski       Add Constants for Calendar process.
# 03/07/2024   Joshua Turner        Updated NYSPAP recipient email
# 03/12/2024   Paul Baranoski       Update RAND FFS email recipient to be bit-extractalerts@index-analytics.com.
#                                   Add RAND_FFS_BOX_RECIPIENTS constant.  
# 03/22/2024   Paul Baranoski       Add Finder_Files_SSA bucket/folder.
# 03/25/2024   Paul Baranoski       Change SSA_RESP Bucket folder to SFTP_Files folder.
#                                   Add MNUP BOX/SFTP recipients.
# 04/04/2024   Paul Baranoski       Add ssa.mnup.support@ssa.gov as MNUP email address recipient. Sent to me by Tim Schickner.
# 04/05/2024   Paul Baranoski       Add S3 Dashboard bucket-folder constant.
# 04/23/2024   Paul Baranoski       Add DSH bucket constant. Added DDOM_BOX_RECIPIENT contants.
# 04/25/2024   Paul Baranoski       Remove BIT_DDOM_PO@cms.hhs.gov from non-Prod DDOM Email/Box recipients.
# 05/03/2024   Joshua Turner        Updated success email for NYSPAP to Robert.Palumbo@Primetherapeutics.com.
# 05/29/2024   Paul Baranoski       Add FOIA bucket.
# 06/05/2024   Paul Baranoski       Add CALENDAR email constants.
# 06/11/2024   Paul Baranoski       Change DDOM_EMAIL constants to DSH_EMAIL.
# 07/01/2024   Paul Baranoski       Modify DSH email constants to use dshquestions@cms.hhs.gov instead of individual email addresses.
# 07/22/2024   Paul Baranoski       Remove Larry Chan email for PSPS requests.
# 08/02/2024   Paul Baranoski       Add OFM_PDE Contractor/Mailbox constants.
# 08/12/2024   Paul Baranoski       Add STS constants.
# 09/12/2024   Paul Baranoski       Remove ASC_PTB_EMAIL_SENDER and PSPS_SUPPRESSION_EMAIL_SENDER.
# 09/12/2024   Paul Baranoski       Update STS EMail constants for Prod.
# 09/16/2024   Viren Khanna         Add MNUP Monthly bucket, added Monica to MNUP Box for DEV
# 09/17/2024   Paul Baranoski       Add SEER Bucket, EMAIL and Box recipients
# 09/20/2024   Paul Baranoski       Add STS_MED_INS_MN Bucket constant.
# 09/20/2024   Sean Whitelock	    Removed Jag and other email of Box recipients for VARTN. Only keeping Monir Hossain 
# 09/23/2024   Sean Whitelock       Added Jag and Monica to the Box Recipients for VARTN.
# 09/24/2024   Sean Whitelock       Reverted VARTN back to only include Monir Hossain on Box Recipients.
# 12/02/2024   Paul Baranoski       Removed FMR_EMAIL_SENDER. FMR script will use CMS_EMAIL_SENDER going forward.
#                                   Add BIT_DDOM_PO@cms.hhs.gov as FMR_EMAIL_SUCCESS_RECIPIENT.
# 12/19/2024   Paul Baranoski       Remove SHEBA.COBLE@CMS.HHS.GOV from Blue Button success email per Keli Chung. 
# 12/23/2024   Paul Baranoski       Add this line to re-migrate code due to "SSM agent on Jenkins server" was down.
# 12/31/2024   Paul Baranoski       Add new bucket STS_PTA_BPYMTS_BUCKET and EMAIL constants for new STS PTA report. 
# 01/09/2025   Paul Baranoski       Add DASHBOARD_RPT Email constants.
# 01/24/2025   Paul Baranoski       Add DUALS_MedAdv constants. 
# 01/29/2025   Paul Baranoski       Remove TRICARE_EMAIL_SENDER,TRICARE_EMAIL_FAILURE_RECIPIENT,PECOS_EMAIL_SENDER,PECOS_EMAIL_FAILURE_RECIPIENT.
# 02/06/2025   Nat.Tinovsky	        Update emails for HOS
# 02/12/2025   Viren Khanna         Add new bucket STS_HOS_FACILITY, STS_HHA_FACILITY and EMAIL constants for new STS reports.
# 02/19/2025   Paul Baranoski       Add new bucket STS_HHA_REV_CTR and STS_HHA_REV_CTR EMAIL constants.
# 04/04/2025   Paul Baranoski       Add PHYZIP S3 bucket, EMAIL and BOX Constants. 
# 04/11/2025   Paul Baranoski       Modify constants used for manifest files. Change Karen Allen info to Ed Belle.
# 05/09/2025   Paul Baranoski       Replace CANDACE.ANDERSON@cms.hhs.gov with Nicole Perry (Nicole.Perry@cms.hhs.gov) on Part D Duals Daily and Monthly.
# 05/20/2025   Paul Baranoski       Added logic to determine if we are executing on a v3 or v4 DEI linux server. Also, added code to set the SMTP server constant
#                                   appropriately based on which linux server we are executing on (v3 or v4). The new SMTP constant will be used by in sendEmail.py 
#                                   and sendEmailHTML.py programs.
# 05/29/2025   Paul Baranoski       Added GITHUB bucket for migrating code.
# 05/29/2025   Paul Baranoski       Add code to set S3 Bucket for v4 if we are on a v4 server. Export DEI_V4_SERVER_SW to be used by downstream scripts and python code.
# 06/06/2025   Sean Whitelock		Add PARTAB_Extract S3 bucket and PARTAB_EMAIL_SUCCESS_RECIPIENT.
# 06/10/2025   Sean Whitelock		Capitalized 'BUCKET' for the 'PARTAB_BUCKET'
# 06/23/2025   Sean Whitelock		Added Angela Huynh, Robert Fox, and Floyd Epps for Part AB Extract recipients for testing.
# 07/10/2025   Sean Whitelock		Changed the Part AB Extract recipients for DEV, TST and IMPL and moved the end users to the PROD recipients.
# 08/05/2025   Paul Baranoski       Added DDOM to all prod success emails.
# 08/22/2025   Viren Khanna         Added Medpac Box Recipient.
# 09/03/2025   Viren Khanna         Updated OFM PDE Box Recipient
# 09/11/2025   Paul Baranoski       Add DoD_NPI bucket constants. Add DOD_NPI_EMAIL_SUCCESS_RECIPIENT and DOD_NPI_BOX_RECIPIENT Constants.
# 09/12/2025   Viren Khanna         Updated Box Recipient email with Emma's email
# 10/29/2025   Paul Baranoski       Update email constant OPMHI_EMAIL_SUCCESS_RECIPIENT to include all DDOM and all of Enigma.
# 10/30/2025   Paul Baranoski       Remove OPMHI_EMAIL_SENDER and OPMHI_EMAIL_FAILURE_RECIPIENT. 
#                                   Add OPMHI_BOX_RECIPIENT.
# 11/13/2025   Viren Khanna         Updated NYSPAP Box Recipient
# 11/21/2025   Paul Baranoski       Add new constant FMR_BOX_RECIPIENT.
# 11/25/2025   Paul Baranoski       Updated list of OFM Box recipients per Mikia Burris email to Monica.
# 01/15/2026   Paul Baranoski       Added logic to retrieve "TESTING" environment variable and set variable swTESTING to its value if it exists, 
#                                   or to default value of "N". Also, create environment variable TESTLOG="TESTING_SH_" and TESTEMAIL="TESTING". 
# 01/28/2026   Paul Baranoski       Add Shine as Box recipient to all STS reports.
# 03/26/2026   Paul Baranoski       Remove LINDA.KING@CMS.HHS.GOV from email distros. She's leaving CMS.
# 04/21/2026   Paul Baranoski       Replace BIT_DDOM_PO@cms.hhs.gov with BIT_DDOM_PO@cms.hhs.gov.
# 05/12/2026   Paul Baranoski       Add Kenneth.Wilkins@cms.hhs.gov to PART_AB_EMAIL_SUCCESS_RECIPIENT.
# 05/28/2026   Paul Baranoski       Remove Informatica source modules and other variables not needed so we can migrate to new server.
#########################################################################################
#set -x
echo "In SET_ENV_XTR "

echo "hostname=`hostname`"

ENV_IND=`hostname | cut -c-1 `
if [ ${ENV_IND} = 'd' ];then
    ENVNAME=DEV
elif [ ${ENV_IND} = 't' ];then    
    ENVNAME=TST    
elif [ ${ENV_IND} = 'i' ];then    
    ENVNAME=IMPL  
elif [ ${ENV_IND} = 'p' ];then   	
    ENVNAME=PRD  
else    
    ENVNAME=UNK 
fi	

# Set default value if ENVNAME is null
ENVNAME=${ENVNAME:='UNK'}

export ENVNAME
    
echo '++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'
echo -e "\t\tCurrent Environment is : " ${ENVNAME}
echo '++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'   


if [ $ENVNAME = 'DEV' -o $ENVNAME = 'TST' -o $ENVNAME = 'IMPL' -o $ENVNAME = 'PRD' ];
then
    if [ $ENVNAME = 'DEV' -o $ENVNAME = 'TST' -o $ENVNAME = 'IMPL' ];
    then
	SF_ETL_WHSE=NP
    else
	SF_ETL_WHSE=P
    fi
else
    echo "Environment should be either DEV or TST or IMPL or PRD !"
    exit 1
fi


# Snowflake warehouse variables
export sf_etl_warehouse=BIA_${SF_ETL_WHSE}_ETL_WKLD
export sf_xtr_warehouse=BIA_${SF_ETL_WHSE}_XTR_WKLD


#export CMN_UTIL=/app/IDRC/COMMON/CMS/scripts/util
export CMN_UTIL=/app/IDRC/XTR/CMS/scripts/run

######################################
#  Configures Additional Software   #
######################################
if  [ ${ENVNAME} = 'DEV' ];then
    #export IDRC_DATALAKE_AWS_ACCT="772614087260"
    export IDRC_DATALAKE_AWS_ACCT="779846826630"
elif [ ${ENVNAME} = 'TST' ];then
    export IDRC_DATALAKE_AWS_ACCT="232722229861"
elif [ ${ENVNAME} = 'IMPL' ];then
    export IDRC_DATALAKE_AWS_ACCT="291492156955"
elif [ ${ENVNAME} = 'PRD' ];then
    export IDRC_DATALAKE_AWS_ACCT="148550782651"
fi
	
# Python Interpreter
export PYTHON_COMMAND=python3


############################################
# DDOM manifest file info
############################################
export DDOM_CONTACT_NM="Edward Belle"
export DDOM_CONTACT_PHNE_NUM="443-764-4548"
export DDOM_CONTACT_EMAIL="edward.belle@cms.hhs.gov"

		
#######################################################
# SET Bucket and SMTP env variables
#######################################################
if [ ${ENVNAME} = 'PRD' ]; then
	export bucket=aws-hhs-cms-eadg-bia-ddom-extracts/xtr/
	export SMTP_SERVER="cloud-smtp-prod.bitaws.local"
else
	export bucket=aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod/xtr/${ENVNAME}/
	export SMTP_SERVER="cloud-smtp-nonprod.bitaws.local"
fi


export ASC_PTB_BUCKET=${bucket}ASC_PTB/
export BLBTN_BUCKET=${bucket}Blbtn/
export CALENDAR_BUCKET=${bucket}CALENDAR/
export CONFIG_BUCKET=${bucket}config/

export DASHBOARD_BUCKET=${bucket}Dashboard/
export DEMO_FNDR_BUCKET=${bucket}DemoFndr/
export DDOM_BUCKET=${bucket}DDOM/
export DOD_NPI_BUCKET=${bucket}DoD_NPI/

export DOJ_BUCKET=${bucket}DOJ/
export DSH_BUCKET=${bucket}DSH/
export DUALS_MedAdv_BUCKET=${bucket}DUALS_MedAdv/

export FINDER_FILE_BUCKET=${bucket}Finder_Files/
export FINDER_FILE_SSA_BUCKET=${bucket}Finder_Files_SSA/
export FMR_BUCKET=${bucket}FMR/
export FOIA_BUCKET=${bucket}FOIA/

export GITHUB_BUCKET=${bucket}GITHUB/

export HCPP_BUCKET=${bucket}HCPP/
export HOS_BUCKET=${bucket}HOS/

export MANIFEST_BUCKET=${bucket}manifest_files/
export MANIFEST_ARCHIVE_BUCKET=${bucket}manifest_files_archive/
export MANIFEST_HOLD_BUCKET=${bucket}manifest_files_hold/
export MANIFEST_SSA_BUCKET=${bucket}manifest_files/SSA/
export MANIFEST_VA_MAC_BUCKET=${bucket}manifest_files/VA/MAC/
export MANIFEST_VA_PBM_BUCKET=${bucket}manifest_files/VA/PBM/

export MEDPAC_BUCKET=${bucket}MEDPAC_HOS/
export MEDPAR_BAYSTATE_BUCKET=${bucket}MEDPAR_BAYSTATE/
export MNUP_BUCKET=${bucket}MNUPAnnual/
export MNUP_MONTHLY_BUCKET=${bucket}MNUPMonthly/
export NYSPAP_BUCKET=${bucket}NYSPAP/	

export OFM_PDE_BUCKET=${bucket}OFM_PDE/
export OPMHI_HHA_BUCKET=${bucket}OPMHI_HHA/
export OPMHI_SNF_BUCKET=${bucket}OPMHI_SNF/
export OPMHI_INP_BUCKET=${bucket}OPMHI_INP/
export OPMHI_OPT_BUCKET=${bucket}OPMHI_OPT/
export OPMHI_HSP_BUCKET=${bucket}OPMHI_HSP/
export OPMHI_CAR_BUCKET=${bucket}OPMHI_CAR/
export OPMHI_DME_BUCKET=${bucket}OPMHI_DME/
export OPMHI_ENRLMNT_BUCKET=${bucket}OPMHI_ENRLMNT/
export OPMHI_PDE_BUCKET=${bucket}OPMHI_PDE/

export PAC_BUCKET=${bucket}PAC/	
export PARTAB_BUCKET=${bucket}PartAB_Extract/	
export PECOS_BUCKET=${bucket}PECOS/
export PHYZIP_BUCKET=${bucket}PHYZIP/
export PSA_BUCKET=${bucket}PSA/
export PSPS_BUCKET=${bucket}PSPS/
export PSPSNPI_BUCKET=${bucket}PSPS_NPI/

export PTB_CARR_BUCKET=${bucket}PTBCarrier/
export PTDDUALMNTH_BUCKET=${bucket}PTDDualMnth/	
export PTDDUALDAILY_BUCKET=${bucket}PTDDualDaily/
export PTDDUALHIST_BUCKET=${bucket}PTDDualHstr/

export RAND_FFSPTAB_BUCKET=${bucket}RAND_FFSPTAB/
export RAND_PDE_BUCKET=${bucket}RAND_PDE/

export SEER_BUCKET=${bucket}SEER/

export SFTP_BUCKET=${bucket}SFTP_Files/
export SFTP_FOLDER=SFTP_Files/
export SRTR_ENC_BUCKET=${bucket}SRTR_ENCPTAB/
export SRTR_FFS_BUCKET=${bucket}SRTR_FFSPTAB/
export SRTR_PDE_BUCKET=${bucket}SRTR_PDE/
export SRTR_ENRLMNT_BUCKET=${bucket}SRTR_ENRLMNT/

export STS_HHA_BUCKET=${bucket}STS_HHA/
export STS_HHA_REV_CTR_BUCKET=${bucket}STS_HHA_REV_CTR/
export STS_MED_INS_BUCKET=${bucket}STS_MED_INS/
export STS_MED_INS_MN_BUCKET=${bucket}STS_MED_INS_MN/
export STS_SNF_BUCKET=${bucket}STS_SNF/
export STS_PTA_BPYMTS_BUCKET=${bucket}STS_PTA_BPYMTS/
export STS_PTA_BPYMTS_MN_BUCKET=${bucket}STS_PTA_BPYMTS_MN/
export STS_HHA_FACILITY_BUCKET=${bucket}STS_HHA_FACILITY/
export STS_HOS_FACILITY_BUCKET=${bucket}STS_HOS_FACILITY/


export SAF_PDE_BUCKET=${bucket}SAF_PDE/
export SAFENC_HHA_BUCKET=${bucket}SAFENC_HHA/
export SAFENC_SNF_BUCKET=${bucket}SAFENC_SNF/
export SAFENC_INP_BUCKET=${bucket}SAFENC_INP/
export SAFENC_OPT_BUCKET=${bucket}SAFENC_OPT/
export SAFENC_CAR_BUCKET=${bucket}SAFENC_CAR/
export SAFENC_DME_BUCKET=${bucket}SAFENC_DME/

export TRICARE_BUCKET=${bucket}TRICARE/

export VAPTD_BUCKET=${bucket}VA_PTD/
export VARTN_BUCKET=${bucket}VA_RTRN/


########################################################################
# To support conversion from bash to python TESTING in production.
########################################################################
swTESTING=${TESTING:-"N"} 

if [ "${swTESTING}" = "Y" ];then
    echo "TESTING"
    export TESTLOG="TESTING_SH_"
    export TESTEMAIL="-TESTING_SH"
else
    echo "NOT TESTING"
    export TESTLOG=""
    export TESTEMAIL=""
fi	
	

########################################################################
# Email Receipients
########################################################################
CMS_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"

if [ $ENVNAME = 'DEV' -o $ENVNAME = 'TST' -o $ENVNAME = 'IMPL' -o "${swTESTING}" = "Y" ];
then

    export ASC_PTB_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"

    export BLBTN_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export BLBTN_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export BLBTN_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"

	export CALENDAR_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
	
    export DSH_EMAIL_SUCCESS_RECIPIENT="bit-extractsupport@index-analytics.com"
    export DSH_EMAIL_FAILURE_RECIPIENT="bit-extractsupport@index-analytics.com"
	export DSH_EMAIL_BCC="bit-extractalerts@index-analytics.com"	
	export DSH_EMAIL_REPLY_MSG="Note: Send inquiries to dshquestions@cms.hhs.gov "
    export DSH_BOX_RECIPIENT="bit-extractsupport@index-analytics.com"

    export DASHBOARD_RPT_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export DASHBOARD_RPT_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"
	
    export DEMO_FINDER_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export DEMO_FINDER_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export DEMO_FINDER_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"

    export DOD_NPI_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export DOD_NPI_BOX_RECIPIENT="bit-extractalerts@index-analytics.com"
	
    export DOJ_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export DOJ_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export DOJ_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"

	export DUALMEDADV_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"

    export EFT_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export EFT_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export EFT_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"

	export ENIGMA_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
	export ENIGMA_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"

    export FMR_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export FMR_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"	
    export FMR_BOX_RECIPIENT="bit-extractalerts@index-analytics.com"
	
    export HCPP_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export HCPP_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export HCPP_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"

    export HOS_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export HOS_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export HOS_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"
    export HOS_EMAIL_BOX_RECIPIENT="bit-extractalerts@index-analytics.com"

    export MEDPAC_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export MEDPAC_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export MEDPAC_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"
    export MEDPAC_EMAIL_BOX_RECIPIENT="bit-extractalerts@index-analytics.com"

    export MEDPAR_BAYSTATE_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"

    export MNUP_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export MNUP_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"	
    export MNUP_EMAIL_BOX_RECIPIENT="jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov,Daniel.Lee2@cms.hhs.gov,olga.yablonovsky@ssa.gov"
	
    export NYSPAP_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export NYSPAP_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export NYSPAP_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"
    export NYSPAP_BOX_RECIPIENT="bit-extractalerts@index-analytics.com"	

    export OFM_PDE_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export OFM_PDE_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"
	
	export OFM_PDE_BLAND_BOX_RECIPIENT="bit-extractalerts@index-analytics.com"
	export OFM_PDE_CGI_BOX_RECIPIENT="bit-extractalerts@index-analytics.com"
	export OFM_PDE_MHM_BOX_RECIPIENT="bit-extractalerts@index-analytics.com"	
	export OFM_PDE_DJLLC_BOX_RECIPIENT="bit-extractalerts@index-analytics.com"	
	export OFM_PDE_CONRAD_BOX_RECIPIENT="bit-extractalerts@index-analytics.com"	
	export OFM_PDE_UNKNOWN_BOX_RECIPIENT="bit-extractalerts@index-analytics.com"	
	
    export OPMHI_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export OPMHI_BOX_RECIPIENT="bit-extractalerts@index-analytics.com"	

    export OPMHI_HIST_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export OPMHI_HIST_EMAIL_FAILURE_RECIPIENT="jturner-con@index-analytics.com"
    export OPMHI_HIST_EMAIL_SUCCESS_RECIPIENT="jturner-con@index-analytics.com"

    export PAC_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export PAC_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export PAC_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"

	export PART_AB_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
	export PART_AB_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"

    export PARTB_CARRIER_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"

    export PECOS_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"

    export PHYZIP_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export PHYZIP_BOX_RECIPIENTS="bit-extractalerts@index-analytics.com"

    export PSA_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export PSA_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export PSA_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"	

    export PSPS_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export PSPS_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export PSPS_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"	

    export PSPS_HCPCS_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export PSPS_HCPCS_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export PSPS_HCPCS_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"	

    export PSPS_SUPPRESSION_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"

    export PSPSNPI_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export PSPSNPI_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export PSPSNPI_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"
	
    export PTDDUALMNTH_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export PTDDUALMNTH_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export PTDDUALMNTH_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"	

    export PTDDUALDAILY_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export PTDDUALDAILY_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export PTDDUALDAILY_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"

    export RAND_FFS_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export RAND_FFS_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export RAND_FFS_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"
	export RAND_FFS_BOX_RECIPIENTS="bit-extractalerts@index-analytics.com"
	
    export RAND_PDE_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export RAND_PDE_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export RAND_PDE_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"

    export SAF_PDE_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"

    export SAFENC_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export SAFENC_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export SAFENC_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"

    export SAFENC_CAR_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export SAFENC_CAR_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export SAFENC_CAR_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"

    export SEER_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export SEER_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"
	export SEER_BOX_RECIPIENTS="bit-extractalerts@index-analytics.com"
	export SEER_EMAIL_BCC="bit-extractalerts@index-analytics.com"	
	
    export SRTR_FNDR_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export SRTR_FNDR_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export SRTR_FNDR_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"

    export SRTR_ENC_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export SRTR_ENC_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export SRTR_ENC_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"

    export SRTR_FFS_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export SRTR_FFS_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export SRTR_FFS_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"

    export SRTR_PDE_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export SRTR_PDE_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export SRTR_PDE_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"

    export SRTR_ENRLMNT_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export SRTR_ENRLMNT_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export SRTR_ENRLMNT_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"

    export STS_HHA_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export STS_HHA_BOX_RECIPIENT="bit-extractalerts@index-analytics.com"

    export STS_HHA_REV_CTR_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export STS_HHA_REV_CTR_BOX_RECIPIENT="bit-extractalerts@index-analytics.com"
	
    export STS_MED_INS_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export STS_MED_INS_BOX_RECIPIENT="bit-extractalerts@index-analytics.com"

    export STS_MED_INS_MN_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export STS_MED_INS_MN_BOX_RECIPIENT="bit-extractalerts@index-analytics.com"
	
    export STS_SNF_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export STS_SNF_BOX_RECIPIENT="bit-extractalerts@index-analytics.com"

    export STS_PTA_BPYMTS_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export STS_PTA_BPYMTS_BOX_RECIPIENT="bit-extractalerts@index-analytics.com"

    export STS_PTA_BPYMTS_MN_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export STS_PTA_BPYMTS_MN_BOX_RECIPIENT="bit-extractalerts@index-analytics.com"

    export STS_HHA_FACILITY_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export STS_HHA_FACILITY_BOX_RECIPIENT="bit-extractalerts@index-analytics.com"

    export STS_HOS_FACILITY_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export STS_HOS_FACILITY_BOX_RECIPIENT="bit-extractalerts@index-analytics.com"
	
    export TRICARE_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"

    export VAPTD_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export VAPTD_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export VAPTD_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"
    export VAPTD_EMAIL_BOX_RECIPIENT="bit-extractalerts@index-analytics.com"	

    export VARTN_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export VARTN_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export VARTN_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"
    export VARTN_EMAIL_BOX_RECIPIENT="bit-extractalerts@index-analytics.com"
    #	
    #
else

    export ASC_PTB_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"

    export BLBTN_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export BLBTN_EMAIL_SUCCESS_RECIPIENT="IDR_SOURCES@CMS.HHS.GOV,JEFF.BYRNES@REVELANTTECH.COM,DONOVAN.WADDEL@CMS.HHS.GOV,BIT_DDOM_PO@cms.hhs.gov,bit-extractsupport@index-analytics.com"
    export BLBTN_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"

	export CALENDAR_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"

    export DASHBOARD_RPT_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"
    export DASHBOARD_RPT_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"
	
    export DSH_EMAIL_SUCCESS_RECIPIENT="dshquestions@cms.hhs.gov"
    export DSH_EMAIL_FAILURE_RECIPIENT="dshquestions@cms.hhs.gov"
	export DSH_EMAIL_BCC="bit-extractalerts@index-analytics.com"
	export DSH_EMAIL_REPLY_MSG="Note: Send inquiries to dshquestions@cms.hhs.gov "
    export DSH_BOX_RECIPIENT="jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov"
	
    export DEMO_FINDER_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export DEMO_FINDER_EMAIL_SUCCESS_RECIPIENT="BIT_DDOM_PO@cms.hhs.gov,bit-extractsupport@index-analytics.com"
    export DEMO_FINDER_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"

    export DOD_NPI_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"
    export DOD_NPI_BOX_RECIPIENT="jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov"
	
    export DOJ_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export DOJ_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export DOJ_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"

	export DUALMEDADV_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"
	
	export ENIGMA_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
	export ENIGMA_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"
	
    export EFT_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export EFT_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export EFT_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"

    export FMR_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov,BIT_DDOM_PO@cms.hhs.gov"
    export FMR_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"
	export FMR_BOX_RECIPIENT="sukgoo.pak@palmettogba.com,nicholas.landry@palmettogba.com,dumbiri.stone@palmettogba.com,jagadeeshwar.pagidimarri@cms.hhs.gov,monica.algozer@cms.hhs.gov"

    export HCPP_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export HCPP_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"
    export HCPP_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"

    export HOS_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export HOS_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"
    export HOS_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"
    export HOS_EMAIL_BOX_RECIPIENT="jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,kimberly.demichele@cms.hhs.gov,alyssa.rosen@cms.hhs.gov,esjackson@rti.org,akandilov@rti.org,aakinseye@rti.org"

    export MEDPAC_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export MEDPAC_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"
    export MEDPAC_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"
    export MEDPAC_EMAIL_BOX_RECIPIENT="monica.algozer@cms.hhs.gov,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,ssiford@acumenllc.com"


    export MEDPAR_BAYSTATE_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"

    export MNUP_EMAIL_SUCCESS_RECIPIENT="ssa.mnup.support@ssa.gov,BIT_DDOM_PO@cms.hhs.gov,bit-extractalerts@index-analytics.com"
    export MNUP_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"		
    export MNUP_EMAIL_BOX_RECIPIENT="jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,Daniel.Lee2@cms.hhs.gov,olga.yablonovsky@ssa.gov,ssa.mnup.support@ssa.gov"
	
    export NYSPAP_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export NYSPAP_EMAIL_SUCCESS_RECIPIENT="Robert.Palumbo@Primetherapeutics.com,bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"
    export NYSPAP_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"	
    export NYSPAP_BOX_RECIPIENT="Robert.Palumbo@Primetherapeutics.com,monica.algozer@cms.hhs.gov,jagadeeshwar.pagidimarri@cms.hhs.gov"


    export OFM_PDE_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"
    export OFM_PDE_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"


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
    #
    ################################################################################################################# 
	export OFM_PDE_BLAND_BOX_RECIPIENT="KBrabec@blandgovconsulting.com,chearn@blandgovconsulting.com,aabboud@blandgovconsulting.com,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov"
	export OFM_PDE_CGI_BOX_RECIPIENT="KSorensen@mslc.com,sgutcher@mslc.com,mcoughlin@mslc.com,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov"
	export OFM_PDE_MHM_BOX_RECIPIENT="MDavis@DavisFarr.com,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov"	
	export OFM_PDE_DJLLC_BOX_RECIPIENT="Michelle.McConkey@djllc.com,Christine.Fleming@djllc.com,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov"	
	export OFM_PDE_CONRAD_BOX_RECIPIENT="mcano@conradllp.com,sperera@conradllp.com,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov"	
	export OFM_PDE_UNKNOWN_BOX_RECIPIENT="jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov"	

    export OPMHI_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,Joseph.Stewart@opm.gov,BIT_DDOM_PO@cms.hhs.gov"
    export OPMHI_BOX_RECIPIENT="Joseph.Stewart@opm.gov,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov"

    export OPMHI_HIST_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export OPMHI_HIST_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"
    export OPMHI_HIST_EMAIL_SUCCESS_RECIPIENT="BIT_DDOM_PO@cms.hhs.gov"

    export PAC_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export PAC_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export PAC_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"	
	
	export PART_AB_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
	export PART_AB_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,Kenneth.Wilkins@cms.hhs.gov,Angela.Huynh@cms.hhs.gov,Floyd.Epps@cms.hhs.gov,Robert.Fox@cms.hhs.gov,BIT_DDOM_PO@cms.hhs.gov"

    #export PARTB_CARRIER_EMAIL_SUCCESS_RECIPIENT="GIFT.TEE@CMS.HHS.GOV,CHARLES.CAMPBELL@CMS.HHS.GOV,LARRY.CHAN@CMS.HHS.GOV,MICHAEL.SORACOE@CMS.HHS.GOV,REBECCA.ZELLER@CMS.HHS.GOV,BIT_DDOM_PO@cms.hhs.gov,bit-extractsupport@index-analytics.com"
    export PARTB_CARRIER_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"

    export PECOS_EMAIL_SUCCESS_RECIPIENT="PRAVEEN.BOBBASANI@CGIFEDERAL.COM,BIT_DDOM_PO@cms.hhs.gov,bit-extractsupport@index-analytics.com"

    export PHYZIP_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,Rebecca.Zeller@cms.hhs.gov,BIT_DDOM_PO@cms.hhs.gov"
    export PHYZIP_BOX_RECIPIENTS="Rebecca.Zeller@cms.hhs.gov,BIT_DDOM_PO@cms.hhs.gov"

    export PSA_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export PSA_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"
    export PSA_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"	

    export PSPS_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export PSPS_EMAIL_SUCCESS_RECIPIENT="GIFT.TEE@CMS.HHS.GOV,CHARLES.CAMPBELL@CMS.HHS.GOV,MICHAEL.SORACOE@CMS.HHS.GOV,REBECCA.ZELLER@CMS.HHS.GOV,BIT_DDOM_PO@cms.hhs.gov,bit-extractsupport@index-analytics.com"
    export PSPS_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"	

    export PSPS_HCPCS_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export PSPS_HCPCS_EMAIL_SUCCESS_RECIPIENT="bit-extractsupport@index-analytics.com,KEVIN.HODGES2@CMS.HHS.GOV,BIT_DDOM_PO@cms.hhs.gov"
    export PSPS_HCPCS_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"	

    export PSPS_SUPPRESSION_EMAIL_SUCCESS_RECIPIENT="bit-extractsupport@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"

    export PSPSNPI_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export PSPSNPI_SUCCESS_RECIPIENT="bit-extractsupport@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"
    export PSPSNPI_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"

    export PTDDUALMNTH_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export PTDDUALMNTH_EMAIL_SUCCESS_RECIPIENT="Nicole.Perry@cms.hhs.gov,SDRC@ACUMENLLC.COM,bit-extractsupport@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"
    export PTDDUALMNTH_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"

    export PTDDUALDAILY_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export PTDDUALDAILY_EMAIL_SUCCESS_RECIPIENT="Nicole.Perry@cms.hhs.gov,SDRC@ACUMENLLC.COM,bit-extractsupport@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"
    export PTDDUALDAILY_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"

    export RAND_FFS_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export RAND_FFS_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"
    export RAND_FFS_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"
	export RAND_FFS_BOX_RECIPIENTS="jdaly@rand.org,jlai@rand.org,Jagadeeshwar.Pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov"

    export RAND_PDE_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export RAND_PDE_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"
    export RAND_PDE_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"

    export SAF_PDE_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"

    export SAFENC_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export SAFENC_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"
    export SAFENC_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"

    export SAFENC_CAR_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export SAFENC_CAR_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"
    export SAFENC_CAR_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"

    export SEER_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"
    export SEER_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"
	export SEER_BOX_RECIPIENTS="jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov"
	export SEER_EMAIL_BCC="bit-extractalerts@index-analytics.com"	
	
    export SRTR_FNDR_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export SRTR_FNDR_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export SRTR_FNDR_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"

    export SRTR_ENC_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export SRTR_ENC_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export SRTR_ENC_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"

    export SRTR_ENRLMNT_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export SRTR_ENRLMNT_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export SRTR_ENRLMNT_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"

    export SRTR_FFS_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export SRTR_FFS_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
    export SRTR_FFS_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"

    export SRTR_PDE_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export SRTR_PDE_EMAIL_SUCCESS_RECIPIENT="it-extractalerts@index-analytics.com"
    export SRTR_PDE_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"

    export STS_HHA_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"
    export STS_HHA_BOX_RECIPIENT="shine.jacob@cms.hhs.gov,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"

    export STS_HHA_REV_CTR_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"
    export STS_HHA_REV_CTR_BOX_RECIPIENT="shine.jacob@cms.hhs.gov,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"
	
    export STS_MED_INS_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"
    export STS_MED_INS_BOX_RECIPIENT="shine.jacob@cms.hhs.gov,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"

    export STS_MED_INS_MN_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov,michelle.wilson@state.mn.us,kelsey.kannenberg@state.mn.us"
    export STS_MED_INS_MN_BOX_RECIPIENT="shine.jacob@cms.hhs.gov,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov,michelle.wilson@state.mn.us,kelsey.kannenberg@state.mn.us"

    export STS_SNF_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"
    export STS_SNF_BOX_RECIPIENT="shine.jacob@cms.hhs.gov,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"

    export STS_PTA_BPYMTS_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"
    export STS_PTA_BPYMTS_BOX_RECIPIENT="shine.jacob@cms.hhs.gov,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"

    export STS_PTA_BPYMTS_MN_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov,michelle.wilson@state.mn.us,kelsey.kannenberg@state.mn.us"
    export STS_PTA_BPYMTS_MN_BOX_RECIPIENT="shine.jacob@cms.hhs.gov,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov,michelle.wilson@state.mn.us,kelsey.kannenberg@state.mn.us"

    export STS_HHA_FACILITY_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"
    export STS_HHA_FACILITY_BOX_RECIPIENT="shine.jacob@cms.hhs.gov,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"

    export STS_HOS_FACILITY_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"
    export STS_HOS_FACILITY_BOX_RECIPIENT="shine.jacob@cms.hhs.gov,jagadeeshwar.pagidimarri@cms.hhs.gov,emma.battista@cms.hhs.gov,monica.algozer@cms.hhs.gov,Anne.Martin@cms.hhs.gov,Jacqueline.Fiore@cms.hhs.gov"
	
	
    export TRICARE_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"

    #export VAPTD_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    #export VAPTD_EMAIL_SUCCESS_RECIPIENT="MIKE.WROBEL@VA.GOV,WALID.GELLAD@VA.GOV,FRAN.CUNNINGHAM@VA.GOV,MONIR.HOSSAIN@VA.GOV,BIT_DDOM_PO@cms.hhs.gov,bit-extractsupport@index-analytics.com"
    #export VAPTD_EMAIL_FAILURE_RECIPIENT="bit-extractsupport@index-analytics.com,vkhanna@index-analytics.com,pbaranoski-con@index-analytics.com,jturner-con@index-analytics.com"
    export VAPTD_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export VAPTD_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"
    export VAPTD_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"
    export VAPTD_EMAIL_BOX_RECIPIENT="bit-extractalerts@index-analytics.com"
	
    export VARTN_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
    export VARTN_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com,BIT_DDOM_PO@cms.hhs.gov"
    export VARTN_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"
    export VARTN_EMAIL_BOX_RECIPIENT="Monir.Hossain@va.gov"

fi
   

#######################################################################################################################################
#                                            End of Script                                                                            #
########################################################################################################################################
