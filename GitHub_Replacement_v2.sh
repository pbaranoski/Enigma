#!/usr/bin/bash
#
######################################################################################
# Name:  GitHub_Replacement_v2.sh
#
# Desc: Migrate our source code to production from S3.
#
#
# Created: Paul Baranoski  06/26/2025
# Modified: 
#
# Paul Baranoski 2025-06-26 Create script to migrate code for any of the Enigma Apps
#                           which have different GITHUB folders and different linux folder paths 
#                           where the source code lives.
#
######################################################################################
set +x

echo "In GitHub_Replacement_v2.sh "

#############################################################
# These are required for setting path to python code  
#############################################################
source /app/INFA/Config/setToolsEnv.ksh
source /app/INFA/Logon/idrc_xtr.logon

#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`

# Export TMSTMP variable for child scripts
export TMSTMP 

CMS_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov"
ENIGMA_EMAIL_SUCCESS_RECIPIENT="bit-extractalerts@index-analytics.com"
ENIGMA_EMAIL_FAILURE_RECIPIENT="bit-extractalerts@index-analytics.com"
PYTHON_COMMAND=python3

#############################################################
# Get ENIGMA APP ID based on service ID
#############################################################
case ${USER} in 
  "idrcxtr")
    ENIGMA_APP_ID="XTR" 
    ;;
  "idrcbepsd")
    ENIGMA_APP_ID="BEPSD" 
    ;;	
  "idrcptb")
    ENIGMA_APP_ID="PTB" 
    ;;	
  "idrcpdb")
    ENIGMA_APP_ID="PDB" 
    ;;
  *)
    ENIGMA_APP_ID="UNKNOWN" 
    ;;
esac		


#############################################################
# Set paths
#############################################################
LOGNAME=/app/IDRC/${ENIGMA_APP_ID}/CMS/logs/GitHub_Replacement_${TMSTMP}.log

RUNDIR=/app/IDRC/${ENIGMA_APP_ID}/CMS/scripts/run/
DATADIR=/app/IDRC/${ENIGMA_APP_ID}/CMS/data/
SOURCE_DIR=/app/IDRC/${ENIGMA_APP_ID}/CMS/scripts/run/


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "GitHub_Replacement_v2.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}


#############################################################
# Include the appropriate ENV script
#############################################################
echo "" >> ${LOGNAME}
echo "Get current environment from server name." >> ${LOGNAME}

# SERVER_ENV_ID should be d1, t1, i1, or p1
SERVER_ENV_ID=`uname -a | awk '{print $2}' | cut -d- -f1` 2>> ${LOGNAME}
echo "SERVER_ENV_ID=${SERVER_ENV_ID}" >> ${LOGNAME}


case ${SERVER_ENV_ID} in 

  "d1")
    ENVNAME="DEV" 
    ;;
  "t1")
    ENVNAME="TST" 
    ;;
  "i1")
    ENVNAME="IMPL" 
    ;;	
  "p1")
    ENVNAME="PRD" 
    ;;	
  *	)
    ENVNAME="UNKNOWN" 
    ;;	
	
esac

echo "ENVNAME=${ENVNAME}" >> ${LOGNAME}


#######################################################
# Set appropriate S3 bucket/path name 
#######################################################
if [ $ENVNAME = 'PRD' ]; then
	export bucket=aws-hhs-cms-eadg-bia-ddom-extracts/xtr/
	export SMTP_SERVER="cloud-smtp-prod.bitaws.local"
else
	export bucket=aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod/xtr/${ENVNAME}/
	export SMTP_SERVER="cloud-smtp-nonprod.bitaws.local"
fi


# ENIGMA_APP_ID = XTR, BEPSD, PTB, PDB
GITHUB_BUCKET=${bucket}GITHUB_${ENIGMA_APP_ID}/ 

echo "GITHUB_BUCKET=${GITHUB_BUCKET}" >> ${LOGNAME}


#################################################################################
# Are there code modules to copy to linux server?
#################################################################################
echo "" >> ${LOGNAME}
echo "Count NOF GitHub source modules found in ${GITHUB_BUCKET}" >> ${LOGNAME}

NOF_FILES=`aws s3 ls s3://${GITHUB_BUCKET} | awk '{print $4}'  | egrep '^[a-zA-Z0-9]+' | wc -l ` 2>> ${LOGNAME}

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Count NOF S3 GitHub source modules found in s3://${GITHUB_BUCKET} failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="GitHub_Replacement.sh - Failed ($ENVNAME)"
	MSG="Count NOF S3 GitHub source modules found in s3://${GITHUB_BUCKET} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	

echo "NOF_FILES=${NOF_FILES}"  >> ${LOGNAME}

#################################################
# If 0 source files --> end gracefully		
#################################################
if [ ${NOF_FILES} -eq 0 ];then 
	echo "" >> ${LOGNAME}
	echo "There are no S3 GitHub source modules to process in s3://${GITHUB_BUCKET}." >> ${LOGNAME}
	
	# Send Info email	
	SUBJECT="GitHub_Replacement.sh script ended - nothing to process ($ENVNAME)"
	MSG="There are no S3 GitHub source modules to process in s3://${GITHUB_BUCKET}."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	
	echo "" >> ${LOGNAME}
	echo "GitHub_Replacement.sh completed successfully." >> ${LOGNAME}

	echo "Ended at `date` " >> ${LOGNAME}
	echo "" >> ${LOGNAME}

	exit 0

fi 


#################################################
# Set current working directory		
#################################################
echo "" >> ${LOGNAME}
echo "Set working directory to ${SOURCE_DIR}" >> ${LOGNAME}

cd ${SOURCE_DIR}  >> ${LOGNAME}  2>&1


#################################################################################
# pwd /scripts/run directory contents before copying modules from S3. 
#################################################################################
echo "" >> ${LOGNAME}
echo "Display contents of `pwd` before copying modules from S3 " >> ${LOGNAME}

echo "" >> ${LOGNAME}
echo "`ls -l `"  >> ${LOGNAME}  2>&1


#################################################################################
# Get list of S3 source modules to process. Skip sub-folders and GITHUB folder itself.
#################################################################################
echo "" >> ${LOGNAME}
echo "Get list GitHub source modules to process in ${GITHUB_BUCKET}" >> ${LOGNAME}

Files2Process=`aws s3 ls s3://${GITHUB_BUCKET} | awk '{print $4}'  | egrep '^[a-zA-Z0-9]+'` 2>> ${LOGNAME}

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Get list GitHub source modules to process in ${GITHUB_BUCKET} failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="GitHub_Replacement.sh - Failed ($ENVNAME)"
	MSG="Get list GitHub source modules to process in ${GITHUB_BUCKET} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	


echo "Files2Process=${Files2Process}" >> ${LOGNAME}


##############################################
# Process each file in S3://GitHub folder
##############################################
for File2Process in ${Files2Process}
do

	echo "" >> ${LOGNAME}
	echo "----------------------------" >> ${LOGNAME}
	echo "File2Process: ${File2Process}" >> ${LOGNAME}


	#########################################################
	# GITHUB Folder is returned as blank file --> skip file
	#########################################################
	if [ "${File2Process}" = "" ];then
		continue
	fi


	#########################################################
	# Copy module from S3 to linux.
	#########################################################
	echo "" >> ${LOGNAME}
	echo "Copy S3 source module from s3://${GITHUB_BUCKET}${File2Process} to linux " >> ${LOGNAME}

	aws s3 cp s3://${GITHUB_BUCKET}${File2Process} ${SOURCE_DIR}${File2Process}  1>> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Copy S3 source module from s3://${GITHUB_BUCKET}${File2Process} to linux failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="GitHub_Replacement.sh - Failed ($ENVNAME)"
		MSG="Copy S3 source module from s3://${GITHUB_BUCKET}${File2Process} to linux failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi	

	#########################################################
	# Set File permissions
	#########################################################
	echo "" >> ${LOGNAME}
	echo "Set File permissions for ${SOURCE_DIR}${File2Process} " >> ${LOGNAME}

	chmod 750 ${SOURCE_DIR}${File2Process} 2>> ${LOGNAME}
	
	#########################################################
	# Remove carrier returns (CR) \r from source module.
	#########################################################
	echo "" >> ${LOGNAME}
	echo "Remove carriage returns for ${SOURCE_DIR}${File2Process} " >> ${LOGNAME}

	sed -i 's/\r//g' ${SOURCE_DIR}${File2Process} 2>> ${LOGNAME}


	##########################################################
	# Move Module in S3 to archive directory (with timestamp)
	##########################################################
	echo "" >> ${LOGNAME}
	echo "Move S3 processed source module from s3://${GITHUB_BUCKET}${File2Process} to s3://${GITHUB_BUCKET}archive_${TMSTMP}/${File2Process} " >> ${LOGNAME}

	aws s3 mv s3://${GITHUB_BUCKET}${File2Process} s3://${GITHUB_BUCKET}archive_${TMSTMP}/${File2Process}  1>> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Move S3 processed source module from s3://${GITHUB_BUCKET}${File2Process} to s3://${GITHUB_BUCKET}archive_${TMSTMP}/${File2Process} failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="GitHub_Replacement.sh - Failed ($ENVNAME)"
		MSG="Move S3 processed source module from s3://${GITHUB_BUCKET}${File2Process} to s3://${GITHUB_BUCKET}archive_${TMSTMP}/${File2Process} failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi	
	
		
done
	

#################################################################################
# pwd /scripts/run directory contents after copying modules from S3. 
#################################################################################
echo "" >> ${LOGNAME}
echo "Display contents of `pwd` after copying modules from S3  " >> ${LOGNAME}

echo "" >> ${LOGNAME}
echo "`ls -l`"  >> ${LOGNAME}  2>&1


#############################################################
# Send success email of DSH Extract files
#############################################################
echo "" >> ${LOGNAME}
echo "Send success email." >> ${LOGNAME}

# Send Success email	
SUBJECT="GitHub Replacements scripts - completed ($ENVNAME)"
MSG="GitHub Replacements script completed. \n\nThe following source modules were moved from ${GITHUB_BUCKET} to ${SOURCE_DIR}:\n\n${Files2Process}."
${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Error in calling sendEmail.py" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Sending Success email in GitHub_Replacement.sh  - Failed (${ENVNAME})"
	MSG="Sending Success email in GitHub_Replacement.sh has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	
			
			
#############################################################
# clean-up linux data directory
#############################################################
echo "" >> ${LOGNAME} 

 
#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "GitHub_Replacement.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS
