#!/usr/bin/bash
############################################################################################################
# Script Name: DashboardInfo_SFUI.sh
#
# Description: This script will extract dashboard info for extracts executed only using the Snowflake UI
#              by examining S3 files named DOJ_SFUI or FOIA_SFUI. Normal execution requires no parameters
#              and will process S3 files from the prior day.
#
#        NOTE: There are two options for providing override parameters.
#              1) supply two override parameter dates. This search for DOJ_SFUI and FOIA_SFUI files created between
#                 the override date range supplied.
#              2) supply three override parameters. Supply two override parameter dates, and 
#                 also supply a non SFUI folder and file prefix that may have been mis-named like DOJ/DOJ_TOUHY
#
# Execute script with no parameters, two override date parameters, or three parameters.
#  ./DashboardInfo_SFUI.sh 
#  ./DashboardInfo_SFUI.sh $1 $2  
#  ./DashboardInfo_SFUI.sh $1 $2 $3 
#
#  $1 --> RUN_FROM_DT (YYYYMMDD format) (Optional)
#  $2 --> RUN_TO_DT   (YYYYMMDD format) (Optional)
# 
#  $1 --> RUN_FROM_DT (YYYYMMDD format) (Optional)
#  $2 --> RUN_TO_DT   (YYYYMMDD format) (Optional)
#  $3 --> BktFldrNFilePrefix (Optional) (Ex. 'DOJ/DOJ_TOUHY') 
# 
#
# Author     : Paul Baranoski	
# Created    : 04/07/2025
#
# Paul Baranoski 2025-04-07 Created script.
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/DashboardInfo_SFUI_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "DashboardInfo_SFUI.sh started at `date` " >> ${LOGNAME}


##################################################################
# Extract can run stand-alone or as a called script.
##################################################################
if ! [[ $# -eq 2 || $# -eq 3 || $# -eq 0 ]]
then
	echo "" >> ${LOGNAME}
	echo "Incorrect # of parameters sent to script. NOF parameters: $#" >> ${LOGNAME}
	exit 12
fi


##################################################################
# Extract log information for yesteray  
#  --> unless overriding with date range   
##################################################################
ProcessNonSFUIFiles="N"
	
if [[ $# -eq 2 ]];then
	echo " " >> ${LOGNAME}
	echo "Using override dates " >> ${LOGNAME}

	RUN_FROM_DT=$1
	RUN_TO_DT=$2
	parmOverrideFldrNFilePrefix=""
	
elif [[ $# -eq 3 ]];then
	echo " " >> ${LOGNAME}
	echo "Using override dates " >> ${LOGNAME}
	
	ProcessNonSFUIFiles="Y"
	
	RUN_FROM_DT=$1
	RUN_TO_DT=$2
	parmOverrideFldrNFilePrefix=$3
	
else
	echo " " >> ${LOGNAME}
	echo "Using script calculated dates " >> ${LOGNAME}
	
	# get yesterday's date
	RUN_FROM_DT=`date -d "-1 day" +%Y%m%d`
	RUN_TO_DT=`date -d "-1 day" +%Y%m%d`
	parmOverrideFldrNFilePrefix=""
fi


#############################################################
# Display parameters passed to script 
#############################################################
echo " " >> ${LOGNAME}
echo "Parameters to script: " >> ${LOGNAME}
echo "   RUN_FROM_DT=${RUN_FROM_DT} " >> ${LOGNAME}
echo "   RUN_TO_DT=${RUN_TO_DT} " >> ${LOGNAME}
echo "   parmOverrideFldrNFilePrefix=${parmOverrideFldrNFilePrefix} " >> ${LOGNAME}

	
#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh 

echo "" >> ${LOGNAME}
echo "DASHBOARD_BUCKET=${DASHBOARD_BUCKET}" >> ${LOGNAME}
echo "DOJ_BUCKET=${DOJ_BUCKET}" >> ${LOGNAME}
echo "FOIA_BUCKET=${FOIA_BUCKET}" >> ${LOGNAME}


#############################################################
# functions
#############################################################
function sendEmailNothing2Process() { 

	prmFromDt=$1 
	prmToDt=$2 
	prmS3FldNFilePrefix=$3
	
	############################################################
	# Success email. 
	############################################################
	echo "" >> ${LOGNAME}
	echo "No S3 files found to process for ${prmS3FldNFilePrefix} between ${prmFromDt} and ${prmToDt}." >> ${LOGNAME}

	SUBJECT="DashboardInfo_SFUI (${ENVNAME})" 
	MSG="No S3 files found to process for ${prmS3FldNFilePrefix} between ${prmFromDt} and ${prmToDt}."

	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Error in calling sendEmail.py" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Sending Success email in DashboardInfo_SFUI.sh - Failed (${ENVNAME})"
		MSG="Sending Success email in DashboardInfo_SFUI.sh  has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi

}

	
function UpdateDashboardSFTables() { 
	
	#############################################################################################
	# Execute Python code to build S3 Job Details/Info json files AND update SF Dashboard tables
	#############################################################################################
	echo "" >> ${LOGNAME}
	echo "In function UpdateDashboardSFTables" >> ${LOGNAME}

	prmBucketnFldr=$1
	prmFilePrefix=$2
	S3LoadFileTmstmp=$3

	BucketFldrNFilePrefix=${prmBucketnFldr}${prmFilePrefix}
	echo "BucketFldrNFilePrefix=${BucketFldrNFilePrefix}"  >> ${LOGNAME}
			
	${PYTHON_COMMAND} ${RUNDIR}DashboardInfo_SFUI.py --BktFldrNFilePrefix ${BucketFldrNFilePrefix} --FromDate ${RUN_FROM_DT} --ToDate ${RUN_TO_DT} --TMSTMP ${S3LoadFileTmstmp} >> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [ $RET_STATUS -eq 4 ]; then
		echo "" >> ${LOGNAME}
		sendEmailNothing2Process ${RUN_FROM_DT} ${RUN_TO_DT} ${BucketFldrNFilePrefix}
		
		return 0
		
	elif [ $RET_STATUS != 0 ]; then	
		echo "" >> ${LOGNAME}
		echo "Python program DashboardInfo_SFUI.py failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Python program DashboardInfo_SFUI.py - Failed (${ENVNAME})"
		MSG="Python program DashboardInfo_SFUI.py failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi


	############################################################
	# Success email. 
	############################################################
	echo "" >> ${LOGNAME}
	echo "Send success email for load of Dashboard tables for ${prmFilePrefix} files for period ${RUN_FROM_DT} to ${RUN_TO_DT}." >> ${LOGNAME}

	SUBJECT="DashboardInfo_SFUI (${ENVNAME})" 
	MSG="The loading of the Dashboard tables with SF UI extract information for ${prmFilePrefix} files from ${RUN_FROM_DT} to ${RUN_TO_DT} has completed successfully."

	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Error in calling sendEmail.py" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Sending Success email in DashboardInfo_SFUI.sh - Failed (${ENVNAME})"
		MSG="Sending Success email in DashboardInfo_SFUI.sh  has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi


	############################################################
	# Define DASHBOARD json load files with correct timestamp
	############################################################
	DASHBOARD_JOBINFO_FILE=DASHBOARD_JOB_INFO_${S3LoadFileTmstmp}.json
	DASHBOARD_JOBDTLS_FILE=DASHBOARD_JOB_DTLS_EXTRACT_FILES_${S3LoadFileTmstmp}.json


	############################################################
	# Move Dashboard JOBINFO json file to S3 archive folder.
	############################################################
	echo "" >> ${LOGNAME}
	echo "Move S3 ${DASHBOARD_JOBINFO_FILE} file to S3 ${DASHBOARD_BUCKET}archive folder" >> ${LOGNAME}

	aws s3 mv s3://${DASHBOARD_BUCKET}${DASHBOARD_JOBINFO_FILE} s3://${DASHBOARD_BUCKET}archive/${DASHBOARD_JOBINFO_FILE}  1>> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Move S3 ${DASHBOARD_JOBINFO_FILE} file to S3 ${DASHBOARD_BUCKET}archive folder - failed." >> ${LOGNAME}
		
		# Send Failure email
		SUBJECT="DashboardInfo_SFUI.sh - Failed (${ENVNAME})"
		MSG="Move ${DASHBOARD_JOBINFO_FILE} file to S3 bucket ${DASHBOARD_BUCKET} archive folder failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi


	############################################################
	# Move Dashboard JOBDTLS json file to S3 archive folder.
	############################################################
	echo "" >> ${LOGNAME}
	echo "Move S3 ${DASHBOARD_JOBDTLS_FILE} file to S3 ${DASHBOARD_BUCKET}archive folder" >> ${LOGNAME}

	aws s3 mv s3://${DASHBOARD_BUCKET}${DASHBOARD_JOBDTLS_FILE} s3://${DASHBOARD_BUCKET}archive/${DASHBOARD_JOBDTLS_FILE}  1>> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Move S3 ${DASHBOARD_JOBDTLS_FILE} file to S3 ${DASHBOARD_BUCKET}archive folder - failed." >> ${LOGNAME}
		
		# Send Failure email
		SUBJECT="DashboardInfo_SFUI.sh - Failed (${ENVNAME})"
		MSG="Move ${DASHBOARD_JOBDTLS_FILE} file to S3 bucket ${DASHBOARD_BUCKET} archive folder failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi
	
}


#################################################################################
# Create and Update S3 Job Details/Info files for loading into SF
#################################################################################
if [ "${ProcessNonSFUIFiles}" = "Y" ];then

	echo "" >> ${LOGNAME}
	echo "Begin processing of ${parmOverrideFldrNFilePrefix} files"  >> ${LOGNAME}

	UpdateDashboardSFTables ${bucket} ${parmOverrideFldrNFilePrefix} ${TMSTMP}
	
else

	echo "" >> ${LOGNAME}
	echo "Begin processing of DOJ_SFUI files"  >> ${LOGNAME}

	UpdateDashboardSFTables ${DOJ_BUCKET} "DOJ_SFUI" "${TMSTMP}1"

	echo "" >> ${LOGNAME}
	echo "Begin processing of FOIA_SFUI files"  >> ${LOGNAME}

	UpdateDashboardSFTables ${FOIA_BUCKET} "FOIA_SFUI" "${TMSTMP}2"

fi


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME} 
echo "Remove temporary text files from data directory" >> ${LOGNAME} 


#############################################################
# end script
#############################################################
echo "" >> ${LOGNAME}
echo "DashboardInfo_SFUI.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS