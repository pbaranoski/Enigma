#!/usr/bin/bash
############################################################################################################
# Script Name: DashboardInfo_GUI.sh
#
# Description: This script will process json SF table load files which have been manually loaded into the S3://Dashboard folder.
#      
#              Ex.DASHBOARD_GUI_JOB_INFO_20250923.151850.json and DASHBOARD_GUI_JOB_DTLS_20250923.151850.json 		
# 
#              The normal Dashboard script/process that parses the logs and the Dashboard SFUI script/process
#              build the SF load files as well as process them. Their companion python modules knows which 
#              SF load files to process since the exact filenames to load are passed from the shell script
#              to the python module. This means any DASHBOARD_GUI load files will not be accidentally processed
#              by either of those processes.
#
#              Since the SF load files to be processed are created outside of the scripts that will process them,
#              this script will look for any DASHBOARD_GUI SF load files in s3://Dashboard bucket/folder and process 
#              them. After processing each load file, it will be moved to the s3://Dashboard/archive folder to 
#              prevent re-processing of the files.
#
#
# Execute script with no parameters
#  ./DashboardInfo_GUI.sh 
# 
#
# Paul Baranoski 2025-04-07 Created script.
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/DashboardInfo_GUI_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "DashboardInfo_GUI.sh started at `date` " >> ${LOGNAME}

	
#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh 

echo "" >> ${LOGNAME}
echo "DASHBOARD_BUCKET=${DASHBOARD_BUCKET}" >> ${LOGNAME}

DASHBOARD_GUI_HLQ=DASHBOARD_GUI

#################################################################
# Get List of DASHBOARD_GUI Load files in s3://Dashboard folder
# and download to DATADIR.
#
# NOTE: Send email when there are no files to process.
#################################################################
echo "" >> ${LOGNAME}
echo "Are there Dashboard GUI SF load files to process? " >> ${LOGNAME}
		
# Ex. Total Objects: 14 --> " 14" --> "14"
NOF_FILES=`aws s3 ls s3://${DASHBOARD_BUCKET}${DASHBOARD_GUI_HLQ} --summarize | grep 'Total Objects' | cut -d: -f2 | sed 's/ //g' ` 2>> ${LOGNAME}

RET_STATUS=$?

if [[ $RET_STATUS = 0 ]]; then

	# We have files to process
	if [[ ${NOF_FILES} -gt 0 ]]; then

		echo "" >> ${LOGNAME}
		echo "List Dashboard GUI load files found:"  >> ${LOGNAME}
		
		DASHBOARD_GUI_FILES=`aws s3 ls s3://${DASHBOARD_BUCKET}${DASHBOARD_GUI_HLQ} | awk '{print $4}' `  2>> ${LOGNAME}
		echo "DASHBOARD_GUI_FILES=${DASHBOARD_GUI_FILES}" >> ${LOGNAME}	
		
	else
		# No files to process/report on
		echo "" >> ${LOGNAME}
		echo "DashboardInfo_GUI.sh - No DASHBOARD_GUI files to process in ${DASHBOARD_BUCKET} like ${DASHBOARD_GUI_HLQ}* " >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="DashboardInfo_GUI.sh - No Dashboard GUI load files found to process. (${ENVNAME})"
		MSG="No Dashboard GUI load files found to process in ${DASHBOARD_BUCKET} like ${DASHBOARD_GUI_HLQ}* "
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 0		
	
	fi
else

	echo "" >> ${LOGNAME}
	echo "Shell script DashboardInfo_GUI.sh failed. (${ENVNAME})" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="DashboardInfo_GUI.sh failed (${ENVNAME})"
	MSG="Listing manifest files from ${DASHBOARD_BUCKET}${DASHBOARD_GUI_HLQ} from S3 has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12

fi	


#################################################################
# Sort the list of files by:
#   1) the 5th node (timestamp) (so the matching INFO and DTLS files
#      are processed together.
#   2) then the 4th node in descending order (INFO before DTLS)
#
# Example:
#     DASHBOARD_GUI_JOB_INFO_20250923.151850.json
#     DASHBOARD_GUI_JOB_DTLS_20250923.151850.json
#     DASHBOARD_GUI_JOB_INFO_20250924.151850.json
#     DASHBOARD_GUI_JOB_DTLS_20250924.151850.json
#
# -t delimter is "_", -k5,5n is 5th node, numeric sort,
# -k4r (sort in descending order: INFO before DTLS) 
#################################################################
DASHBOARD_GUI_s3Files2Process=`echo "${DASHBOARD_GUI_FILES}" | sort -t_ -k5,5n -k4,4r` 2>> ${LOGNAME} 

echo "" >> ${LOGNAME}
echo "DASHBOARD_GUI_s3Files2Process=${DASHBOARD_GUI_s3Files2Process}" >> ${LOGNAME}


#################################################################
# Init loop variables
#################################################################
echo "" >> ${LOGNAME}
echo "Initialize loop variables" >> ${LOGNAME}

bJOB_INFO_EXISTS=0
bJOB_DTLS_EXISTS=0
DASHBOARD_JOBINFO_FILE=""
DASHBOARD_JOBDTLS_FILE=""


#################################################################
# Loop thru DASHBOARD_GUI companion files (JOB_INFO and JOB_DTLS)
#################################################################
echo "Process GUI S3 Files" >> ${LOGNAME}

for DASHBOARD_GUI_s3File in ${DASHBOARD_GUI_s3Files2Process}
do
	echo "" >> ${LOGNAME}
	echo "Next DASHBOARD_GUI_s3File to process: ${DASHBOARD_GUI_s3File}" >> ${LOGNAME}

	#################################################################
	# Count NOF Files read. Process when there are an even number of 
	# files. (NOTE: Meaning both companion load files should be present.
	#################################################################
	NOF_FILES_READ=$(( ${NOF_FILES_READ} + 1 ))
	echo "NOF_FILES_READ=${NOF_FILES_READ}"  >> ${LOGNAME}

	#################################################################
	# Load appropriate s3 Load filename variables, set boolean flags
	#################################################################	
	bIsItJOB_INFO_FILE=`echo "${DASHBOARD_GUI_s3File}" | grep -c 'JOB_INFO' `  2>> ${LOGNAME}
	echo "bIsItJOB_INFO_FILE=${bIsItJOB_INFO_FILE}" >> ${LOGNAME} 
	
	if [ ${bIsItJOB_INFO_FILE} -eq 1 ];then
		bJOB_INFO_EXISTS=1

		DASHBOARD_JOBINFO_FILE=${DASHBOARD_GUI_s3File} 
		echo "DASHBOARD_JOBINFO_FILE=${DASHBOARD_JOBINFO_FILE}"  >> ${LOGNAME}

		JOB_INFO_TMSTMP=`echo "${DASHBOARD_JOBINFO_FILE}" | cut -d_ -f5 ` 2>> ${LOGNAME}
		echo "JOB_INFO_TMSTMP=${JOB_INFO_TMSTMP}" >> ${LOGNAME}
	else
		bJOB_DTLS_EXISTS=1

		DASHBOARD_JOBDTLS_FILE=${DASHBOARD_GUI_s3File} 
		echo "DASHBOARD_JOBDTLS_FILE=${DASHBOARD_JOBDTLS_FILE}"  >> ${LOGNAME}

		JOB_DTLS_TMSTMP=`echo "${DASHBOARD_JOBDTLS_FILE}" | cut -d_ -f5 ` 2>> ${LOGNAME}
		echo "JOB_DTLS_TMSTMP=${JOB_DTLS_TMSTMP}" >> ${LOGNAME}

	fi

	#################################################################
	# Need both JOB_INFO and JOB_DTLS files to be present to process
	#################################################################
	if [ $(( ${NOF_FILES_READ} % 2 )) -eq 0 ];then
		echo "NOF_FILES_READ is even" >> ${LOGNAME}

		###############################################################################
		# File timestamps need to match --> ensure they are the correct pair to process
		###############################################################################		
		if  [[ ${bJOB_INFO_EXISTS} -eq 1 && ${bJOB_DTLS_EXISTS} -eq 1 ]];then
		
			if [ "${JOB_INFO_TMSTMP}" !=  "${JOB_DTLS_TMSTMP}" ];then
				# JOB_INFO and JOB_DTLS files are not correct paired files
				echo "" >> ${LOGNAME}
				echo "Program DashboardInfo_GUI.sh failed. " >> ${LOGNAME}
				echo "JOB_INFO and JOB_DTLS files are not correct paired files. \n\nDASHBOARD_JOBINFO_FILE=${DASHBOARD_JOBINFO_FILE} \nDASHBOARD_JOBDTLS_FILE=${DASHBOARD_JOBDTLS_FILE}" >> ${LOGNAME}
				
				# Send Failure email	
				SUBJECT="Program DashboardInfo_GUI.sh - Failed (${ENVNAME})"
				MSG="Program DashboardInfo_GUI.sh failed. JOB_INFO and JOB_DTLS files are not correct paired files. \n\nDASHBOARD_JOBINFO_FILE=${DASHBOARD_JOBINFO_FILE} \nDASHBOARD_JOBDTLS_FILE=${DASHBOARD_JOBDTLS_FILE}"
				${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

				exit 12			
			
			fi
		else
			# One of the paired files is missing
			echo "" >> ${LOGNAME}
			echo "Program DashboardInfo_GUI.sh failed. " >> ${LOGNAME}
			echo "One of the paired Dashboard GUI files is missing. \n\nDASHBOARD_JOBINFO_FILE=${DASHBOARD_JOBINFO_FILE} \nDASHBOARD_JOBDTLS_FILE=${DASHBOARD_JOBDTLS_FILE}" >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="Program DashboardInfo_GUI.sh - Failed (${ENVNAME})"
			MSG="Program DashboardInfo_GUI.sh failed. One of the paired Dashboard GUI files is missing. \n\nDASHBOARD_JOBINFO_FILE=${DASHBOARD_JOBINFO_FILE} \nDASHBOARD_JOBDTLS_FILE=${DASHBOARD_JOBDTLS_FILE}"
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
		fi
	else
		# We need to know the two paired load filenames before processing them.
		continue
	fi		

	
	#################################################################
	# Execute python module to process the 2 json load files to 
	#  update SF tables. 
	#################################################################
	echo "" >> ${LOGNAME}
	echo "Start execution of Dashboard_GUI.py program"  >> ${LOGNAME}

	# Export environment variables for Python code
	export DASHBOARD_JOBINFO_FILE
	export DASHBOARD_JOBDTLS_FILE

	${PYTHON_COMMAND} ${RUNDIR}DashboardInfo_GUI.py >> ${LOGNAME} 2>&1

	#################################################################################
	# Check the status of python script
	#################################################################################
	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Python prgoram DashboardInfo_GUI.py failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Python program DashboardInfo_GUI.py - Failed (${ENVNAME})"
		MSG="Python program DashboardInfo_GUI.py failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi

	
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
		SUBJECT="DashboardInfo_MS.sh - Failed (${ENVNAME})"
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
		SUBJECT="DashboardInfo_MS.sh - Failed (${ENVNAME})"
		MSG="Move ${DASHBOARD_JOBDTLS_FILE} file to S3 bucket ${DASHBOARD_BUCKET} archive folder failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi	

	############################################################
	# Success email for each set of load files
	############################################################
	echo "" >> ${LOGNAME}
	echo "Send success email for load of Dashboard tables using load files: \n\nDASHBOARD_JOBINFO_FILE=${DASHBOARD_JOBINFO_FILE} \nDASHBOARD_JOBDTLS_FILE=${DASHBOARD_JOBDTLS_FILE}" >> ${LOGNAME}

	SUBJECT="DashboardInfo_GUI (${ENVNAME})" 
	MSG="The loading of the Dashboard tables with GUI Load files (listed below) has completed successfully. \n\nDASHBOARD_JOBINFO_FILE=${DASHBOARD_JOBINFO_FILE} \nDASHBOARD_JOBDTLS_FILE=${DASHBOARD_JOBDTLS_FILE}"

	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Error in calling sendEmail.py" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Sending Success email in DashboardInfo_GUI.sh - Failed (${ENVNAME})"
		MSG="Sending Success email in DashboardInfo_GUI.sh  has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi	


	#################################################################
	# Initialize loop variables for next set of load files to process
	#################################################################
	bJOB_INFO_EXISTS=0
	bJOB_DTLS_EXISTS=0
	DASHBOARD_JOBINFO_FILE=""
	DASHBOARD_JOBDTLS_FILE=""
	
done


#################################################################
# If there were an odd number of Load files read  
#  --> an incomplete set of load files was not fully processed. 
#################################################################
if [ $(( ${NOF_FILES_READ} % 2 )) -eq 1 ];then
	echo "" >> ${LOGNAME}
	echo "Program DashboardInfo_GUI.sh failed." >> ${LOGNAME}
	echo "Python program DashboardInfo_GUI.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Program DashboardInfo_GUI.sh - Failed (${ENVNAME})"
	MSG="Program DashboardInfo_GUI.sh failed. One of the paired Dashboard GUI files is missing. \nDASHBOARD_JOBINFO_FILE=${DASHBOARD_JOBINFO_FILE} \nDASHBOARD_JOBDTLS_FILE=${DASHBOARD_JOBDTLS_FILE}"
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi			

exit 0 



#############################################################
# functions
#############################################################
	
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
			
	${PYTHON_COMMAND} ${RUNDIR}DashboardInfo_GUI.py --BktFldrNFilePrefix ${BucketFldrNFilePrefix} --FromDate ${RUN_FROM_DT} --ToDate ${RUN_TO_DT} --TMSTMP ${S3LoadFileTmstmp} >> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [ $RET_STATUS -eq 4 ]; then
		echo "" >> ${LOGNAME}
		sendEmailNothing2Process ${RUN_FROM_DT} ${RUN_TO_DT} ${BucketFldrNFilePrefix}
		
		return 0
		
	elif [ $RET_STATUS != 0 ]; then	
		echo "" >> ${LOGNAME}
		echo "Python program DashboardInfo_GUI.py failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Python program DashboardInfo_GUI.py - Failed (${ENVNAME})"
		MSG="Python program DashboardInfo_GUI.py failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi


	############################################################
	# Success email. 
	############################################################
	echo "" >> ${LOGNAME}
	echo "Send success email for load of Dashboard tables for ${prmFilePrefix} files for period ${RUN_FROM_DT} to ${RUN_TO_DT}." >> ${LOGNAME}

	SUBJECT="DashboardInfo_GUI (${ENVNAME})" 
	MSG="The loading of the Dashboard tables with SF UI extract information for ${prmFilePrefix} files from ${RUN_FROM_DT} to ${RUN_TO_DT} has completed successfully."

	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Error in calling sendEmail.py" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Sending Success email in DashboardInfo_GUI.sh - Failed (${ENVNAME})"
		MSG="Sending Success email in DashboardInfo_GUI.sh  has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi


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
		SUBJECT="DashboardInfo_GUI.sh - Failed (${ENVNAME})"
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
		SUBJECT="DashboardInfo_GUI.sh - Failed (${ENVNAME})"
		MSG="Move ${DASHBOARD_JOBDTLS_FILE} file to S3 bucket ${DASHBOARD_BUCKET} archive folder failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi
	
}


#################################################################################
# Create and Update S3 Job Details/Info files for loading into SF
#################################################################################
echo "" >> ${LOGNAME}
echo "Begin processing of FOIA_SFUI files"  >> ${LOGNAME}

UpdateDashboardSFTables ${FOIA_BUCKET} "FOIA_SFUI" "${TMSTMP}"



#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME} 
echo "Remove temporary text files from data directory" >> ${LOGNAME} 


#############################################################
# end script
#############################################################
echo "" >> ${LOGNAME}
echo "DashboardInfo_GUI.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS