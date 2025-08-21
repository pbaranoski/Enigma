#!/usr/bin/bash
############################################################################################################
# Script Name: DOJ_PartB_Standard.sh
# Description: This script executes the standard Part B layout with dynamic filter for DOJ/Ad-hoc requests 
#
# Author: BIT Team
# Created: 01/29/2024
#
# Modified:             Date:        Description:
# --------------------  -----------  -------------------------------------------------------------------
############################################################################################################

send_failure_email() {
#############################################################################
# Send Failure Email
# $1 local = SUBJECT LINE
# $2 local = MSG Body
#################################################################################
	EML_SUBJECT=$1
	EML_MSG=$2
	## TESTING
	printf "\nSUBJECT:\t%s\n" "${EML_SUBJECT}" >> ${LOGNAME}
	printf "MSG:\t\t%s\n" "${EML_MSG}" >> ${LOGNAME}
	#${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
	exit 12
}

copy_sql_filter_file() {
#############################################################################
# Copy SQL Filter file from S3 to local DATA DIR
#############################################################################
	printf "\nCopying SQL filter config file ${sql_filter_filename} from ${CONFIG_BUCKET}\n" >> ${LOGNAME}
	#aws s3 cp s3://${CONFIG_BUCKET}${sql_filter_filename} ${DATADIR}${sql_filter_filename} 1>> ${LOGNAME} 2>&1  

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		printf "\nCopying the SQL filter config file from S3 failed.\n" >> ${LOGNAME}
		SUBJECT="DOJ_PartB_Standard.sh script (${doj_title}) - Failed (${ENVNAME})"
		MSG="Copying SQL filter config file ${sql_filter_filename} from ${CONFIG_BUCKET} failed."
		send_failure_email "${SUBJECT}" "${MSG}"
	fi
	SQL_FILTER_FILE=${DATADIR}${sql_filter_filename}
	printf "\nFull path for SQL Filter File: %s\n" "${SQL_FILTER_FILE}" >> ${LOGNAME}
}

exec_python() {
#############################################################################
# Execute Python script DOJ_PartB_Standard.py
# If Counts-only option is selected, only the counts will be returned
# Otherwise, the full extract is produced under the supplied output filename
# Set SINGLE=TRUE option based on single_option parameter
#############################################################################
	case "${counts_option}" in
		Y) 
			COUNT=1
			;;
		N)
			COUNT=0
			;;
		*)
			SUBJECT="DOJ_PartB_Standard.sh script (${doj_title}) - Failed (${ENVNAME})"
			MSG="Invalid COUNTS OPTION value supplied. Expected Y/N"
			send_failure_email "${SUBJECT}" "${MSG}"
	esac
	
	case "${single_option}" in 
		Y)
			SINGLE="SINGLE=TRUE"
			;;
		N)
			SINGLE=""
			;;
		*)
			SUBJECT="DOJ_PartB_Standard.sh script (${doj_title}) - Failed (${ENVNAME})"
			MSG="Invalid SINGLE FILE value supplied. Expected Y/N"
			send_failure_email "${SUBJECT}" "${MSG}"
	esac

	export DATADIR
	export doj_title
	export OUTPUT_FN
	export SQL_FILTER_FILE
	export SINGLE
	export COUNT

	${PYTHON_COMMAND} ${RUNDIR}DOJ_PartB_Standard.py >> ${LOGNAME} 2>&1
	
	#################################################################################
	# Check the status of python script - Load Finder File
	#################################################################################
	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		printf "\nPython script DOJ_PTB_STANDARD.py failed\n" >> ${LOGNAME}
		SUBJECT="DOJ_PartB_Standard.sh script (${doj_title}) - Failed (${ENVNAME})"
		MSG="DOJ Part B Standard for ${doj_title} failed."
		send_failure_email "${SUBJECT}" "${MSG}"
	fi

	printf "\nPython script DOJ_PTB_STANDARD.py completed successfully.\n" >> ${LOGNAME}
}

combine_files() {
#################################################################################
# Combine part files
# If SINGLE = TRUE option selected, the combine script will exit without issue
#################################################################################
	printf "\nCombining files with prefix like: %s\n" "${OUTPUT_FN}" >> ${LOGNAME}
	${RUNDIR}CombineS3Files.sh ${S3BUCKET} ${EXT_OUTPUT_FILENAME} 

	if [[ $RET_STATUS != 0 ]]; then
		printf "\nCombining part files for %s failed.\n" "${OUTPUT_FN}" >> ${LOGNAME}
		SUBJECT="DOJ_PartB_Standard.sh script (${doj_title}) - Failed (${ENVNAME})"
		MSG="DOJ Part B Standard for ${doj_title} failed while calling CombineS3Files.sh."
		send_failure_email "${SUBJECT}" "${MSG}"
	fi

}

create_manifest_file() {
#################################################################################
# Create Manifest File
# S3BUCKET --> points to location of extract file. 
#          --> S3 folder is key token to config file to determine of manifest file is in HOLD status   
# TMSTMP   --> uniquely identifies extract file(s) 
# DOJ_EMAIL_SUCCESS_RECIPIENT --> manifest file recipients
#
# First check JIRA_Extract_Mappings.txt for the DOJ Title:
#   If not present, add it with the JIRA Ticket # and upload to S3
#   If present, continue with manifest file creation
#################################################################################
	printf "\nCreating Manifest File for Standard Part B DOJ request ${doj_title}\n" >> ${LOGNAME}
	
	#################################################################################
	# Download JIRA_Extracts_Mappings.txt from /config folder
	#################################################################################
	printf "\nDownloading JIRA_Extract_Mappings.txt to check for ${doj_title}\n\n" >> ${LOGNAME}
	aws s3 cp s3://${CONFIG_BUCKET}JIRA_Extract_Mappings.txt ${DATADIR} 1>> ${LOGNAME} 2>&1
	
	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		printf "\nCopying JIRA_Extract_Mappings.txt config file from S3 failed.\n" >> ${LOGNAME}
		SUBJECT="DOJ_PartB_Standard.sh script (${doj_title}) - Failed (${ENVNAME})"
		MSG="Copying JIRA_Extract_Mappings.txt config file from ${CONFIG_BUCKET} failed."
		send_failure_email "${SUBJECT}" "${MSG}"
	fi

	#################################################################################
	# Check/Update JIRA_Extracts_Mappings.txt in DATADIR
	#################################################################################	
	if grep -q ${doj_title} ${DATADIR}JIRA_Extract_Mappings.txt; then
		printf "\nDOJ title %s found in JIRA_Extract_Mappings.txt. Continuing to Manifest file creation.\n" "${doj_title}" >> ${LOGNAME}
	else
		MAP_NEW_LINE="${doj_title}=https://jiraent.cms.gov/browse/${jira_number}"
		printf "\nDOJ title %s NOT found in JIRA_Extract_Mappings.txt. Added new entry below:\n%s\n" "${doj_title}" "${MAP_NEW_LINE}" >> ${LOGNAME}
		echo ${MAP_NEW_LINE} >> ${DATADIR}JIRA_Extract_Mappings.txt
	fi
	
	#################################################################################
	# Upload new JIRA_Extracts_Mappings.txt to /config folder
	#################################################################################	
	#aws s3 cp ${DATADIR}JIRA_Extract_Mappings.txt s3://${CONFIG_BUCKET} 1>> ${LOGNAME} 2>&1
	
	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		printf "\nUploading JIRA_Extract_Mappings.txt config file to S3 failed.\n" >> ${LOGNAME}
		SUBJECT="DOJ_PartB_Standard.sh script (${doj_title}) - Failed (${ENVNAME})"
		MSG="Uploading JIRA_Extract_Mappings.txt config file to ${CONFIG_BUCKET} failed."
		send_failure_email "${SUBJECT}" "${MSG}"
	fi	
	
	#${RUNDIR}CreateManifestFile.sh ${S3BUCKET} ${TMSTMP} ${DOJ_EMAIL_SUCCESS_RECIPIENT}
}

script_cleanup() {
#################################################################################
# Script cleanup & success email
# If the full extract was executed, send email with filenames and counts
# If COUNTS only option was selected, parse log file for the result and send email
# Remove the SQL filter file from the data directory
#################################################################################
	printf "\nRemoving SQL file %s from the data directory.\n" "${sql_filter_filename}" >> ${LOGNAME}
	#rm ${DATADIR}${sql_filter_filename} >> ${LOGNAME} 2>&1

	if [[ "${counts_option}" -eq "Y" ]]; then
		printf "\nGetting COUNT results from logfile\n" >> ${LOGNAME}
		COUNT_RESULT=`awk '/^COUNT_HIC_NUM/{getline;print $1}' ${LOGNAME}`
		MSG="DOJ Part B Standard COUNTS completed successfully for ${doj_title}.\n\nCount Result: ${COUNT_RESULT}"
	else
		getExtractFilenamesAndCounts ${LOGNAME}  >> ${LOGNAME} 2>&1
		S3Files="${filenamesAndCounts}"  
		MSG="DOJ Part B Standard completed successfully for ${doj_title} completed successfully.\n\nThe Following file was created:\n\n${S3Files}"
	fi
	
	SUBJECT="DEV TEST -- DOJ_PartB_Standard.sh script (${doj_title}) - Completed (${ENVNAME})"
	printf "\nSUBJECT:\t%s\n" "${SUBJECT}" >> ${LOGNAME}
	printf "\nMSG:\t\t%s\n" "${MSG}" >> ${LOGNAME}
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		printf "\nError in calling sendEmail.py\n" >> ${LOGNAME}	
		SUBJECT="Sending Success email in DOJ_PartB_Standard.sh - Failed (${ENVNAME})"
		MSG="Sending Success email in DOJ_PartB_Standard.sh has failed."
		send_failure_email "${SUBJECT}" "${MSG}"
	fi
}

#################################################################################
# START
#################################################################################
set +x

LOGDIR=/app/IDRC/XTR/CMS/logs/
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/
source ${RUNDIR}SET_XTR_ENV.sh
S3BUCKET=${DOJ_BUCKET}
TMSTMP=`date +%Y%m%d.%H%M%S`

#################################################################################
# Get parameters sent from Rundeck
# Parameters will be forced at the Rundeck level. In the event the script is 
# executed from another source, check # of number of parameters first
#################################################################################
#tmpLOG=${LOGDIR}/DOJ_PTB_STANDARD_${TMSTMP}.log

#if ! [[ $# -eq 6 ]]; then
#	printf "Invalid number of parameters supplied. Expected 6 - See script for usage\n\n" >> ${tmpLOG}
#	exit 1
#fi

while getopts ":t:o:f:s:c:j:" opt; do
	case "${opt}" in
		t)
			doj_title=${OPTARG}
			;;
		o)
			output_filename=${OPTARG}
			;;
		f)
			sql_filter_filename=${OPTARG}
			;;
		s)
			single_option=${OPTARG}
			;;
		c)
			counts_option=${OPTARG}
			;;
		j)
			jira_number=${OPTARG}
			;;
		\?)
			printf "%s\n" "Invalid argument: ${OPTARG}" >> ${tmpLOG}
			exit 1
			;;
		*)
			exit 1
	esac
done

#################################################################################
# Establish log file and start process
#################################################################################
LOGNAME=/app/IDRC/XTR/CMS/logs/DOJ_PartB_Standard_${doj_title}_${TMSTMP}.log

touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

printf "###################################\n" >> ${LOGNAME}
printf "DOJ_PartB_Standard.sh started at `date`\n\n" >> ${LOGNAME}

# Remove any file extension from the output filename and append TMSTMP/.txt.gz ext
OUTPUT_FN=${output_filename%.*}_${TMSTMP}.txt.gz

printf "DOJ Part B Standard Layout process started with the following parameters:\n" >> ${LOGNAME}
printf "Title:\t\t%s\n" "${doj_title}" >> ${LOGNAME}
printf "Output File:\t%s\n" "${OUTPUT_FN}" >> ${LOGNAME}
printf "SQL Filter File:\t%s\n" "${sql_filter_filename}" >> ${LOGNAME}
printf "JIRA #:\t\t%s\n" "${jira_number}" >> ${LOGNAME}
printf "Single File:\t%s\n" "${single_option}" >> ${LOGNAME}
printf "Counts Only:\t%s\n" "${counts_option}" >> ${LOGNAME}

# Copy SQL Filter File from S3 to the DATA DIR
copy_sql_filter_file

# Execute Python script for execution; determine counts vs. full extract
exec_python

# Combine part files
#combine_files

# Create Manifest File
create_manifest_file

# script cleanup and send success email with counts
script_cleanup

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS
