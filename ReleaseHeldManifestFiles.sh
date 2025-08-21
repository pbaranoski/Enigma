#!/usr/bin/bash
######################################################################################
# Name:  ReleaseHeldManifestFiles.sh
#
# Desc: Release Held Manifest files. Migrate manifest files from S3://manifest_files_hold 
#       folder to S3://manifest_files folder one at a time with a delay defined in
#       configuration file S3://config/MANIFEST_FILE_PROCESS_CONFIG.txt  
#
# Execute as ./ReleaseHeldManifestFiles.sh $1 $2 $3  (600 DOJ_TRAVIS MickeyMouse@Disney.com )
#
# $1 = # of sleep seconds           Ex: 600  (Ignored for DOJ manifest files)
# $2 = HLQ of S3 manifest filename  Ex: DOJ_ANTI_TRUST   OR SAF_ENC   
# $3 = Box Recipient email addresses (comma-delimited list)  (OPTIONAL) (Ignored for DOJ manifest files)
#
# Created: Paul Baranoski  10/11/2023
# Modified:
#
# Paul Baranoski 2023-10-11 Created script. 
# Paul Baranoski 2024-01-09 Modify to process DOJ manifest files differently. 
#                           1) Do NOT move DOJ manifest files from hold folder to active folder.
#                           2) Do NOT modify recipient email addresses.
#                           3) Add call to ManifestFileReport.sh to report on released manifest files.
# Paul Baranoski 2024-05-17 Modify email constants. Use CMS_EMAIL_SENDER. Use ENIGMA_EMAIL_*
#####################################################################################
set +x


#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
LOGNAME=/app/IDRC/XTR/CMS/logs/ReleaseHeldManifestFiles_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

#default sleep time - default 10 minutes
SLEEP_SECS=600
	
	
touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "ReleaseHeldManifestFiles.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}


##################################################################
# Verify that required NOF parameters have been sent from RunDeck
##################################################################
if ! [[ $# -eq 2 || $# -eq 3  ]]
then
	echo "Incorrect # of parameters sent to script. NOF parameters: $#" >> ${LOGNAME}
	exit 12
fi


#############################################################
# Display parameters passed to script 
#############################################################
SLEEP_SECS=$1
ManifestFileHLQ=$2
OverrideRecipientEmailAddresses=$3

echo " " >> ${LOGNAME}
echo "Parameters to script: " >> ${LOGNAME}
echo "   SLEEP_SECS=${SLEEP_SECS} " >> ${LOGNAME}
echo "   ManifestFileHLQ=${ManifestFileHLQ} " >> ${LOGNAME}
echo "   OverrideRecipientEmailAddresses=${OverrideRecipientEmailAddresses} " >> ${LOGNAME}
echo " " >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh >> ${LOGNAME}

echo " " >> ${LOGNAME}
echo "MANIFEST_BUCKET=${MANIFEST_BUCKET} " >> ${LOGNAME}
echo "MANIFEST_HOLD_BUCKET=${MANIFEST_HOLD_BUCKET} " >> ${LOGNAME}

MANIFEST_CONFIG_FILE=MANIFEST_FILE_PROCESS_CONFIG.txt
echo "MANIFEST_CONFIG_FILE=${MANIFEST_CONFIG_FILE}" >> ${LOGNAME}


#############################################################
# function definitions  
#############################################################
function updateRecipientEmailAddresses () {


	echo "" >> ${LOGNAME}
	echo "In function updateRecipientEmailAddresses" >> ${LOGNAME}

	#############################################################
	# Copy manifest file from S3 Hold folder to data directory
	#############################################################		
	echo "" >> ${LOGNAME}
	echo "Copy manifest file ${manifest_filename} to data directory " >> ${LOGNAME}
	aws s3 cp s3://${MANIFEST_HOLD_BUCKET}${manifest_filename} ${DATADIR}${manifest_filename} 1>> ${LOGNAME} 2>&1 

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Copying S3 file ${manifest_filename} from s3://${MANIFEST_HOLD_BUCKET} to ${DATADIR} failed." >> ${LOGNAME}
		
		# Send Failure email
		SUBJECT="ReleaseHeldManifestFiles.sh  - Failed (${ENVNAME})"
		MSG="Copying S3 file ${manifest_filename} from s3://${MANIFEST_HOLD_BUCKET} to ${DATADIR} failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi	


	#############################################################
	# Replace Recipient addresses with override values
	#############################################################		
	echo "" >> ${LOGNAME}
	echo "Modify manifest file box recipient email addresses " >> ${LOGNAME}

	Str2Replace=`grep 'dataRecipientEmails' ${DATADIR}${manifest_filename} | sed 's/^[ ]*//g' `
	ReplacementStr="\"dataRecipientEmails\": \"${OverrideRecipientEmailAddresses}\", "
	
	echo "Str2Replace=${Str2Replace}" >> ${LOGNAME}
	echo "ReplacementStr=${ReplacementStr}" >> ${LOGNAME}
	
	sed -i "s|${Str2Replace}|${ReplacementStr}|g" ${DATADIR}${manifest_filename}  >> ${LOGNAME} 2>&1


	################################################################
	# Copy manifest file from data directory back to S3 hold folder
	################################################################		
	echo "" >> ${LOGNAME}
	echo "Copy manifest file ${manifest_filename} from data directory to S3 hold folder " >> ${LOGNAME}
	aws s3 mv ${DATADIR}${manifest_filename} s3://${MANIFEST_HOLD_BUCKET}${manifest_filename}  1>> ${LOGNAME} 2>&1 

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Moving S3 file ${manifest_filename} from ${DATADIR} to s3://${MANIFEST_HOLD_BUCKET} failed." >> ${LOGNAME}
		
		# Send Failure email
		SUBJECT="ReleaseHeldManifestFiles.sh  - Failed (${ENVNAME})"
		MSG="Moving S3 file ${manifest_filename} from ${DATADIR} to s3://${MANIFEST_HOLD_BUCKET} failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi

}


#############################################################
# Identify Manifest file type (Is it a DOJ manifest file?
#############################################################
echo " " >> ${LOGNAME}

ManifestFileType=`echo ${ManifestFileHLQ} | cut -d_ -f1 `  2>> ${LOGNAME}
echo "ManifestFileType=${ManifestFileType}" >> ${LOGNAME}


#############################################################
# Get list of manifest files
#############################################################
echo "" >> ${LOGNAME}
echo "Find Manifest files on hold: " >> ${LOGNAME}
aws s3 ls s3://${MANIFEST_HOLD_BUCKET}${ManifestFileHLQ}  >> ${LOGNAME}
		
# Ex. Total Objects: 14 --> " 14" --> "14"
NOF_FILES=`aws s3 ls s3://${MANIFEST_HOLD_BUCKET}${ManifestFileHLQ} --summarize | grep 'Total Objects' | cut -d: -f2 | sed 's/ //g' ` 2>> ${LOGNAME}

RET_STATUS=$?

if [[ $RET_STATUS = 0 ]]; then

	# We have files to process
	if [[ ${NOF_FILES} -gt 0 ]]; then

		echo "" >> ${LOGNAME}
		echo "List manifest files found:"  >> ${LOGNAME}
		
		MANIFEST_FILES=`aws s3 ls s3://${MANIFEST_HOLD_BUCKET}${ManifestFileHLQ} | awk '{print $4}' `  2>> ${LOGNAME}
		echo "MANIFEST_FILES=${MANIFEST_FILES}" >> ${LOGNAME}	
		
	else
		# No files to process/report on
		echo "" >> ${LOGNAME}
		echo "ReleaseHeldManifestFiles.sh - No manifest files to release in ${MANIFEST_HOLD_BUCKET} like ${ManifestFileHLQ}* " >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="ReleaseHeldManifestFiles.sh - No manifest files to release (${ENVNAME})"
		MSG="No manifest files to release in ${MANIFEST_HOLD_BUCKET} like ${ManifestFileHLQ}* "
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 0		
	
	fi
else

	echo "" >> ${LOGNAME}
	echo "Shell script ReleaseHeldManifestFiles.sh failed. (${ENVNAME})" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="ReleaseHeldManifestFiles.sh failed (${ENVNAME})"
	MSG="Listing manifest files from ${MANIFEST_HOLD_BUCKET}${ManifestFileHLQ} from S3 has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12

fi	


#############################################################
# Is it a DOJ manifest file? 
#############################################################
if [ "${ManifestFileType}" = "DOJ" ];then

	echo "" >> ${LOGNAME}
	echo "Processing DOJ manifest files " >> ${LOGNAME}

	#############################################################
	# Report on DOJ manifest files that are like a HLQ
	#   NOTE: this script will move reported manifest files to S3://Manifest_files_archive folder
	#############################################################
	${RUNDIR}ManifestFileReport.sh ${MANIFEST_HOLD_BUCKET} ${ManifestFileHLQ} >> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Moving S3 file ${manifest_filename} from s3://${MANIFEST_HOLD_BUCKET} to s3://${MANIFEST_BUCKET} failed." >> ${LOGNAME}
		
		# Send Failure email
		SUBJECT="ReleaseHeldManifestFiles.sh  - Failed (${ENVNAME})"
		MSG="Moving S3 file ${manifest_filename} from s3://${MANIFEST_HOLD_BUCKET} to s3://${MANIFEST_BUCKET} failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi	


else
	#############################################################
	# Non-DOJ manifest file 
	#############################################################
	echo "" >> ${LOGNAME}
	echo "Processing CMS BOX manifest files " >> ${LOGNAME}
	
	#############################################################
	# Process non-DOJ manifest files (CMS BOX) - one-at-a-time
	#############################################################
	for manifest_filename in ${MANIFEST_FILES} 
	do

		echo "" >> ${LOGNAME}
		echo "Processing manifest_filename=${manifest_filename}" >> ${LOGNAME}
		
		#############################################################
		# Are we overriding the Box recipient email addresses ?
		#############################################################
		if ! [[ "${OverrideRecipientEmailAddresses}" = "" ]];then 

			echo "" >> ${LOGNAME}
			echo "We are overriding the Box recipient email addresses with email address(es): ${OverrideRecipientEmailAddresses}  " >> ${LOGNAME}

			updateRecipientEmailAddresses
		fi


		#############################################################
		# Move manifest file from hold folder to active folder
		#############################################################
		echo "" >> ${LOGNAME}
		echo "Move manifest filename ${manifest_filename} from s3://${MANIFEST_HOLD_BUCKET} to s3://${MANIFEST_BUCKET} " >> ${LOGNAME}

		aws s3 mv s3://${MANIFEST_HOLD_BUCKET}${manifest_filename} s3://${MANIFEST_BUCKET}${manifest_filename}  1>> ${LOGNAME} 2>&1

		RET_STATUS=$?

		if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Moving S3 file ${manifest_filename} from s3://${MANIFEST_HOLD_BUCKET} to s3://${MANIFEST_BUCKET} failed." >> ${LOGNAME}
			
			# Send Failure email
			SUBJECT="ReleaseHeldManifestFiles.sh  - Failed (${ENVNAME})"
			MSG="Moving S3 file ${manifest_filename} from s3://${MANIFEST_HOLD_BUCKET} to s3://${MANIFEST_BUCKET} failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
		fi	
		
		
		#############################################################
		# Wait/sleep
		#############################################################
		echo "" >> ${LOGNAME}
		echo "Sleeping for ${SLEEP_SECS} seconds"  >> ${LOGNAME}
		echo " Starting to sleep at `date +%Y-%m-%d.%H:%M:%S` "  >> ${LOGNAME}

		sleep ${SLEEP_SECS}  

		echo " Woke up at `date +%Y-%m-%d.%H:%M:%S` "  >> ${LOGNAME}

	done 

fi		


#############################################################
# Set Status literal for email.
#############################################################
if [ "${ManifestFileType}" = "DOJ" ];then
	STATUS="archive folder"
else
	STATUS="active folder"
fi

	
#############################################################
# Send Success email.
#############################################################
echo "" >> ${LOGNAME}
echo "Send success email with moved S3 Manifest filenames." >> ${LOGNAME}
echo "MANIFEST_FILES=${MANIFEST_FILES} "   >> ${LOGNAME}

SUBJECT="Process Held Manifest Files (${ENVNAME})" 
MSG="Processing Held Manifest Files has completed.\n\nThe following file(s) were moved to ${STATUS}:\n\n${MANIFEST_FILES}"

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Error in calling sendEmail.py" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Sending Success email in ReleaseHeldManifestFiles.sh - Failed (${ENVNAME})"
		MSG="Sending Success email in ReleaseHeldManifestFiles.sh has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
fi	


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "ReleaseHeldManifestFiles.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS



