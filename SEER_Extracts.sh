#!/usr/bin/bash
#
######################################################################################
# Name:  SEER_Extracts.sh
#
# Desc: Create SEER Extract file for each Request/Finder File sent by IMS.
#
#
# Created: Paul Baranoski  09/17/2024
# Modified: 
#
# Paul Baranoski 2024-09-17 Create script.
######################################################################################
set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`

# Export TMSTMP variable for child scripts
export TMSTMP 

LOGNAME=/app/IDRC/XTR/CMS/logs/SEER_Extracts_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "SEER_Extracts.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh >> ${LOGNAME}

LOGDIR=${LOG_PATH}/

S3BUCKET=${SEER_BUCKET} 

# SEER_REQUEST_{Sender}_DYYYYMMDD.csv	
PREFIX=SEER_REQUEST_

echo "SEER bucket=${S3BUCKET}" >> ${LOGNAME}
echo "Finder file bucket=${FINDER_FILE_BUCKET}" >> ${LOGNAME}

source ${RUNDIR}FilenameCounts.bash


function archiveRequestFile() { 

	#############################################################
	# Move Finder File in S3 to archive folder
	#############################################################
	echo " " >> ${LOGNAME}
	echo "Moving S3 SEER Finder file ${SEER_FNDR_FILE} to S3 archive folder." >> ${LOGNAME}
	
	aws s3 mv s3://${FINDER_FILE_BUCKET}${SEER_FNDR_FILE} s3://${FINDER_FILE_BUCKET}archive/${SEER_FNDR_FILE}  1>> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Moving S3 SEER Finder file to S3 archive folder failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="SEER Extract - Failed ($ENVNAME)"
		MSG="Moving S3 Finder file to S3 archive folder failed.  ( ${FINDER_FILE_BUCKET}${S3Filename} to ${FINDER_FILE_BUCKET}archive/${S3Filename} )"
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi	

	
	#############################################################
	# Delete Finder File in Linux
	#############################################################
	echo "" >> ${LOGNAME}
	echo "Delete finder file ${DATADIR}${SEER_FNDR_FILE} from linux data directory." >> ${LOGNAME}
	rm ${DATADIR}${SEER_FNDR_FILE} 2>> ${LOGNAME}

}

function getNOFFILES4ManifestFile() { 

	#############################################################
	# Get list of S3 files to include in manifest.
	#############################################################
	echo "" >> ${LOGNAME}
	echo "Count NOF extract files to include in the manifest file " >> ${LOGNAME}

	NOF_FILES_4_MANIFEST=`aws s3 ls s3://${S3BUCKET} | grep ${TMSTMP} | wc -l `

	RET_STATUS=$?

	if [ $RET_STATUS != 0 ]; then
		echo "" >> ${LOGNAME}
		echo "Error in getting count of extract files to include in manifest file. S3 Bucket ${S3BucketAndFldr} " >> ${LOGNAME}

		exit 12
	fi

	echo "NOF_FILES_4_MANIFEST=${NOF_FILES_4_MANIFEST}" >> ${LOGNAME}
	
}


#################################################################################
# Are there any SEER Extract/Finder files in S3?
#################################################################################
echo "" >> ${LOGNAME}
echo "Count NOF SEER Request/Finder files found in ${FINDER_FILE_BUCKET}" >> ${LOGNAME}

NOF_FILES=`aws s3 ls s3://${FINDER_FILE_BUCKET}${PREFIX} --summarize | grep 'Total Objects' | cut -d: -f2 | sed 's/ //g' ` 2>> ${LOGNAME}

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Counting NOF S3 SEER Finder files in s3://${FINDER_FILE_BUCKET}${PREFIX} failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="SEER Extract - Failed ($ENVNAME)"
	MSG="Counting NOF S3 SEER Finder files in s3://${FINDER_FILE_BUCKET}${PREFIX} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	

echo "NOF_FILES=${NOF_FILES}"  >> ${LOGNAME}


#################################################
# If 0 finder files --> end gracefully		
#################################################
if [ ${NOF_FILES} -eq 0 ];then 
	echo "" >> ${LOGNAME}
	echo "There are no S3 SEER Finder files to process in s3://${FINDER_FILE_BUCKET}${PREFIX}." >> ${LOGNAME}
	
	# Send Info email	
	SUBJECT="SEER Extract ended - nothing to process ($ENVNAME)"
	MSG="There are no S3 SEER Finder files to process in s3://${FINDER_FILE_BUCKET}${PREFIX}."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	
	echo "" >> ${LOGNAME}
	echo "SEER_Extracts.sh completed successfully." >> ${LOGNAME}

	echo "Ended at `date` " >> ${LOGNAME}
	echo "" >> ${LOGNAME}

	exit 0

fi 


#################################################
# Get list of S3 SEER Extract/Finder Files.		
#################################################
echo "" >> ${LOGNAME}
echo "Get list of SEER Finder Files in S3 " >> ${LOGNAME}

Files2Process=`aws s3 ls s3://${FINDER_FILE_BUCKET}${PREFIX} | awk '{print $4}'` 2>> ${LOGNAME}

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Counting NOF S3 SEER Finder files in s3://${FINDER_FILE_BUCKET}${PREFIX} failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="SEER Extract - Failed ($ENVNAME)"
	MSG="Counting NOF S3 SEER Finder files in s3://${FINDER_FILE_BUCKET}${PREFIX} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	

echo "Files2Process=${Files2Process}"  >> ${LOGNAME}
		
		
#################################################################################
# Loop thru SEER Extract/Finder files in data directory.
#################################################################################
echo "" >> ${LOGNAME}

for SEER_FNDR_FILE in ${Files2Process}
do

	echo "" >> ${LOGNAME}
	echo "******************************" >> ${LOGNAME}
	echo "Processing ${SEER_FNDR_FILE}" >> ${LOGNAME}


 	#################################################
	# Verify that file extension is a .txt file	
	#################################################
	FF_EXT=`echo ${SEER_FNDR_FILE} | cut -d. -f2 `
	echo "FF_EXT=${FF_EXT}"  >> ${LOGNAME}
	
	if [ "${FF_EXT}" != "txt" ];then
		echo "" >> ${LOGNAME}
		echo "Request file ${SEER_FNDR_FILE} has incorrect file extension. File cannot be processed. " >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="SEER Extract - Failed ($ENVNAME)"
		MSG="Request file ${SEER_FNDR_FILE} has incorrect file extension. File cannot be processed. Please correct and re-submit file as txt file."
		
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${SEER_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}"   >> ${LOGNAME} 2>&1

		# migrate finder file to archive folder
		archiveRequestFile
		
		# process next Finder File
		continue
		
	fi	

 	#################################################
	# Verify Request filename matches expected format
	# Ex. SEER_REQUEST_{ID-Node}_YYYYMMDD.txt
	# NOTE: 1) No double underscores.
	#       2) Unique ID contains no spaces or special character except dash
 	#################################################
	VALID_FILE_FORMAT=`echo "${SEER_FNDR_FILE}" | egrep -c '^SEER_REQUEST_[a-zA-Z0-9-]+_[0-9]+.txt$' ` 
	
	if [ ${VALID_FILE_FORMAT} -eq 0 ];then
		echo "" >> ${LOGNAME}
		echo "Request file ${SEER_FNDR_FILE} is named incorrectly. " >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="SEER Extract - Failed ($ENVNAME)"
		MSG="Request file ${SEER_FNDR_FILE} is named incorrectly. Please ensure that filename follows this pattern: SEER_REQUEST_{UNIQ-ID}_YYYYMMDD.csv. Please correct and re-submit file with proper filename."
		
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${SEER_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}"   >> ${LOGNAME} 2>&1

		# migrate finder file to archive folder
		archiveRequestFile
		
		# process next Finder File
		continue
		
	fi
	
 	#################################################
	# Extract FF ID NODE to use for extract files
	# Ex. SEER_REQUEST_{ID-Node}_YYYYMMDD.txt
 	#################################################
	FF_ID_NODE=`echo ${SEER_FNDR_FILE} | cut -d_ -f3 ` 	2>> ${LOGNAME}
	echo "FF_ID_NODE=${FF_ID_NODE}"  >> ${LOGNAME}
	
	
	#################################################
	# Copy SEER Extract/Finder File to linux.		
	#################################################
	echo "" >> ${LOGNAME}
	echo "Copy S3 Finder File s3://${FINDER_FILE_BUCKET}${SEER_FNDR_FILE} to linux " >> ${LOGNAME}

	aws s3 cp s3://${FINDER_FILE_BUCKET}${SEER_FNDR_FILE} ${DATADIR}${SEER_FNDR_FILE}  1>> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Copying S3 Finder File s3://${FINDER_FILE_BUCKET}${SEER_FNDR_FILE} to ${DATADIR}${SEER_FNDR_FILE} failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="SEER Extract - Failed ($ENVNAME)"
		MSG="Copying S3 Finder File s3://${FINDER_FILE_BUCKET}${SEER_FNDR_FILE} to linux datadir failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi	


	##############################################
	# Cleansing of request file
	##############################################
	# Perform sed on file to remove carrier returns
	sed -i 's/\r//g' ${DATADIR}${SEER_FNDR_FILE} 2>> ${LOGNAME}

	# Perform sed to remove any non-display characters, non-UTF characters. Thanks Monica!
	LC_ALL=C sed -i 's/[\x80-\xff]//g' ${DATADIR}${SEER_FNDR_FILE} 2>> ${LOGNAME}

	# Add ending newline character in case its missing for last record. (Occurs with files created in Windows). 
	printf "\n" >>  ${DATADIR}${SEER_FNDR_FILE}	

	
	##############################################
	# Load SEER Request/Finder File
	##############################################
	export SEER_FNDR_FILE
	
	echo " " >> ${LOGNAME}
	echo "Load SEER Request/Finder File " >> ${LOGNAME}
	${RUNDIR}LOAD_SEER_FNDR_FILE.py  >> ${LOGNAME} 2>&1


	#############################################################
	# Check the status of extract script
	#############################################################
	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Python script LOAD_SEER_FNDR_FILE.py failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Python script SEER_Extracts.py - Failed ($ENVNAME)"
		MSG="Python script SEER_Extracts.py  has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi


	##############################################
	# Extract SEER records using FF.
	##############################################
	export TMSTMP
	export FF_ID_NODE

	echo " " >> ${LOGNAME}
	echo "Extract SEER data for Request File ${SEER_FNDR_FILE} " >> ${LOGNAME}
	${RUNDIR}SEER_Extracts.py  >> ${LOGNAME} 2>&1


	#############################################################
	# Check the status of extract script
	#############################################################
	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Python script SEER_Extracts.py failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Python script SEER_Extracts.py - Failed ($ENVNAME)"
		MSG="Python script SEER_Extracts.py  has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi


	#############################################################
	# Move Finder File in S3 to archive folder
	#############################################################
	echo " " >> ${LOGNAME}
	echo "Moving S3 SEER Finder file ${SEER_FNDR_FILE} to S3 archive folder." >> ${LOGNAME}
	
	aws s3 mv s3://${FINDER_FILE_BUCKET}${SEER_FNDR_FILE} s3://${FINDER_FILE_BUCKET}archive/${SEER_FNDR_FILE}  1>> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Moving S3 SEER Finder file to S3 archive folder failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="SEER Extract - Failed ($ENVNAME)"
		MSG="Moving S3 Finder file to S3 archive folder failed.  ( ${FINDER_FILE_BUCKET}${S3Filename} to ${FINDER_FILE_BUCKET}archive/${S3Filename} )"
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi	

	
	#############################################################
	# Delete Finder File in Linux
	#############################################################
	echo "" >> ${LOGNAME}
	echo "Delete finder file ${DATADIR}${SEER_FNDR_FILE} from linux data directory." >> ${LOGNAME}
	rm ${DATADIR}${SEER_FNDR_FILE} 2>> ${LOGNAME}

	
done

#############################################################
# Get list of S3 files and record counts for success email.
#############################################################
echo "" >> ${LOGNAME}
echo "Get S3 Extract file list and record counts" >> ${LOGNAME}

getExtractFilenamesAndCounts ${LOGNAME}  >> ${LOGNAME} 2>&1
S3Files="${filenamesAndCounts}" 

echo "" >> ${LOGNAME}
S3Files=`echo "${S3Files}" ` >> ${LOGNAME}  2>&1


#############################################################
# Create Manifest file.
#############################################################
echo "" >> ${LOGNAME}
echo "Create Manifest file for SEER Request Extract.  " >> ${LOGNAME}

# Get Count of NOF Extract Files to include in manifest file
getNOFFILES4ManifestFile

if [ ${NOF_FILES_4_MANIFEST} -eq 0 ];then

	echo "No manifest file to create for SEER Request Extract.  " >> ${LOGNAME}

	#############################################################
	# Send success email of SEER Extract files
	#############################################################
	echo "" >> ${LOGNAME}
	echo "Send success email." >> ${LOGNAME}

	# Send Success email	
	SUBJECT="SEER Extract - completed ($ENVNAME)"
	MSG="SEER Extract completed for request files: ${Files2Process}. \n\nThe following extract files were created:\n\n${S3Files}\n\nNo manifest file was created."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${SEER_SUCCESS_EMAIL_RECIPIENT}" "${SUBJECT}" "${MSG}"   >> ${LOGNAME} 2>&1

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Error in calling sendEmail.py" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Sending Success email in SEER_Extract.sh  - Failed (${ENVNAME})"
		MSG="Sending Success email in SEER_Extract.sh has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi
	
else	
	#####################################################
	# S3BUCKET --> points to location of extract file. 
	#          --> S3 folder is key token to config file to determine of manifest file is in HOLD status   
	# TMSTMP   --> uniquely identifies extract file(s) 
	# EMAIL_BOX_RECIPIENT --> manifest file recipients
	# MANIFEST_HOLD_BUCKET --> overide destination for manifest file
	#
	# Ex. CreateManifestFile.sh s3://aws-hhs-cms-eadg-bia-ddom-extracts/xtr/DSH/ 20231211.125522 pbaranoski-con@index.com 
	#####################################################
	echo "Creating manifest file for SEER Request Extract.  " >> ${LOGNAME}
	
	${RUNDIR}CreateManifestFile.sh ${S3BUCKET} ${TMSTMP} "${SEER_BOX_RECIPIENTS}" 

	#############################################################
	# Check the status of script
	#############################################################
	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Shell script CreateManifestFile.sh failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Create Manifest file in SEER_Extracts.sh  - Failed ($ENVNAME)"
		MSG="Create Manifest file in SEER_Extracts.sh  has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi	


	#############################################################
	# Send success email of SEER Extract files
	#############################################################
	echo "" >> ${LOGNAME}
	echo "Send success email." >> ${LOGNAME}

	# Send Success email	
	SUBJECT="SEER Extract - completed ($ENVNAME)"
	MSG="SEER Extract completed for request files:\n ${Files2Process}. \n\nThe following extract files were created:\n\n${S3Files}\n\nThe manifest file is SEER_EXTRACT_Manifest_${TMSTMP}.json"
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${SEER_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}"   >> ${LOGNAME} 2>&1

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Error in calling sendEmail.py" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Sending Success email in SEER_Extract.sh  - Failed (${ENVNAME})"
		MSG="Sending Success email in SEER_Extract.sh has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi	
fi
	
	
#############################################################
# clean-up linux data directory
#############################################################
echo "" >> ${LOGNAME} 
echo "Remove temp files from data directory" >> ${LOGNAME} 

 
#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "SEER_Extracts.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS
