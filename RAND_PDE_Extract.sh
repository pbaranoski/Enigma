#!/usr/bin/bash
############################################################################################################
# Name: RAND_PDE_Extract.sh
#
# Desc: RAND PDE Extract
#
# Author     : Viren Khanna
# Created    : 3/07/2023
#
# Modified:
# Viren Khanna 	2023-03-07 	New script.
# Paul Baranoski    2024-03-12 Modify logic for extract year to be 2 years prior to current year.
#                              Add call to create manifest file.
#                              Add ENVNAME to SUBJECT of all emails.
#                              Add ${TMSTMP} to temp_RAND_PARTA_Files_${TMSTMP}.txt. When Part A or B jobs
#                              are run concurrently, the later job over-writes the temp file. The presence 
#                              of the timestamp will all for jobs to be run concurrently.
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/RAND_PTD_Extract_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "RAND_PDE_Extract.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

source ${RUNDIR}SET_XTR_ENV.sh

source ${RUNDIR}FilenameCounts.bash

S3BUCKET=${RAND_FFSPTAB_BUCKET} 

echo "RAND FFS_PTAB bucket=${S3BUCKET}" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh

S3BUCKET=${RAND_PDE_BUCKET} 
echo "RAND PDE bucket=${S3BUCKET}" >> ${LOGNAME}


############################################
# Extract current and prior year
############################################
CUR_YR=`date +%Y `
CUR_YR=`expr ${CUR_YR}` 
EXT_YR=`expr ${CUR_YR} - 2` 
CLM_TYPE=PDE

echo "EXT_YR=${EXT_YR}" >> ${LOGNAME}
echo "CUR_YR=${CUR_YR}" >> ${LOGNAME}


#############################################################
# Make variables available to Python code module.
#############################################################
export TMSTMP	
export CUR_YR
export EXT_YR
export CLM_TYPE=PDE
export DATADIR



#############################################################
# Execute Python code to extract data.
#############################################################
		echo "" >> ${LOGNAME}
		echo "Start execution of RAND_PDE_Extract.py program"  >> ${LOGNAME}
		${PYTHON_COMMAND} ${RUNDIR}RAND_PDE_Extract.py >> ${LOGNAME} 2>&1


		#############################################################
		# Check the status of python script  
		#############################################################
		RET_STATUS=$?

		if [[ $RET_STATUS != 0 ]]; then
				echo "" >> ${LOGNAME}
				echo "Python script RAND_PDE_Extract.py failed" >> ${LOGNAME}
				
				# Send Failure email	
				SUBJECT="RAND_PDE_Extract.sh - Failed (${ENVNAME})"
				MSG="RAND PDE extract has failed."
				${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${RAND_PDE_EMAIL_SENDER}" "${RAND_PDE_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

				exit 12
		fi

		echo "" >> ${LOGNAME}
		echo "Python script RAND_PDE_Extract.py completed successfully. " >> ${LOGNAME}


###########################################################################################
# Loop through the extract file list and call combineFiles.sh for each 
# Note: this step will likely take the bulk of processing time for the script
###########################################################################################
echo "##########################################" >> ${LOGNAME}
echo "Reading extract file list and calling CombineS3Files.sh" >> ${LOGNAME}

RAND_FILE_LIST=temp_RAND_PDE_Files_${TMSTMP}.txt	
FILE_LIST=`ls ${DATADIR}${RAND_FILE_LIST}` 1>> ${LOGNAME}  2>&1
FILE_PREFIX=`cat ${FILE_LIST} | tr -d '\r' `

for EXT in $(echo $FILE_PREFIX ) 
do
	${RUNDIR}CombineS3Files.sh ${RAND_PDE_BUCKET} ${EXT} >> ${LOGNAME} 2>&1

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "RAND_PDE_Extract.sh failed during the combine step" >> ${LOGNAME}

		#Send Failure email	
		SUBJECT="RAND PDE Extract - Failed (${ENVNAME})"
		MSG="RAND_PDE_Extract.sh failed while combining S3 files."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${RAND_PDE_EMAIL_SENDER}" "${RAND_PDE_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
		exit 12
	fi
	
done


###########################################################################################
# Delete any temp RAND PDE file list files from DATADIR
###########################################################################################
rm ${DATADIR}temp_RAND_PDE_Files_${TMSTMP}.txt >> ${LOGNAME} 2>&1


###########################################################################################
# Call getExtractFilenamesAndCounts() from FilenameCounts.bash to get all files created
# and the counts for each file to send in the success email
###########################################################################################
echo "" >> ${LOGNAME}
echo "Get S3 Extract file list and record counts." >> ${LOGNAME}

getExtractFilenamesAndCounts ${LOGNAME} >> ${LOGNAME} 2>&1
FILE_LIST="${filenamesAndCounts}"


#############################################################
# Send Success email.
#############################################################
echo "" >> ${LOGNAME}
echo "Send success email with S3 Extract filename." >> ${LOGNAME}

SUBJECT="RAND PDE extract (${ENVNAME})" 
MSG="The RAND PDE extract from Snowflake has completed.\n\nThe following file(s) were created:\n\n${FILE_LIST}"

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${RAND_PDE_EMAIL_SENDER}" "${RAND_PDE_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Error in calling sendEmail.py" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Sending Success email in RAND_PDE_Extract.sh - Failed (${ENVNAME})"
	MSG="Sending Success email in RAND_PDE_Extract.sh  has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${RAND_PDE_EMAIL_SENDER}" "${RAND_PDE_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	


#############################################################
# Create Manifest file
# Override default S3 Manifest folder with S3 Manifest HOLD folder.
# This is so we can manually split files between multiple manifest folders. 
#############################################################
echo "" >> ${LOGNAME}
echo "Create Manifest file for RAND FFS Extract.  " >> ${LOGNAME}

${RUNDIR}CreateManifestFile.sh ${S3BUCKET} ${TMSTMP} ${RAND_FFS_BOX_RECIPIENTS} ${MANIFEST_HOLD_BUCKET}


#############################################################
# Check the status of script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Shell script CreateManifestFile.sh failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Create Manifest file in RAND_PDE_Extract.sh - Failed (${ENVNAME})"
		MSG="Create Manifest file in RAND_PDE_Extract.sh has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${RAND_FFS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
fi


#############################################################
# End script
#############################################################
echo "" >> ${LOGNAME}
echo "RAND_PDE_Extract.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

exit $RET_STATUS
