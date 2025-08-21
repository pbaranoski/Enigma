#!/usr/bin/sh
############################################################################################################
# Script Name: BAYSTATE_LOAD_RRB_FNDR_FILE.sh
# Description: This script executes the python program that loads the MEDPAR BAYSTATE SSN finder file to 
#              BIA_{ENV}.CMS_TARGET_XTR_{ENV}.MEDPAR_BAYSTATE_RRB from S3
#
#              The RRB file is a subset of SSNs from the main file only to be used for BENE_ID_TYPE_CD = 'R'
#
# Note: The expected filename for the finder file is MEDPAR_BAYSTATE_RRB_*.txt. It will probably need to be
#       renamed in S3 before running this process. It is possible for there to be junk data in the last record.
#       It may or may not need to be removed before processing.
#
# Author    : Joshua Turner
# Created   : 10/05/2023
#
# Modified:             Date:        Description:
# --------------------  -----------  -------------------------------------------------------------------
# Joshua Turner         2024-01-09   Updated for coding standards. 
# Paul Baranoski        2025-01-28   Change CMS_EMAIL_SENDER to CMS_EMAIL_SENDER. 
#                                    Change ENIGMA_EMAIL_FAILURE_RECIPIENT to ENIGMA_EMAIL_FAILURE_RECIPIENT
############################################################################################################
set +x
#################################################################################
# Establish log file  
#################################################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
LOGNAME=/app/IDRC/XTR/CMS/logs/BAYSTATE_LOAD_RRB_FNDR_FILE_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "BAYSTATE_LOAD_RRB_FNDR_FILE.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#################################################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#################################################################################
source ${RUNDIR}SET_XTR_ENV.sh
S3BUCKET=${FINDER_FILE_BUCKET}

#################################################################################
# Remove any residual MEDPAR BAYSTATE Finder files in data directory.
#################################################################################
echo " " >> ${LOGNAME}
echo "Remove any residual Finder Files in data directory." >> ${LOGNAME}
rm ${DATADIR}MEDPAR_BAYSTATE_RRB*  >> ${LOGNAME}  2>&1

#################################################################################
# Get SSN files from the finder file bucket
#################################################################################
echo "Locating finder files in bucket=${S3BUCKET}" >> ${LOGNAME}
PREFIX=MEDPAR_BAYSTATE_RRB

# Get all filenames in S3 bucket that match filename prefix
aws s3 ls s3://${S3BUCKET}${PREFIX}  > ${DATADIR}temp_MEDPAR_BAYSTATE_RRB.txt  

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Listing S3 files from ${S3BUCKET}${PREFIX} failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="BAYSTATE_LOAD_RRB_FNDR_FILE.sh script - Failed (${ENVNAME})"
	MSG="Listing Finder Files in S3 from ${S3BUCKET}${PREFIX} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi

# if zero files found --> end script
NOF_FILES=`wc -l ${DATADIR}temp_MEDPAR_BAYSTATE_RRB.txt  | awk '{print $1}' `	2>> ${LOGNAME}

echo "" >> ${LOGNAME}
echo "${NOF_FILES} MEDPAR Baystate RRB SSN finder files found in S3." >> ${LOGNAME}

if [ ${NOF_FILES} -eq 0 ]; then
	echo "" >> ${LOGNAME}
	echo "No Finder files found in ${S3BUCKET}${PREFIX}." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="BAYSTATE_LOAD_RRB_FNDR_FILE.sh script - Failed (${ENVNAME})"
	MSG="No Finder Files found in ${S3BUCKET}${PREFIX}."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 0
	
# if more than one finder file found --> error --> which file to process?	
elif [ ${NOF_FILES} -gt 1 ]; then
	echo "" >> ${LOGNAME}
	echo "More than one Finder files found in ${S3BUCKET}${PREFIX}." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="BAYSTATE_LOAD_RRB_FNDR_FILE.sh script - Failed (${ENVNAME})"
	MSG="More than one Finder Files found in ${S3BUCKET}${PREFIX}."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi

# Extract just the filename from the S3 filename information
filename=`awk '{print $4}' ${DATADIR}temp_MEDPAR_BAYSTATE_RRB.txt` 

echo "" >> ${LOGNAME}
echo "MEDPAR Baystate RRB SSN Finder file found: ${filename}" >> ${LOGNAME}

#################################################################################
# Copy S3 Finder File to Linux. 
#################################################################################
echo "" >> ${LOGNAME}
echo "Copy MEDPAR Baystate RRB SSN finder file from s3 to linux " >> ${LOGNAME}

# Copy S3 file to linux
aws s3 cp s3://${S3BUCKET}${filename} ${DATADIR}${filename}  1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Copying MEDPAR Baystate RRB SSN finder file to Linux failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="BAYSTATE_LOAD_RRB_FNDR_FILE.sh  - Failed (${ENVNAME})"
	MSG="Copying S3 file from ${S3BUCKET} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	

# uncompress file if needed
#echo "" >> ${LOGNAME}
#echo "Unzip Finder file if needed " >> ${LOGNAME}
#
#gzip -d ${DATADIR}${filename}  2>>  ${LOGNAME}
#

# load export variable with finder filename
BAYSTATE_RRB_FF=${filename}

#################################################################################
# Execute Python code to load Finder File
#################################################################################
echo "" >> ${LOGNAME}
echo "Start execution of BAYSTATE_LOAD_RRB_FNDR_FILE.py program"  >> ${LOGNAME}

# Export environment variables for Python code
export DATADIR
export BAYSTATE_RRB_FF

${PYTHON_COMMAND} ${RUNDIR}BAYSTATE_LOAD_RRB_FNDR_FILE.py >> ${LOGNAME} 2>&1

#################################################################################
# Check the status of python script - Load Finder File
#################################################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script BAYSTATE_LOAD_RRB_FNDR_FILE.py failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="BAYSTATE_LOAD_RRB_FNDR_FILE.sh  - Failed (${ENVNAME})"
		MSG="MEDPAR Baystate loading RRB SSN finder file has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script BAYSTATE_LOAD_RRB_FNDR_FILE.py completed successfully. " >> ${LOGNAME}

#################################################################################
# Remove SSN Finder File from the Linux server
#################################################################################
echo "" >> ${LOGNAME}
echo "Delete MEDPAR Baystate RRB SSN finder file from linux data directory"  >> ${LOGNAME}

rm ${DATADIR}MEDPAR_BAYSTATE_RRB*  >> ${LOGNAME}  2>&1

#################################################################################
# Move Finder File to archive folder when loaded into table.
#################################################################################
echo "" >> ${LOGNAME}
echo "Move MEDPAR Baystate RRB SSN finder file to archive folder after successful load into table"  >> ${LOGNAME}

#move S3 finder file to archive folder
aws s3 mv s3://${S3BUCKET}${filename} s3://${S3BUCKET}archive/${filename}  1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Copying MEDPAR Baystate RRB SSN finder file to Linux failed." >> ${LOGNAME}
	
	Send Failure email	
	SUBJECT="BAYSTATE_LOAD_RRB_FNDR_FILE.sh  - Failed (${ENVNAME})"
	MSG="Copying S3 file from ${S3BUCKET} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	

#################################################################################
# script clean-up and send success email
#################################################################################
echo "" >> ${LOGNAME}
echo "BAYSTATE_LOAD_RRB_FNDR_FILE.sh completed successfully." >> ${LOGNAME}

SUBJECT="BAYSTATE_LOAD_RRB_FNDR_FILE.sh  - Completed (${ENVNAME})"
MSG="Loading MEDPAR Baystate RRB SSN finder file to Snowflake has completed successfully."
${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${MEDPAR_BAYSTATE_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Error in calling sendEmail.py" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Sending Success email in BAYSTATE_LOAD_RRB_FNDR_FILE.sh - Failed (${ENVNAME})"
		MSG="Sending Success email in BAYSTATE_LOAD_RRB_FNDR_FILE.sh has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
fi

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS