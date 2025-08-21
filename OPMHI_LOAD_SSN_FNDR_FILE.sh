#!/usr/bin/sh
############################################################################################################
# Script Name: OPMHI_LOAD_SSN_FNDR_FILE.sh
# Description: This script uploads the OPM-HI ICD-10 diagnosis code finder file to 
#              BIA_{ENV}.CMS_TARGET_XTR_{ENV}.OPMHI_SSN
#
# Created: Joshua Turner
# Modified: 06/06/2023
#
# Paul Baranoski 2023-10-14 Uncomment logic to move finder file to 23://Finder_Files/archive folder.
# Paul Baranoski 2024-08-02 Add ENV to Subject line for emails.
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
LOGNAME=/app/IDRC/XTR/CMS/logs/OPMHI_LOAD_SSN_FNDR_FILE_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "OPMHI_LOAD_SSN_FNDR_FILE.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh
S3BUCKET=${FINDER_FILE_BUCKET}

#################################################################################
# Remove any residual OPMHI SSN Finder files in data directory.
#################################################################################
echo " " >> ${LOGNAME}
echo "Remove any residual Finder Files in data directory." >> ${LOGNAME}
rm ${DATADIR}OPMHI_SSN*  >> ${LOGNAME}  2>&1

#################################################################################
# Get ICD10DGS EXCL finder files from the finder file bucket
#################################################################################
echo "Locating finder files in bucket=${S3BUCKET}" >> ${LOGNAME}
PREFIX=OPMHI_SSN

# Get all filenames in S3 bucket that match filename prefix
aws s3 ls s3://${S3BUCKET}${PREFIX}  > ${DATADIR}temp_OPMHI_SSN.txt  

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Listing S3 files from ${S3BUCKET}${PREFIX} failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="OPMHI_LOAD_SSN_FNDR_FILE.sh script - Failed (${ENVNAME})"
	MSG="Listing Finder Files in S3 from ${S3BUCKET}${PREFIX} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${OPMHI_EMAIL_SENDER}" "${OPMHI_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi

# if zero files found --> end script
NOF_FILES=`wc -l ${DATADIR}temp_OPMHI_SSN.txt  | awk '{print $1}' `	2>> ${LOGNAME}

echo "" >> ${LOGNAME}
echo "${NOF_FILES} OPMHI SSN finder files found in S3." >> ${LOGNAME}

if [ ${NOF_FILES} -eq 0 ]; then
	echo "" >> ${LOGNAME}
	echo "No Finder files found in ${S3BUCKET}${PREFIX}." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="OPMHI_LOAD_SSN_FNDR_FILE.sh script - Failed (${ENVNAME})"
	MSG="No Finder Files found in ${S3BUCKET}${PREFIX}."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${OPMHI_EMAIL_SENDER}" "${OPMHI_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 0
	
# if more than one finder file found --> error --> which file to process?	
elif [ ${NOF_FILES} -gt 1 ]; then
	echo "" >> ${LOGNAME}
	echo "More than one Finder files found in ${S3BUCKET}${PREFIX}." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="OPMHI_LOAD_SSN_FNDR_FILE.sh script - Failed (${ENVNAME})"
	MSG="More than one Finder Files found in ${S3BUCKET}${PREFIX}."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${OPMHI_EMAIL_SENDER}" "${OPMHI_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi

# Extract just the filename from the S3 filename information
filename=`awk '{print $4}' ${DATADIR}temp_OPMHI_SSN.txt` 

echo "" >> ${LOGNAME}
echo "OPMHI SSN Finder file found: ${filename}" >> ${LOGNAME}

#################################################################################
# Copy S3 Finder File to Linux. 
#################################################################################
echo "" >> ${LOGNAME}
echo "Copy OPMHI SSN finder file from s3 to linux " >> ${LOGNAME}

# Copy S3 file to linux
aws s3 cp s3://${S3BUCKET}${filename} ${DATADIR}${filename}  1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Copying OPMHI SSN finder file to Linux failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="OPMHI_LOAD_SSN_FNDR_FILE.sh - Failed (${ENVNAME})"
	MSG="Copying S3 file from ${S3BUCKET} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${OPMHI_EMAIL_SENDER}" "${OPMHI_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	

# uncompress file if needed
#echo "" >> ${LOGNAME}
#echo "Unzip Finder file if needed " >> ${LOGNAME}
#
#gzip -d ${DATADIR}${filename}  2>>  ${LOGNAME}
#

# load export variable with finder filename
OPMHI_SSN_FF=${filename}

#################################################################################
# Execute Python code to load Finder File
#################################################################################
echo "" >> ${LOGNAME}
echo "Start execution of OPMHI_LOAD_SSN_FNDR_FILE.py program"  >> ${LOGNAME}

# Export environment variables for Python code
export DATADIR
export OPMHI_SSN_FF

${PYTHON_COMMAND} ${RUNDIR}OPMHI_LOAD_SSN_FNDR_FILE.py >> ${LOGNAME} 2>&1

#################################################################################
# Check the status of python script - Load Finder File
#################################################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script OPMHI_LOAD_SSN_FNDR_FILE.py failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="OPMHI_LOAD_SSN_FNDR_FILE.sh - Failed (${ENVNAME})"
		MSG="OPMHI loading SSN finder file has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${OPMHI_EMAIL_SENDER}" "${OPMHI_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script OPMHI_LOAD_SSN_FNDR_FILE.py completed successfully. " >> ${LOGNAME}

#################################################################################
# Remove SSN Finder File from the Linux server
#################################################################################
echo "" >> ${LOGNAME}
echo "Delete SSN finder file from linux data directory"  >> ${LOGNAME}

rm ${DATADIR}OPMHI_SSN*  >> ${LOGNAME}  2>&1

#################################################################################
# Move Finder File to archive folder when loaded into table.
#################################################################################
echo "" >> ${LOGNAME}
echo "Move OPMHI SSN finder file to archive folder after successful load into table"  >> ${LOGNAME}

#move S3 finder file to archive folder
aws s3 mv s3://${S3BUCKET}${filename} s3://${S3BUCKET}archive/${filename}  1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Copying OPMHI SSN finder file to Linux failed." >> ${LOGNAME}
	
	Send Failure email	
	SUBJECT="OPMHI_LOAD_SSN_FNDR_FILE.sh - Failed (${ENVNAME})"
	MSG="Copying S3 file from ${S3BUCKET} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${OPMHI_EMAIL_SENDER}" "${OPMHI_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	

#################################################################################
# script clean-up and send success email
#################################################################################
echo "" >> ${LOGNAME}
echo "OPMHI_LOAD_SSN_FNDR_FILE.sh completed successfully." >> ${LOGNAME}

SUBJECT="OPMHI_LOAD_SSN_FNDR_FILE.sh - Completed (${ENVNAME})"
MSG="Loading OPMHI SSN finder file to Snowflake has completed successfully."
${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${OPMHI_EMAIL_SENDER}" "${OPMHI_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS