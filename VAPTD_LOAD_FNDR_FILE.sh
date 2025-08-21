#!/usr/bin/bash
############################################################################################################
# Name:  VAPTD_LOAD_FNDR_FILE.sh
#
# Desc: VA Part D Quaterly Extract Load Finder File for Q1. Executes VAPTD_LOAD_FNDR_FILE.py
#
# Author     : Joshua Turner	
# Created    : 05/22/2024
#
# Modified:
# Joshua Turner    2024-06-30  New script, based on the finder file portion of VAPTD_LOAD_FNDR_FILE.sh. This driver
#                              will only execute before VA PTD extract for Q1 of the current year
############################################################################################################
set +x

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source /app/IDRC/XTR/CMS/scripts/run/SET_XTR_ENV.sh
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
LOGNAME=/app/IDRC/XTR/CMS/logs/VAPTD_LOAD_FNDR_FILE_${TMSTMP}.log
touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "VAPTD_LOAD_FNDR_FILE.sh started at ${TMSTMP} " >> ${LOGNAME}
echo "" >> ${LOGNAME}

###########################################################################################
# The finder file load is only required for Q1, however there are no month checks in this 
# script in case a finder file needs to be reloaded. This will be a stand-alone job that can
# be executed at any time.
###########################################################################################
echo "################################### " >> ${LOGNAME}
echo "Start Finder File load to Snowflake from S3." >> ${LOGNAME}
echo "Loading Finder File from ${FINDER_FILE_BUCKET}." >> ${LOGNAME}

PREFIX=MOV_VAPTD

################################################################
# Find VAPTD Finder File in AWS and load to Snowflake
################################################################
aws s3 ls s3://${FINDER_FILE_BUCKET}${PREFIX} > ${DATADIR}tempVAPTD_FF.txt

RET_STATUS=$?
if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Listing S3 files from Finder File bucket FAILED." >> ${LOGNAME}
	# Send failure email
	SUBJECT="VA Part D Finder File FAILED in LIST step ($ENVNAME)"
	MSG="VAPTD_LOAD_FNDR_FILE.sh FAILED while locating the finder file in S3."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${VAPTD_EMAIL_SENDER}" "${VAPTD_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
	exit 12
fi

################################################################
# if number of files = 0 or greater than 1, send error
################################################################
NO_OF_FILES=`wc -l ${DATADIR}tempVAPTD_FF.txt | awk '{print $1}' ` 2>> ${LOGNAME}

if [ ${NO_OF_FILES} -eq 0 ]; then
	echo "" >> ${LOGNAME}
	echo "No Finder File was found for VAPTD." >> ${LOGNAME}
	
	# Send failure email
	SUBJECT="VAPTD_LOAD_FNDR_FILE.sh - FAILED ($ENVNAME)"
	MSG="No Finder Files were found in ${FINDER_FILE_BUCKET} for VAPTD"
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${VAPTD_EMAIL_SENDER}" "${VAPTD_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
	
	exit 12
elif [ ${NO_OF_FILES} -gt 1 ]; then
	echo "" >> ${LOGNAME}
	echo "More than 1 Finder File was found for VAPTD." >> ${LOGNAME}
	
	# Send failure email
	SUBJECT="VAPTD_LOAD_FNDR_FILE.sh - FAILED"
	MSG="More than one Finder File was found in ${FINDER_FILE_BUCKET} for VAPTD. Please correct the files in S3 before rerunning"
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${VAPTD_EMAIL_SENDER}" "${VAPTD_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
	
	exit 12
fi

FF_FILENAME=`awk '{print $4}' ${DATADIR}tempVAPTD_FF.txt`

export CURR_YEAR
export FF_FILENAME
echo ""
echo "Starting load to Snowflake." >> ${LOGNAME}
${PYTHON_COMMAND} ${RUNDIR}VAPTD_LOAD_FNDR_FILE.py >> ${LOGNAME} 2>&1

RET_STATUS=$?
if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Loading Finder File from S3 failed." >> ${LOGNAME}
	
	# Send failure email
	SUBJECT="VA Part D Finder File Load FAILED ($ENVNAME)"
	MSG="VAPTD_LOAD_FNDR_FILE.sh FAILED while loading the finder file from S3."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${VAPTD_EMAIL_SENDER}" "${VAPTD_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
	
	exit 12
fi

# Move finder file to the archive folder
echo "Moving Finder file ${FF_FILENAME} to S3 archive folder." >> ${LOGNAME}
aws s3 mv s3://${FINDER_FILE_BUCKET}${FF_FILENAME} s3://${FINDER_FILE_BUCKET}archive/${FF_FILENAME}  1>> ${LOGNAME} 2>&1

echo "" >> ${LOGNAME}
echo "Loading Finder File from S3 completed successfully." >> ${LOGNAME}
echo "VAPTD_LOAD_FNDR_FILE.sh ended at: ${TMSTMP}" >> ${LOGNAME}
exit $RET_STATUS
