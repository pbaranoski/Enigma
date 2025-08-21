#!/usr/bin/sh
############################################################################################################
# Script Name: LOAD_MNUP_Monthly_FNDR_FILE.sh
# Description: This script uploads the MNUP Monthly finder file to BIA_{ENV}.CMS_TARGET_XTR_{ENV}.MNUP_Monthly_FF table.
#
# Author     : Vureb Khanna
# Created    : 07/24/2024
#
# Viren Khanna 2024-07-22 Update to download Finder File from S3:/Finder_Files bucket
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/LOAD_MNUP_FNDR_FILE_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "LOAD_MNUP_Monthly_FNDR_FILE.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh


S3BUCKET=${MNUP_MONTHLY_BUCKET} 
PREFIX=MNUP_MONTHLY_FNDR

echo "MNUP_MONTHLY_BUCKET=${S3BUCKET}" >> ${LOGNAME}
echo "Finder files SSA bucket=${FINDER_FILE_SSA_BUCKET}" >> ${LOGNAME}


#################################################################################
# Find MNNUP Finder Files in S3
#################################################################################
echo "" >> ${LOGNAME}
echo "Find MNUP Monthly Finder Files in S3." >> ${LOGNAME}

NOF_FILES=`aws s3 ls s3://${FINDER_FILE_SSA_BUCKET}${PREFIX} --summarize | grep 'Total Objects' | cut -d: -f2 | sed 's/^ *//g' `  2>> ${LOGNAME}

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Get count of S3 files from ${FINDER_FILE_SSA_BUCKET}${PREFIX} failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="LOAD_MNUP_Monthly_FNDR_FILE.sh script - Failed (${ENVNAME})"
	MSG="Listing Finder Files in S3 from ${FINDER_FILE_SSA_BUCKET}${PREFIX} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${MNUP_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


#################################################################################
# if zero files found --> end script
#################################################################################
echo "" >> ${LOGNAME}
echo "${NOF_FILES} MNUP Monthly Finder files found in S3." >> ${LOGNAME}

if [ ${NOF_FILES} -eq 0 ]; then
	echo "" >> ${LOGNAME}
	echo "No MNUP Monthly Finder files found in ${FINDER_FILE_BUCKET}${PREFIX}." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="LOAD_MNUP_Monthly_FNDR_FILE.sh script - Failed (${ENVNAME})"
	MSG="No MNUP Monthly Finder Files found in ${FINDER_FILE_BUCKET}${PREFIX}."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${MNUP_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12

# if more than one finder file found --> error --> which file to process?	
elif [ ${NOF_FILES} -gt 1 ]; then
	echo "" >> ${LOGNAME}
	echo "More than one MNUP Monthly Finder file found in ${FINDER_FILE_BUCKET}${PREFIX}." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="LOAD_MNUP_Monthly_FNDR_FILE.sh script - Failed (${ENVNAME})"
	MSG="More than one MNUP Monthly Finder Files found in ${FINDER_FILE_BUCKET}${PREFIX}."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${MNUP_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
 	
fi


#################################################################################
# Extract just the filename from the S3 filename information
#################################################################################
echo "" >> ${LOGNAME}
echo "Get MNUP Finder File filename" >> ${LOGNAME}

LOAD_MNUP_Monthly_FINDER_FILE=`aws s3 ls s3://${FINDER_FILE_SSA_BUCKET}${PREFIX} | awk '{print $4}' ` 2>> ${LOGNAME}

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Get MNUP Monthly Finder File filename from ${FINDER_FILE_SSA_BUCKET}${PREFIX} failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="LOAD_Monthly_MNUP_FNDR_FILE.sh script - Failed (${ENVNAME})"
	MSG="Get MNUP Monthly Finder File filename in S3 from ${FINDER_FILE_SSA_BUCKET}${PREFIX} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${MNUP_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi

echo "MNUP Monthly Finder file found: ${LOAD_MNUP_Monthly_FINDER_FILE}" >> ${LOGNAME}


#############################################################
# Execute Python code to load Finder File to MNUP FF table.
#############################################################
echo "" >> ${LOGNAME}
echo "Start execution of LOAD_MNUP_Monthly_FNDR_FILE.py program"  >> ${LOGNAME}

# Export environment variables for Python code
export LOAD_MNUP_Monthly_FINDER_FILE

${PYTHON_COMMAND} ${RUNDIR}LOAD_MNUP_Monthly_FNDR_FILE.py >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script - Load Finder File
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script LOAD_MNUP_Monthly_FNDR_FILE.py failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="LOAD_MNUP_Monthly_FNDR_FILE.sh  - Failed"
		MSG="MNUP Monthly loading finder file has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${MNUP_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script LOAD_MNUP_Monthly_FNDR_FILE.py completed successfully. " >> ${LOGNAME}


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "LOAD_MNUP_Monthly_FNDR_FILE.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS