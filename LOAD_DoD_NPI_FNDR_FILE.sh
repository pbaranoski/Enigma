#!/usr/bin/sh
############################################################################################################
# Script Name: LOAD_DOD_NPI_FNDR_FILE.sh
#
# Description: This script uploads the DOD NPI Finder Files to BIA_{ENV}.CMS_TARGET_XTR_{ENV}.DOD_NPI_FF table.
#
#
# Paul Baranoski 2025-09-11 Create script.
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/LOAD_DOD_NPI_FNDR_FILE_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "LOAD_DOD_NPI_FNDR_FILE.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh

S3BUCKET=${DOD_NPI_BUCKET} 
#PREFIX=DOD_NPI_FNDR
PREFIX=P#EFT.ON.MDD.C999.DOD

echo "DoD NPIbucket=${S3BUCKET}" >> ${LOGNAME}
echo "Finder files DoD NPI bucket=${FINDER_FILE_BUCKET}" >> ${LOGNAME}


#################################################################################
# Find MNNUP Finder Files in S3
#################################################################################
echo "" >> ${LOGNAME}
echo "Find DoD NPIFinder Files in S3." >> ${LOGNAME}

NOF_FILES=`aws s3 ls s3://${FINDER_FILE_BUCKET}${PREFIX} --summarize | grep 'Total Objects' | cut -d: -f2 | sed 's/^ *//g' `  2>> ${LOGNAME}

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Get count of S3 files from ${FINDER_FILE_BUCKET}${PREFIX} failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="LOAD_DOD_NPI_FNDR_FILE.sh script - Failed (${ENVNAME})"
	MSG="Listing Finder Files in S3 from ${FINDER_FILE_BUCKET}${PREFIX} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


#################################################################################
# if zero files found --> end script
#################################################################################
echo "" >> ${LOGNAME}
echo "${NOF_FILES} DoD NPIFinder files found in S3." >> ${LOGNAME}

if [ ${NOF_FILES} -eq 0 ]; then
	echo "" >> ${LOGNAME}
	echo "No DoD NPIFinder files found in ${FINDER_FILE_BUCKET}${PREFIX}." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="LOAD_DOD_NPI_FNDR_FILE.sh script - Failed (${ENVNAME})"
	MSG="No DoD NPIFinder Files found in ${FINDER_FILE_BUCKET}${PREFIX}."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12

# if more than one finder file found --> error --> which file to process?	
elif [ ${NOF_FILES} -gt 1 ]; then
	echo "" >> ${LOGNAME}
	echo "More than one DoD NPIFinder file found in ${FINDER_FILE_BUCKET}${PREFIX}." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="LOAD_DOD_NPI_FNDR_FILE.sh script - Failed (${ENVNAME})"
	MSG="More than one DoD NPIFinder Files found in ${FINDER_FILE_BUCKET}${PREFIX}."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
 	
fi


#################################################################################
# Extract just the filename from the S3 filename information
#################################################################################
echo "" >> ${LOGNAME}
echo "Get DoD NPIFinder File filename" >> ${LOGNAME}

LOAD_FINDER_FILE=`aws s3 ls s3://${FINDER_FILE_BUCKET}${PREFIX} | awk '{print $4}' ` 2>> ${LOGNAME}

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Get DoD NPIFinder File filename from ${FINDER_FILE_BUCKET}${PREFIX} failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="LOAD_DOD_NPI_FNDR_FILE.sh script - Failed (${ENVNAME})"
	MSG="Get DoD NPIFinder File filename in S3 from ${FINDER_FILE_BUCKET}${PREFIX} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi

echo "DoD NPIFinder file found: ${LOAD_FINDER_FILE}" >> ${LOGNAME}


#############################################################
# Execute Python code to load Finder File to DoD NPIFF table.
#############################################################
echo "" >> ${LOGNAME}
echo "Start execution of LOAD_DOD_NPI_FNDR_FILE.py program"  >> ${LOGNAME}

# Export environment variables for Python code
export LOAD_FINDER_FILE

${PYTHON_COMMAND} ${RUNDIR}LOAD_DoD_NPI_FNDR_FILE.py >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script - Load Finder File
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script LOAD_DOD_NPI_FNDR_FILE.py failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="LOAD_DOD_NPI_FNDR_FILE.sh  - Failed"
		MSG="DoD NPIloading finder file has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script LOAD_DOD_NPI_FNDR_FILE.py completed successfully. " >> ${LOGNAME}


#################################################################################
# MOVE Finder File to archive folder after loaded into table.
#################################################################################
echo "" >> ${LOGNAME}
echo "Move Finder file to archive folder after successful load into table"  >> ${LOGNAME}

# move S3 finder file to archive folder
aws s3 mv s3://${FINDER_FILE_BUCKET}${LOAD_FINDER_FILE} s3://${FINDER_FILE_BUCKET}archive/${LOAD_FINDER_FILE}  1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Moving S3 Finder file to archive folder failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="LOAD_Dod_NPI_FNDR_FILE.sh  - Failed (${ENVNAME})"
	MSG="Moving S3 Finder file to archive folder failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "LOAD_DOD_NPI_FNDR_FILE.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS