#!/usr/bin/bash
############################################################################################################
# Script Name: LOAD_PSA_FINDER_FILES.sh
#
# Description: This script will load all the PSA Finder file tables in Snowflake from S3 Finder Files.
#
#         1) PSA_HCPCS_APC_CAT_FF  --> PSA_FINDER_FILE_HCPCS_APC_CATEGORIES_YYYYMMDD.csv
#         2) PSA_APC_CAT_FF        --> PSA_FINDER_FILE_APC_Categories_YYYYMMDD.csv
#         3) PSA_DRG_MDC_FF        --> PSA_FINDER_FILE_DRG_MDC_YYYYMMDD.csv 
#         4) PSA_PRVDR_SPCLTY_FF   --> PSA_FINDER_FILE_PRVDR_SPCLTY_CDS_YYYYMMDD.csv
#
#
# Author     : Paul Baranoski	
# Created    : 12/08/2022
#
# Paul Baranoski 2023-12-08 Create script
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/LOAD_PSA_FINDER_FILES_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "LOAD_PSA_FINDER_FILES.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh

S3BUCKET=${PSA_BUCKET} 

echo "PSA bucket=${S3BUCKET}" >> ${LOGNAME}
echo "Finder files bucket=${FINDER_FILE_BUCKET}" >> ${LOGNAME}

PSA_PRVDR_SPCLTY_PREFIX=PSA_FINDER_FILE_PRVDR_SPCLTY_CDS
PSA_HCPCS_APC_CAT_PREFIX=PSA_FINDER_FILE_HCPCS_APC_CATEGORIES
PSA_APC_CAT_PREFIX=PSA_FINDER_FILE_APC_Categories
PSA_DRG_MDC_PREFIX=PSA_FINDER_FILE_DRG_MDC 


#################################################################################
# Remove any residual temp files or PSA Finder files in data directory.
#################################################################################
echo " " >> ${LOGNAME}
echo "Remove any residual Finder Files in data directory." >> ${LOGNAME}
rm ${DATADIR}PSA_FINDER_FILE_*  >> ${LOGNAME}  2>&1
rm ${DATADIR}tempPSA.txt  >> ${LOGNAME}  2>&1


#################################################################################
# Function to verify a finder file exists AND get the actual filename 
#################################################################################
verifyFFExistsAndGetName() {

	#################################################################################
	# Load variable with parameter --> Finder file prefix 
	#################################################################################
	FINDER_FILE_PREFIX=$1
	
	#################################################################################
	# Searching for PSA Finder Files in S3
	#################################################################################
	echo "" >> ${LOGNAME}
	echo "Searching for PSA Finder File ${FINDER_FILE_PREFIX} in S3." >> ${LOGNAME}


	# Get all filenames in S3 bucket that match filename prefix
	aws s3 ls s3://${FINDER_FILE_BUCKET}${FINDER_FILE_PREFIX}  > ${DATADIR}tempPSA.txt  

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Listing S3 files from ${FINDER_FILE_BUCKET}${FINDER_FILE_PREFIX} failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="LOAD_PSA_FINDER_FILES.sh script - Failed (${ENVNAME})"
		MSG="Listing Finder Files in S3 from ${FINDER_FILE_BUCKET}${FINDER_FILE_PREFIX} failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSA_EMAIL_SENDER}" "${PSA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi


	#################################################################################
	# if zero files found --> end script
	#################################################################################
	NOF_FILES=`wc -l ${DATADIR}tempPSA.txt | awk '{print $1}' `	2>> ${LOGNAME}

	echo "${NOF_FILES} ${FINDER_FILE_PREFIX} Finder file(s) found in S3." >> ${LOGNAME}

	if [ ${NOF_FILES} -eq 0 ]; then
		echo "" >> ${LOGNAME}
		echo "No PSA Finder files found in ${FINDER_FILE_BUCKET}${FINDER_FILE_PREFIX}." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="LOAD_PSA_FINDER_FILES.sh script - Failed (${ENVNAME})"
		MSG="No PSA Finder Files found in ${FINDER_FILE_BUCKET}${FINDER_FILE_PREFIX}."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSA_EMAIL_SENDER}" "${PSA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 0

	# if more than one finder file found --> error --> which file to process?	
	elif [ ${NOF_FILES} -gt 1 ]; then
		echo "" >> ${LOGNAME}
		echo "More than one PSA Finder file found in ${FINDER_FILE_BUCKET}${FINDER_FILE_PREFIX}." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="LOAD_PSA_FINDER_FILES.sh script - Failed (${ENVNAME})"
		MSG="More than one PSA Finder Files found in ${FINDER_FILE_BUCKET}${FINDER_FILE_PREFIX}."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSA_EMAIL_SENDER}" "${PSA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
		
	fi


	#################################################################################
	# Extract just the filename from the S3 filename information
	#################################################################################
	FINDER_FILENAME=`awk '{print $4}' ${DATADIR}tempPSA.txt` 

	echo "S3 PSA Finder file found: ${FINDER_FILENAME}" >> ${LOGNAME}
	
}


#############################################################
# Verify required finder files exist AND get the full finder 
#    filename to pass to python code.
#############################################################
verifyFFExistsAndGetName ${PSA_PRVDR_SPCLTY_PREFIX}
PSA_FF_PRVDR_SPCLTY_S3FILENAME=${FINDER_FILENAME}

verifyFFExistsAndGetName ${PSA_HCPCS_APC_CAT_PREFIX}
PSA_FF_HCPCS_APC_CAT_S3FILENAME=${FINDER_FILENAME}

verifyFFExistsAndGetName ${PSA_APC_CAT_PREFIX}
PSA_FF_APC_CAT_S3FILENAME=${FINDER_FILENAME}

verifyFFExistsAndGetName ${PSA_DRG_MDC_PREFIX}
PSA_FF_DRG_MDC_S3FILENAME=${FINDER_FILENAME}


#############################################################
# Execute Python code to load Finder File to PSA FF table.
#############################################################
echo "" >> ${LOGNAME}
echo "Start execution of LOAD_PSA_FINDER_FILES.py program"  >> ${LOGNAME}

export TMSTMP
# Export environment variables for Python code
export PSA_FF_PRVDR_SPCLTY_S3FILENAME
export PSA_FF_HCPCS_APC_CAT_S3FILENAME
export PSA_FF_APC_CAT_S3FILENAME
export PSA_FF_DRG_MDC_S3FILENAME

${PYTHON_COMMAND} ${RUNDIR}LOAD_PSA_FINDER_FILES.py >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script - Load Finder File
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script LOAD_PSA_FINDER_FILES.py failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="LOAD_PSA_FINDER_FILES.sh  - Failed (${ENVNAME})"
		MSG="PSA loading finder file has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSA_EMAIL_SENDER}" "${PSA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script LOAD_PSA_FINDER_FILES.py completed successfully. " >> ${LOGNAME}


#################################################################################
# Remove any residual temp files or PSA Finder files in data directory.
#################################################################################
echo " " >> ${LOGNAME}
echo "Remove any residual Finder Files in data directory." >> ${LOGNAME}
rm ${DATADIR}PSA_FINDER_FILE_*  >> ${LOGNAME}  2>&1
rm ${DATADIR}tempPSA.txt  >> ${LOGNAME}  2>&1


#############################################################
# script End
#############################################################
echo "" >> ${LOGNAME}
echo "LOAD_PSA_FINDER_FILES.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS