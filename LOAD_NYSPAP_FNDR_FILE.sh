#!/usr/bin/bash
############################################################################################################
# Script Name: LOAD_NYSPAP_FNDR_FILE.sh
# Description: This script uploads the NYSPAP finder file to BIA_{ENV}.CMS_TARGET_XTR_{ENV}.NYSPAP_FF table.
#
# NOTE: Expected Finder File has format is .txt file. Not .gz file. 
#       Ex. NYSPAP_FNDR_{DATE}.txt
#
# Author     : Paul Baranoski	
# Created    : 09/28/2022
#
# Paul Baranoski 2023-04-11 Modify rm statement to not include '_' after "FNDR".
# Paul Baranoski 2023-08-22 Change location of Finder Files to S3 Finder_Files folder
# Paul Baranoski 2025-01-10 Add ENVNAME to SUBJECT line of all emails.
# Paul Baranoski 2025-01-10 Change this line to see if web-hook is created.
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP:=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/LOAD_NYSPAP_FNDR_FILE_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "LOAD_NYSPAP_FNDR_FILE.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh


S3BUCKET=${NYSPAP_BUCKET} 
PREFIX=NYSPAP_FNDR

echo "NYSPAP bucket=${S3BUCKET}" >> ${LOGNAME}


#################################################################################
# Remove any residual NYSPAP Finder files in data directory.
#################################################################################
echo " " >> ${LOGNAME}
echo "Remove any residual Finder Files in data directory." >> ${LOGNAME}
rm ${DATADIR}NYSPAP_FNDR*  >> ${LOGNAME}  2>&1


#################################################################################
# Find NYSPAP Finder Files in S3
#################################################################################
echo "" >> ${LOGNAME}
echo "Find S3 NYSPAP Finder Files in S3." >> ${LOGNAME}


# Get all filenames in S3 bucket that match filename prefix
aws s3 ls s3://${FINDER_FILE_BUCKET}${PREFIX}  > ${DATADIR}tempNYSPAP.txt  

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Listing S3 files from ${S3BUCKET}${PREFIX} failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="LOAD_NYSPAP_FNDR_FILE.sh script - Failed (${ENVNAME})"
	MSG="Listing Finder Files in S3 from ${S3BUCKET}${PREFIX} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${NYSPAP_EMAIL_SENDER}" "${NYSPAP_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


# if zero files found --> end script
NOF_FILES=`wc -l ${DATADIR}tempNYSPAP.txt | awk '{print $1}' `	2>> ${LOGNAME}

echo "" >> ${LOGNAME}
echo "${NOF_FILES} NYSPAP Finder files found in S3." >> ${LOGNAME}

if [ ${NOF_FILES} -eq 0 ]; then
	echo "" >> ${LOGNAME}
	echo "No Finder files found in ${S3BUCKET}${PREFIX}." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="LOAD_NYSPAP_FNDR_FILE.sh script - Failed (${ENVNAME})"
	MSG="No Finder Files found in ${S3BUCKET}${PREFIX}."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${NYSPAP_EMAIL_SENDER}" "${NYSPAP_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 0
	
# if more than one finder file found --> error --> which file to process?	
elif [ ${NOF_FILES} -gt 1 ]; then
	echo "" >> ${LOGNAME}
	echo "More than one Finder files found in ${S3BUCKET}${PREFIX}." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="LOAD_NYSPAP_FNDR_FILE.sh script - Failed (${ENVNAME})"
	MSG="More than one Finder Files found in ${S3BUCKET}${PREFIX}."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${NYSPAP_EMAIL_SENDER}" "${NYSPAP_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
 	
fi


# Extract just the filename from the S3 filename information
filename=`awk '{print $4}' ${DATADIR}tempNYSPAP.txt` 

echo "" >> ${LOGNAME}
echo "NYSPAP Finder file found: ${filename}" >> ${LOGNAME}


#################################################################################
# Iterate thru finder files in S3 and copy from S3 to Linux. 
# S3 cp --include option does not properly filter results when copying contents 
#         from a folder
#################################################################################
echo "" >> ${LOGNAME}

# Copy S3 file to linux
aws s3 cp s3://${FINDER_FILE_BUCKET}${filename} ${DATADIR}${filename}  1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Copying S3 NYSPAP Finder file to Linux failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="LOAD_NYSPAP_FNDR_FILE.sh  - Failed (${ENVNAME})"
	MSG="Copying S3 file from ${FINDER_FILE_BUCKET} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${NYSPAP_EMAIL_SENDER}" "${NYSPAP_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	

#############################################################
# Expected Finder File has format is .txt file. Not .gz file.
# Ex. NYSPAP_FNDR_{DATE}.txt
#############################################################
# uncompress file
#gzip -d ${DATADIR}${filename}  2>>  ${LOGNAME}

# load export variable with finder filename
LOAD_NYSPAP_FINDER_FILE=${filename}


#############################################################
# Execute Python code to load Finder File to NYSPAP FF table.
#############################################################
echo "" >> ${LOGNAME}
echo "Start execution of LOAD_NYSPAP_FNDR_FILE.py program"  >> ${LOGNAME}

# Export environment variables for Python code
export DATADIR
export LOAD_NYSPAP_FINDER_FILE

${PYTHON_COMMAND} ${RUNDIR}LOAD_NYSPAP_FNDR_FILE.py >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script - Load Finder File
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script LOAD_NYSPAP_FNDR_FILE.py failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="LOAD_NYSPAP_FNDR_FILE.sh  - Failed (${ENVNAME})"
		MSG="NYSPAP loading finder file has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${NYSPAP_EMAIL_SENDER}" "${NYSPAP_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script LOAD_NYSPAP_FNDR_FILE.py completed successfully. " >> ${LOGNAME}


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "LOAD_NYSPAP_FNDR_FILE.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS