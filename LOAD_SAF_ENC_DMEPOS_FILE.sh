#!/usr/bin/sh
############################################################################################################
# Script Name: LOAD_SAF_ENC_DMEPOS_FILE.sh
# Description: This script uploads the DMEPOS finder file to BIA_{ENV}.CMS_TARGET_XTR_{ENV}.SAFENC_DMEPOS_HCPCS table.
#
# Created: Viren Khanna
# Modified: 05/31/2023
#
# Modified:
##
#  
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
LOGNAME=/app/IDRC/XTR/CMS/logs/LOAD_SAFENC_CAR_FNDR_FILE_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "LOAD_SAF_ENC_DMEPOS_FILE.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh


S3BUCKET=${FINDER_FILE_BUCKET} 
PREFIX=SAF_ENC_DMEPOS

echo "SAF ENC CAR bucket=${S3BUCKET}" >> ${LOGNAME}


#################################################################################
# Remove any residual SAF ENC Finder files in data directory.
#################################################################################
echo " " >> ${LOGNAME}
echo "Remove any residual Finder Files in data directory." >> ${LOGNAME}
rm ${DATADIR}SAF_ENC_DMEPOS_*  >> ${LOGNAME}  2>&1


#################################################################################
# Find SAF ENC CAR Finder Files in S3
#################################################################################
echo "" >> ${LOGNAME}
echo "Find S3 SAF ENC DMEPOS Finder Files in S3." >> ${LOGNAME}


# Get all filenames in S3 bucket that match filename prefix
aws s3 ls s3://${S3BUCKET}${PREFIX}  > ${DATADIR}temp_SAF_ENC_DMEPOS.txt  

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Listing S3 files from ${S3BUCKET}${PREFIX} failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="LOAD_SAF_ENC_DMEPOS_FILE.sh script - Failed"
	MSG="Listing Finder Files in S3 from ${S3BUCKET}${PREFIX} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${SAF_ENC_DMEPOS_EMAIL_SENDER}" "${SAF_ENC_DMEPOS_FNDR_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


# if zero files found --> end script
NOF_FILES=`wc -l ${DATADIR}temp_SAF_ENC_DMEPOS.txt  | awk '{print $1}' `	2>> ${LOGNAME}

echo "" >> ${LOGNAME}
echo "${NOF_FILES} SAF ENC DMEPOS Finder files found in S3." >> ${LOGNAME}

if [ ${NOF_FILES} -eq 0 ]; then
	echo "" >> ${LOGNAME}
	echo "No Finder files found in ${S3BUCKET}${PREFIX}." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="LOAD_SAF_ENC_DMEPOS_FILE.sh script - Failed "
	MSG="No Finder Files found in ${S3BUCKET}${PREFIX}."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${SAF_ENC_DMEPOS_EMAIL_SENDER}" "${SAF_ENC_DMEPOS_FNDR_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 0
	
# if more than one finder file found --> error --> which file to process?	
elif [ ${NOF_FILES} -gt 1 ]; then
	echo "" >> ${LOGNAME}
	echo "More than one Finder files found in ${S3BUCKET}${PREFIX}." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="LOAD_SAF_ENC_DMEPOS_FILE.sh script - Failed "
	MSG="More than one Finder Files found in ${S3BUCKET}${PREFIX}."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${SAF_ENC_DMEPOS_EMAIL_SENDER}" "${SAF_ENC_DMEPOS_FNDR_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
 	
fi


# Extract just the filename from the S3 filename information
filename=`awk '{print $4}' ${DATADIR}temp_SAF_ENC_DMEPOS.txt` 

echo "" >> ${LOGNAME}
echo "SAF ENC DMEPOS Finder file found: ${filename}" >> ${LOGNAME}


#################################################################################
# Copy S3 SAF ENC CAR Finder File to Linux. 
# S3 cp --include option does not properly filter results when copying contents 
#         from a folder
#################################################################################
echo "" >> ${LOGNAME}
echo "Copy SAF ENC DMEPOS Finder file from s3 to linux " >> ${LOGNAME}

# Copy S3 file to linux
aws s3 cp s3://${S3BUCKET}${filename} ${DATADIR}${filename}  1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Copying S3 SAF ENC CAR Finder file to Linux failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="LOAD_SAF_ENC_DMEPOS_FILE.sh  - Failed"
	MSG="Copying S3 file from ${S3BUCKET} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${SAF_ENC_DMEPOS_EMAIL_SENDER}" "${SAF_ENC_DMEPOS_FNDR_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	

# uncompress file if needed
echo "" >> ${LOGNAME}
echo "Unzip Finder file if needed " >> ${LOGNAME}

gzip -d ${DATADIR}${filename}  2>>  ${LOGNAME}

# load export variable with finder filename
LOAD_SAF_ENC_DMEPOS_FILE=${filename}


#############################################################
# Execute Python code to load Finder File to SAF ENC DMEPOS table.
#############################################################
echo "" >> ${LOGNAME}
echo "Start execution of LOAD_SAF_ENC_DMEPOS_FILE.py program"  >> ${LOGNAME}

# Export environment variables for Python code
export DATADIR
export LOAD_SAF_ENC_DMEPOS_FILE

${PYTHON_COMMAND} ${RUNDIR}LOAD_SAF_ENC_DMEPOS_FILE.py >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script - Load Finder File
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script LOAD_SAF_ENC_DMEPOS_FILE.py failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="LOAD_SAF_ENC_DMEPOS_FILE.sh  - Failed"
		MSG="SAF ENC DMEPOS loading finder file has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${SAF_ENC_DMEPOS_EMAIL_SENDER}" "${SAF_ENC_DMEPOS_FNDR_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script LOAD_SAF_ENC_DMEPOS_FILE.py completed successfully. " >> ${LOGNAME}


#################################################################################
# MOVE S3 SAF ENC CAR Finder File to archive folder when loaded into table.
#################################################################################
echo "" >> ${LOGNAME}
echo "Move SAF ENC CAR Finder file to archive folder after successful load into table"  >> ${LOGNAME}

# move S3 finder file to archive folder
aws s3 mv s3://${S3BUCKET}${filename} s3://${S3BUCKET}archive/${filename}  1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Copying S3 SAF ENC CAR Finder file to Linux failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="LOAD_SAF_ENC_DMEPOS_FILE.sh  - Failed"
	MSG="Copying S3 file from ${S3BUCKET} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${SAF_ENC_DMEPOS_EMAIL_SENDER}" "${SAF_ENC_DMEPOS_FNDR_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	

#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "LOAD_SAF_ENC_DMEPOS_FILE.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS