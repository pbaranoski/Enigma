#!/usr/bin/sh
############################################################################################################
# Name:   LOAD_STS_HOS_HHA_FNDR_FILE.sh
# DESC:   This python program loads the STS_HOS_HHA finder file to BIA_{ENV}.CMS_TARGET_XTR_{ENV}.STS_HOS_HHA_FF table.
#
# Created: Viren Khanna 
# Modified: 02/07/2025
#
# Viren Khanna 2025-02-07 Create script to load STS_HOS_HHA_FF table
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/LOAD_STS_HOS_HHA_FNDR_FILE_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "LOAD_STS_HOS_HHA_FNDR_FILE.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh

S3BUCKET=${FINDER_FILE_BUCKET} 
PREFIX=POS_File_iQIES

echo "STS_HOS_HHA_FNDR_FILE=${S3BUCKET}" >> ${LOGNAME}




#################################################################################
# Remove any residual STS HOS HHA Finder files in data directory.
#################################################################################
echo " " >> ${LOGNAME}
echo "Remove any residual Finder Files in data directory." >> ${LOGNAME}
rm ${DATADIR}POS_File_iQIES_*  >> ${LOGNAME}  2>&1


#################################################################################
# Find STS HOS HHA Finder Files in S3
#################################################################################
echo "" >> ${LOGNAME}
echo "Find S3 STS HOS HHA Finder Files in S3." >> ${LOGNAME}


# Get all filenames in S3 bucket that match filename prefix
aws s3 ls s3://${S3BUCKET}${PREFIX}  > ${DATADIR}tempSTS.txt  

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Listing S3 files from ${S3BUCKET}${PREFIX} failed (${ENVNAME})." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="LOAD_STS_HOS_HHA_FNDR_FILE.sh script - Failed (${ENVNAME})"
	MSG="Listing Finder Files in S3 from ${S3BUCKET}${PREFIX} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


# if zero files found --> end script
NOF_FILES=`wc -l ${DATADIR}tempSTS.txt | awk '{print $1}' `	2>> ${LOGNAME}

echo "" >> ${LOGNAME}
echo "${NOF_FILES} STS HOS HHA Finder files found in S3." >> ${LOGNAME}

if [ ${NOF_FILES} -eq 0 ]; then
	echo "" >> ${LOGNAME}
	echo "No Finder files found in ${S3BUCKET}${PREFIX}." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="LOAD_STS_HOS_HHA_FNDR_FILE.sh script - Failed (${ENVNAME}) "
	MSG="No Finder Files found in ${S3BUCKET}${PREFIX}."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 0
	
# if more than one finder file found --> error --> which file to process?	
elif [ ${NOF_FILES} -gt 1 ]; then
	echo "" >> ${LOGNAME}
	echo "More than one Finder files found in ${S3BUCKET}${PREFIX}." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="LOAD_STS_HOS_HHA_FNDR_FILE.sh script - Failed (${ENVNAME}) "
	MSG="More than one Finder Files found in ${S3BUCKET}${PREFIX}."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
 	
fi


# Extract just the filename from the S3 filename information
filename=`awk '{print $4}' ${DATADIR}tempSTS.txt` 

echo "" >> ${LOGNAME}
echo "STS HOS HHA Finder file found: ${filename}" >> ${LOGNAME}


#################################################################################
# Copy S3 STS HOS HHA Finder File to Linux. 
# S3 cp --include option does not properly filter results when copying contents 
#         from a folder
#################################################################################
echo "" >> ${LOGNAME}
echo "Copy STS HOS HHA Finder file from s3 to linux " >> ${LOGNAME}

# Copy S3 file to linux
aws s3 cp s3://${S3BUCKET}${filename} ${DATADIR}${filename}  1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Copying S3 STS HOS HHA Finder file to Linux failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="LOAD_STS_HOS_HHA_FNDR_FILE.sh  - Failed (${ENVNAME})"
	MSG="Copying S3 file from ${S3BUCKET} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	

# uncompress file if needed
echo "" >> ${LOGNAME}
echo "Unzip Finder file if needed " >> ${LOGNAME}

gzip -d ${DATADIR}${filename}  2>>  ${LOGNAME}

# load export variable with finder filename
STS_HOS_HHA_FINDER_FILE=${filename}


#############################################################
# Execute Python code to load Finder File to STS HOA HHA FF table.
#############################################################
echo "" >> ${LOGNAME}
echo "Start execution of LOAD_STS_HOS_HHA_FNDR_FILE.py program"  >> ${LOGNAME}

# Export environment variables for Python code
export DATADIR
export STS_HOS_HHA_FINDER_FILE

${PYTHON_COMMAND} ${RUNDIR}LOAD_STS_HOS_HHA_FNDR_FILE.py >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script - Load Finder File
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script LOAD_STS_HOS_HHA_FNDR_FILE.py failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="LOAD_STS_HOS_HHA_FNDR_FILE.sh  - Failed (${ENVNAME})"
		MSG="STS HOA HHA loading finder file has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script LOAD_STS_HOS_HHA_FNDR_FILE.py completed successfully. " >> ${LOGNAME}


#################################################################################
# MOVE S3 STS HOA HHA Finder File to archive folder when loaded into table.
#################################################################################
echo "" >> ${LOGNAME}
echo "Move STS HOA HHA Finder file to archive folder after successful load into table"  >> ${LOGNAME}

# move S3 finder file to archive folder
aws s3 mv s3://${S3BUCKET}${filename} s3://${S3BUCKET}archive/${filename}  1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Copying S3 STS HOA HHA Finder file to Linux failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="LOAD_STS_HOS_HHA_FNDR_FILE.sh  - Failed (${ENVNAME})"
	MSG="Copying S3 file from ${S3BUCKET} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	

#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "LOAD_STS_HOS_HHA_FNDR_FILE.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS