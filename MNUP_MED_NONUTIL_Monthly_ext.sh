#!/usr/bin/bash
############################################################################################################
# Name:  MNUP_MED_NONUTIL_Monthly_ext.sh
#
# Desc: MNUP Monthly Extract of Medical Non-Utilization for SSA
# !!!!! NOTE : - FOR PREVIOUS MONTH WE NEED TO RUN THIS EXTRACT BEFORE 27TH OF EACH MONTH OTHERWISE IT WILL NOT GIVE CORRECT DATE
# Author     Viren Khanna
# Created    : 07/23/2024
#
# Modified:
#
# Viren Khanna 2024-07-23 Created script.
# Viren Khanna 2024-09-23 Updated SFTP_DEST folder
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/MNUP_MED_NONUTIL_Monthly_ext_${TMSTMP}.log
EFT_LOGNAME=/app/IDRC/XTR/CMS/logs/ProcessFiles2EFT_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

MANIFEST_FILE_HLQ=MNUP_Monthly
SFTP_DEST_FLDR=MNTHLYMNUP

PREFIX=MNUP_MONTHLY_FNDR

touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "MNUP_MED_NONUTIL_ext.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh

source ${RUNDIR}FilenameCounts.bash

S3BUCKET=${MNUP_MONTHLY_BUCKET} 

echo "MNUP_MONTHLY_BUCKET=${S3BUCKET}" >> ${LOGNAME}
echo "Finder files bucket=${FINDER_FILE_BUCKET}" >> ${LOGNAME}
echo "Finder files SSA bucket=${FINDER_FILE_SSA_BUCKET}" >> ${LOGNAME}
echo "SFTP_FOLDER=${SFTP_FOLDER}" >> ${LOGNAME}

echo "MANIFEST_SSA_BUCKET=${MANIFEST_SSA_BUCKET}" >> ${LOGNAME}


#############################################################
# Execute Script to load Finder File table into S3
#############################################################
echo "" >> ${LOGNAME}
echo "Execute script LOAD_MNUP_Monthly_FNDR_FILE.sh"  >> ${LOGNAME}
${RUNDIR}LOAD_MNUP_Monthly_FNDR_FILE.sh   2>> ${LOGNAME}


#############################################################
# Check the status of script 
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "LOAD_MNUP_FNDR_FILE.sh failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="LOAD_MNUP_Monthly_FNDR_FILE.sh  - Failed (${ENVNAME})"
		MSG="LOAD_MNUP_Monthly_FNDR_FILE.sh has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${MNUP_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "LOAD_MNUP_Monthly_FNDR_FILE.sh completed successfully. " >> ${LOGNAME}


#################################################################################
# Create Extract Date parameter
#################################################################################
echo " " >> ${LOGNAME}
echo "Create date parameter for the Python Extract program." >> ${LOGNAME}

# Create date parameter for Prior Year
CUR_YY_MM=`date -d "$(date +%Y-%m-01) -1 day" +%Y-%m`
echo "CUR_YY_MM=${CUR_YY_MM}" >> ${LOGNAME}

MNUP_YR=`echo ${CUR_YY_MM} | cut -c1-4` >> ${LOGNAME}  2>&1
PRIOR_MM=`echo ${CUR_YY_MM} | cut -c6-7` >> ${LOGNAME}  2>&1
echo "MNUP_YR=${MNUP_YR}"  >> ${LOGNAME}
echo "PRIOR_MM=${PRIOR_MM}"  >> ${LOGNAME}

CUR_YYYY=`date +%Y`  >> ${LOGNAME}  2>&1
PRIOR_YYYY=`expr ${CUR_YYYY} - 1`
echo "PRIOR_YYYY=${PRIOR_YYYY}" >> ${LOGNAME}
echo "CUR_YYYY=${CUR_YYYY}"  >> ${LOGNAME}

#############################################################
# Execute Python code to extract data.
#############################################################
echo "" >> ${LOGNAME}
echo "Start execution of MNUP_MED_NONUTIL_Monthly_ext.py program"  >> ${LOGNAME}

# Export environment variables for Python code
export TMSTMP
export MNUP_YR
export PRIOR_YYYY
export PRIOR_MM

${PYTHON_COMMAND} ${RUNDIR}MNUP_MED_NONUTIL_Monthly_ext.py >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script  
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script MNUP_MED_NONUTIL_Monthly_ext.py failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="MNUP_MED_NONUTIL_Monthly_ext.sh  - Failed (${ENVNAME}) "
		MSG="MNUP Extracting Bene Info has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${MNUP_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script MNUP_MED_NONUTIL_Monthly_ext.py completed successfully. " >> ${LOGNAME}


################################################################
# Create EFT/SFTP Extract file - 
# NOTE: Use override of S3 EFT Destination folder
################################################################
echo " " >> ${LOGNAME}
echo "EFT MNUP Monthly Extract File " >> ${LOGNAME}

${RUNDIR}ProcessFiles2EFT.sh ${S3BUCKET} ${SFTP_FOLDER} >> ${LOGNAME} 2>&1


#############################################################
# Check the status of extract script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script ProcessFiles2EFT.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT=" MNUP Monthly EFT process  - Failed (${ENVNAME})"
	MSG=" MNUP Monthly EFT process has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${MNUP_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


#############################################################
# Get SFTP Filename for success email.
#############################################################
echo "" >> ${LOGNAME}
echo "Get SFTP filename for email." >> ${LOGNAME}
SFTP_FILENAME=`grep "FINAL MF_FILENAME=" ${EFT_LOGNAME} | cut -d= -f2 ` 2>> ${LOGNAME}

echo "SFTP_FILENAME=${SFTP_FILENAME}" >> ${LOGNAME}


#############################################################
# Get list of S3 files and record counts for success email.
#############################################################
echo "" >> ${LOGNAME}
echo "Get S3 Extract file list and record counts" >> ${LOGNAME}

getExtractFilenamesAndCounts ${LOGNAME}  >> ${LOGNAME} 2>&1
S3Files="${filenamesAndCounts}" 


#############################################################
# Send Success email.
#############################################################
echo "" >> ${LOGNAME}
echo "Send success email with S3 Extract filename." >> ${LOGNAME}
echo "S3Files=${S3Files} "   >> ${LOGNAME}

SUBJECT="MNUP Monthly extract (${ENVNAME})" 
MSG="The Medicare Non-Usage (MNUP) Monthly extract file has been created.\n\nAn SFTP version of the below file was created as ${SFTP_FILENAME}.\n\nThe following file was created:\n\n${S3Files}"


${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${MNUP_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Error in calling sendEmail.py" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Sending Success email in MNUP_MED_NONUTIL_Monthly_ext.sh  - Failed (${ENVNAME})"
		MSG="Sending Success email in MNUP_MED_NONUTIL_Monthly_ext.sh  has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
fi	


#################################################################################
# Extract just the filename from the S3 filename information
#################################################################################
echo "" >> ${LOGNAME}
echo "Get MNUP Finder File filename" >> ${LOGNAME}

MNUP_FINDER_FILE=`aws s3 ls s3://${FINDER_FILE_SSA_BUCKET}${PREFIX} | awk '{print $4}' ` 2>> ${LOGNAME}

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Get MNUP Finder File filename from ${FINDER_FILE_SSA_BUCKET}${PREFIX} failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="LOAD_MNUP_FNDR_Monthly_FILE.sh script - Failed (${ENVNAME})"
	MSG="Get MNUP Monthly Finder File filename in S3 from ${FINDER_FILE_SSA_BUCKET}${PREFIX} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi

echo "MNUP Finder file found: ${MNUP_FINDER_FILE}" >> ${LOGNAME}


#################################################################################
# Move finder file in S3 to archive folder. 
#################################################################################
echo "" >> ${LOGNAME}
echo "Move processed finder file ${MNUP_FINDER_FILE} to S3 Finder File archive folder." >> ${LOGNAME}

# Move S3 file to archive folder
aws s3 mv s3://${FINDER_FILE_SSA_BUCKET}${MNUP_FINDER_FILE} s3://${FINDER_FILE_BUCKET}archive/${MNUP_FINDER_FILE}  1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Moving S3 MNUP Finder file ${MNUP_FINDER_FILE} to S3 archive folder." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="MNUP_MED_NONUTIL_Monthly_ext.sh  - Failed (${ENVNAME})"
	MSG="Moving S3 file ${MNUP_FINDER_FILE} to s3 folder ${FINDER_FILE_BUCKET}archive failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	

################################################################
# Create Manifest file for SFTP of file
################################################################
echo "" >> ${LOGNAME}
echo "Create Manifest file for SFTP of MNUP Monthly Extract.  " >> ${LOGNAME}

# Convert TMSTMP to EFT timestamp for call to CreateManifestFile.sh
YYMMDD=`echo ${TMSTMP} | cut -c3-8 `
HHMMSS=`echo ${TMSTMP} | cut -c10-15 `
EFT_TMSTMP="R${YYMMDD}.T${HHMMSS}"
echo "EFT_TMSTMP=${EFT_TMSTMP}" >> ${LOGNAME}


################################################
# $1 = bucket/folder where file(s) referenced in manifest file are located 
# $2 = timestamp of file(s) to include (how to find file in folder)
# $3 = Manifest file email addresses
# $4 = where to place manifest file
# $5 = HLQ of manifest .json filename
# $6 = the dataRequestID = Destination folder name
################################################
${RUNDIR}CreateManifestSFTPFile.sh ${SFTP_BUCKET} ${EFT_TMSTMP} ${MNUP_EMAIL_BOX_RECIPIENT} ${MANIFEST_SSA_BUCKET} ${MANIFEST_FILE_HLQ} ${SFTP_DEST_FLDR}


#############################################################
# Check the status of script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script CreateManifestSFTPFile.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Create Manifest file in MNUP_MED_NONUTIL_ext.sh - Failed (${ENVNAME})"
	MSG="Create Manifest file in MNUP_MED_NONUTIL_ext.sh has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "MNUP_MED_NONUTIL_Monthly_ext.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS