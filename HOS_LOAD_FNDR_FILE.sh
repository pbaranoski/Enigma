#!/usr/bin/sh
############################################################################################################
# Script Name: HOS_LOAD_FNDR_FILE.sh
# Description: This script executes the python that loads the finder files for H and M contract types to 
#              to the BIA_{ENV}.CMS_TARGET_XTR_{ENV}.HOSHFF and HOSMFF tables.
#
# Author     : Joshua Turner	
# Created    : 03/27/2023
#
# Modified:             Date:        Description:
# --------------------  -----------  ----------------------------------------------------------------------
# Joshua Turner 	2023-03-27   New script.
# Joshua Turner         2023-10-26   Created new Finder File stage in Snowflake so I removed the FF copy
#                                    from /Finder_Files to /HOS
############################################################################################################
set +x

#####################################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#####################################################################
source /app/IDRC/XTR/CMS/scripts/run/SET_XTR_ENV.sh
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/
#####################################################################
# Establish log file  
#####################################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
LOGNAME=/app/IDRC/XTR/CMS/logs/HOS_LOAD_FNDR_FILE_${TMSTMP}.log
touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME}

echo "#############################################################" >> ${LOGNAME}
echo "HOS_LOAD_FNDR_FILE.sh started at: ${TMSTMP}" >> ${LOGNAME}
echo "#############################################################" >> ${LOGNAME}

#####################################################################
# Locate finder files in S3 and copy to DATADIR
#####################################################################
echo "Locating Finder Files in ${FINDER_FILE_BUCKET}." >> ${LOGNAME}
aws s3 ls s3://${FINDER_FILE_BUCKET}HOS_HFILE_FF > ${DATADIR}tempHOS_HFILE.txt
RET_STATUS_1=$?
aws s3 ls s3://${FINDER_FILE_BUCKET}HOS_MFILE_FF > ${DATADIR}tempHOS_MFILE.txt
RET_STATUS_2=$?

if [[ ${RET_STATUS_1} != 0 ]] || [[ ${RET_STATUS_2} != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Get Finder Files list from S3 bucket ${FINDER_FILE_BUCKET} failed." >> ${LOGNAME}
	#SEND FAILURE EMAIL
	SUBJECT="HOS_LOAD_FNDR_FILE.sh - Failed in ${ENVNAME}"
	MSG="Listing finder files from S3 failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${HOS_EMAIL_SENDER}" "${HOS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
	exit 12
fi

#####################################################################
# Check the temp files to see if 1, many, or none were found in S3.
#####################################################################
NO_OF_H_FILES=`wc -l ${DATADIR}tempHOS_HFILE.txt | awk '{print $1}' ` 2>> ${LOGNAME}
NO_OF_M_FILES=`wc -l ${DATADIR}tempHOS_MFILE.txt | awk '{print $1}' ` 2>> ${LOGNAME}

if [[ ${NO_OF_H_FILES} -gt 1 ]] || [[ ${NO_OF_M_FILES} -gt 1 ]]; then
	echo "" >> ${LOGNAME}
	echo "More than 1 H-FILE or M-FILE was found in S3. Check ${FINDER_FILE_BUCKET}." >> ${LOGNAME}
	#SEND FAILURE EMAIL
	SUBJECT="HOS_LOAD_FNDR_FILE.sh - Failed in ${ENVNAME}"
	MSG="More than 1 Finder File was found in S3 bucket ${FINDER_FILE_BUCKET} for one or both contract types."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${HOS_EMAIL_SENDER}" "${HOS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
	exit 12
elif [[ ${NO_OF_H_FILES} -eq 0 ]] || [[ ${NO_OF_M_FILES} -eq 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "No finder files were found for one or both contract types in S3. Check ${FINDER_FILE_BUCKET}." >> ${LOGNAME}
	#SEND FAILURE EMAIL
	SUBJECT="HOS_LOAD_FNDR_FILE.sh - Failed in ${ENVNAME}"
	MSG="No finder files were found in S3 bucket ${FINDER_FILE_BUCKET} for one or both contract types."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${HOS_EMAIL_SENDER}" "${HOS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
	exit 12
fi

#####################################################################
# Copy Finder Files to the DATADIR and call the load script 
#####################################################################
echo "" >> ${LOGNAME}
echo "#############################################################" >> ${LOGNAME}
echo "Finder File Copy and Load started at: ${TMSTMP}" >> ${LOGNAME}
echo "Copying files to ${HOS_BUCKET}" >> ${LOGNAME}
echo "#############################################################" >> ${LOGNAME}

HOSHFF=`awk '{print $4}' ${DATADIR}tempHOS_HFILE.txt`
HOSMFF=`awk '{print $4}' ${DATADIR}tempHOS_MFILE.txt`

#aws s3 cp s3://${FINDER_FILE_BUCKET}${HOSHFF} s3://${HOS_BUCKET}${HOSHFF} 1>> ${LOGNAME} 2>&1
#RET_STATUS_1=$?
#aws s3 cp s3://${FINDER_FILE_BUCKET}${HOSMFF} s3://${HOS_BUCKET}${HOSMFF} 1>> ${LOGNAME} 2>&1
#RET_STATUS_2=$?

#if [[ ${RET_STATUS_1} != 0 ]] || [[ ${RET_STATUS_2} != 0 ]]; then
#	echo "" >> ${LOGNAME}
#	echo "Copying finder files from S3 bucket ${FINDER_FILE_BUCKET} failed." >> ${LOGNAME}
	#SEND FAILURE EMAIL IF EITHER COPY FAILED
#	SUBJECT="HOS_LOAD_FNDR_FILE.sh - Failed in ${ENVNAME}"
#	MSG="Copying finder files from S3 bucket ${FINDER_FILE_BUCKET} failed."
#	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${HOS_EMAIL_SENDER}" "${HOS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
#	exit 12
#fi

#####################################################################
# Execute load script for both files
#####################################################################
export DATADIR
export HOSHFF
export HOSMFF

${PYTHON_COMMAND} ${RUNDIR}HOS_LOAD_FNDR_FILE.py >> ${LOGNAME} 2>&1

#####################################################################
# Execute load script for both files
#####################################################################
RET_STATUS=$?
if [[ ${RET_STATUS} != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "HOS_LOAD_FNDR_FILE.py failed while loading the files to Snowflake" >> ${LOGNAME}
	#SEND FAILURE EMAIL IF EITHER COPY FAILED
	SUBJECT="HOS_LOAD_FNDR_FILE.sh - Failed in ${ENVNAME}"
	MSG="HOS_LOAD_FNDR_FILE.sh/py failed while loading the files to Snowflake"
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${HOS_EMAIL_SENDER}" "${HOS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
	exit 12
fi

#####################################################################
# If the load was successful, move files to the archive folder in S3
#####################################################################
aws s3 mv s3://${FINDER_FILE_BUCKET}${HOSHFF} s3://${FINDER_FILE_BUCKET}archive/${HOSHFF} 1>> ${LOGNAME} 2>&1
RET_STATUS_1=$?
aws s3 mv s3://${FINDER_FILE_BUCKET}${HOSMFF} s3://${FINDER_FILE_BUCKET}archive/${HOSMFF} 1>> ${LOGNAME} 2>&1
RET_STATUS_2=$?

if [[ ${RET_STATUS_1} != 0 ]] || [[ ${RET_STATUS_2} != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Moving finder files to S3 ${FINDER_FILE_BUCKET}archive failed." >> ${LOGNAME}
	#SEND FAILURE EMAIL IF EITHER MOVE FAILED
	SUBJECT="HOS_LOAD_FNDR_FILE.sh - WARNING in ${ENVNAME}"
	MSG="Moving finder files to S3 ${FINDER_FILE_BUCKET}archive failed. Please check the logs and manually archive the files. The process will continue."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${HOS_EMAIL_SENDER}" "${HOS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
fi

TMSTMP=`date +%Y%m%d.%H%M%S`
echo ""
echo "#############################################################" >> ${LOGNAME}
echo "Finder File Copy and Load completed at: ${TMSTMP}" >> ${LOGNAME}
echo "#############################################################" >> ${LOGNAME}
echo "HOS_LOAD_FNDR_FILE.sh completed successfully." >> ${LOGNAME}
exit $?