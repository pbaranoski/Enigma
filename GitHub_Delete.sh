#!/usr/bin/bash
#
######################################################################################
# Name:  GitHub_Delete.sh
#
# Desc: Remove obsolete source code from server which GitHub could not do.
#       Move deleted members to GITHUB S3 archive folder for auditing purposes.
#
#
# Created: Paul Baranoski  06/17/2025
# Modified: 
#
# Paul Baranoski 2025-06-17 Create script.
#
######################################################################################
set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`

# Export TMSTMP variable for child scripts
export TMSTMP 

LOGNAME=/app/IDRC/XTR/CMS/logs/GitHub_Delete_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/
SOURCE_DIR=/app/IDRC/XTR/CMS/scripts/run/


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "GitHub_Delete.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh >> ${LOGNAME}

S3BUCKET=${GITHUB_BUCKET} 

echo "GITHUB bucket=${S3BUCKET}" >> ${LOGNAME}


##################################################################
# Verify that parameter year has been passed.
##################################################################
if ! [[ $# -eq 1 ]]
then
	echo "Incorrect # of parameters sent to script. NOF parameters: $#" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT=" GitHub_Delete.sh  - Failed (${ENVNAME})"
	MSG="Incorrect # of parameters sent to script. NOF parameters: $#. Process failed. "
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12		
			
fi


#############################################################
# Display parameters passed to script 
#############################################################
File2Delete=$1

echo " " >> ${LOGNAME}
echo "Parameters to script: " >> ${LOGNAME}
echo "NOF parameters for script: " $# >> ${LOGNAME}
echo "   File2Delete=${File2Delete} " >> ${LOGNAME}


#################################################
# Set current working directory		
#################################################
echo "" >> ${LOGNAME}
echo "Set working directory to ${SOURCE_DIR}" >> ${LOGNAME}

cd ${SOURCE_DIR}  >> ${LOGNAME}  2>&1


#################################################################################
# Verify that file exists. 
#################################################################################
echo "" >> ${LOGNAME}
echo "Verify that ${File2Delete} exists on ${SOURCE_DIR} " >> ${LOGNAME}

echo "" >> ${LOGNAME}
ls -l ${SOURCE_DIR}${File2Delete}  >> ${LOGNAME}  2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Verify that ${File2Delete} exists on ${SOURCE_DIR} - file does not exist.  Nothing to do. Ending gracefully. " >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="GitHub_Delete.sh - ($ENVNAME)"
	MSG="Verify that ${File2Delete} exists on ${SOURCE_DIR} - file does not exist. Nothing to do. Ending gracefully."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 0
fi	


#################################################################################
# pwd /scripts/run directory contents before copying modules from S3. 
#################################################################################
echo "" >> ${LOGNAME}
echo "Display contents of `pwd` before copying modules from S3 " >> ${LOGNAME}

echo "" >> ${LOGNAME}
echo "`ls -l`"  >> ${LOGNAME}  2>&1


##########################################################
# Copy Module in S3 to archive_delete directory (with timestamp)
##########################################################
echo "" >> ${LOGNAME}
echo "Copy removed source module from ${SOURCE_DIR}${File2Delete} to s3://${GITHUB_BUCKET}archive_delete_${TMSTMP}/${File2Delete} " >> ${LOGNAME}

aws s3 cp ${SOURCE_DIR}${File2Delete} s3://${GITHUB_BUCKET}archive_delete_${TMSTMP}/${File2Delete}  1>> ${LOGNAME} 2>&1
echo "cp ${SOURCE_DIR}${File2Delete} to s3://${GITHUB_BUCKET}archive_delete_${TMSTMP}/${File2Delete} " 1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Copy removed source module to s3://${GITHUB_BUCKET}archive_delete_${TMSTMP}/${File2Delete} failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="GitHub_Delete.sh - Failed ($ENVNAME)"
	MSG="Copy removed source module to s3://${GITHUB_BUCKET}archive_delete_${TMSTMP}/${File2Delete} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	
	

#################################################################################
# Remove obsolete code module file from linux 
#################################################################################
echo "" >> ${LOGNAME}
echo "Remove obsolete code module ${File2Delete} from linux " >> ${LOGNAME}

rm ${SOURCE_DIR}${File2Delete}  >> ${LOGNAME} 2>&1
	

#################################################################################
# pwd /scripts/run directory contents after copying modules from S3. 
#################################################################################
echo "" >> ${LOGNAME}
echo "Display contents of `pwd` after removing obsolete code module from linux  " >> ${LOGNAME}

echo "" >> ${LOGNAME}
echo "`ls -l`"  >> ${LOGNAME}  2>&1


#############################################################
# Send success email of DSH Extract files
#############################################################
echo "" >> ${LOGNAME}
echo "Send success email." >> ${LOGNAME}

# Send Success email	
SUBJECT="GitHub Delete scripts - completed ($ENVNAME)"
MSG="GitHub Delete script completed. \n\nThe following obsolete code module was removed:\n\n${File2Delete}."
${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Error in calling sendEmail.py" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Sending Success email in GitHub_Delete.sh  - Failed (${ENVNAME})"
	MSG="Sending Success email in GitHub_Delete.sh has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	

 
#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "GitHub_Delete.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS
