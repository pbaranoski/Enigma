#!/usr/bin/bash
############################################################################################################
# Script Name: BAYSTATE_RRB_Extract.sh
# Description: This script executes the RRB extraction python script for MEDPAR Baystate
#
# Author    : Joshua Turner
# Created   : 10/06/2023
#
# Modified:             Date:        Description:
# --------------------  -----------  -------------------------------------------------------------------
# Joshua Turner         2024-01-09   Updated for coding standards.
# Joshua Turner         2024-02-02   Updated to correct filenames 
# Paul Baranoski        2025-01-28   Change MEDPAR_BAYSTATE_EMAIL_SENDER to CMS_EMAIL_SENDER. 
#                                    Change MEDPAR_BAYSTATE_EMAIL_FAILURE_RECIPIENT to ENIGMA_EMAIL_FAILURE_RECIPIENT
############################################################################################################
set +x
#################################################################################
# Establish log file  
#################################################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
CUR_DT=`date +%Y%m%d`
LOGNAME=/app/IDRC/XTR/CMS/logs/BAYSTATE_RRB_Extract_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

#################################################################################
# Included to produce filenames and counts from the extracts
#################################################################################
source ${RUNDIR}FilenameCounts.bash

echo "################################### " >> ${LOGNAME}
echo "BAYSTATE_RRB_Extract.sh started at `date` " >> ${LOGNAME}

#################################################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#################################################################################
source ${RUNDIR}SET_XTR_ENV.sh
S3BUCKET=${MEDPAR_BAYSTATE_BUCKET}

#################################################################################
# Set exctract filename parameters based on the current month.
# There two runs in the year:
#    January - file to be named with FY01{PRIOR_YY}
#    March   - file to be named with FY03{CURR_YY}
#################################################################################
echo "" >> ${LOGNAME}
echo "Calculate parameters based on current calendar month" >> ${LOGNAME}

MONTH=`date +%m`

if [ ${MONTH} -lt 3 ]; then
	YEAR=`date -d "-1 year" +%y`
	FNAME_SUFFIX="FY01${YEAR}"
else
	YEAR=`date +%y`
	FNAME_SUFFIX="FY03${YEAR}"
fi

echo "Current Run Year: ${YEAR}" >> ${LOGNAME}
echo "Current Run Month: ${MONTH}" >> ${LOGNAME}

export TMSTMP
export FNAME_SUFFIX

#################################################################################
# Execute Python code to produce Baystate RRB SSN extract
#################################################################################
echo "" >> ${LOGNAME}
echo "Start execution of BAYSTATE_RRB_Extract.py program"  >> ${LOGNAME}

${PYTHON_COMMAND} ${RUNDIR}BAYSTATE_RRB_Extract.py >> ${LOGNAME} 2>&1

#################################################################################
# Check the status of python script 
#################################################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script BAYSTATE_RRB_Extract.py failed" >> ${LOGNAME}
		
        # Send Failure email	
        SUBJECT="BAYSTATE_RRB_Extract.sh  - Failed (${ENVNAME})"
        MSG="MEDPBAR Baystate RRB SSN extract has failed."
        ${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script BAYSTATE_RRB_Extract.py completed successfully. " >> ${LOGNAME}

###########################################################################################
# Get a list of all S3 files for success email
###########################################################################################
echo "" >> ${LOGNAME}
echo "Get S3 Extract file list" >> ${LOGNAME}

getExtractFilenamesAndCounts ${LOGNAME}  >> ${LOGNAME}  2>&1
S3Files="${filenamesAndCounts}"
echo "" >> ${LOGNAME}

#################################################################################
# Send success email
#################################################################################
echo "" >> ${LOGNAME}
echo "Send success email with S3 Extract filename." >> ${LOGNAME}
echo "S3Files=${S3Files} "   >> ${LOGNAME}

SUBJECT="BAYSTATE_RRB_Extract.sh  - Completed (${ENVNAME})"
MSG="MEDPAR Baystate RRB SSN Extract has completed successfully.\n\nThe following file(s) were created:\n\n${S3Files}"

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${MEDPAR_BAYSTATE_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Error in calling sendEmail.py" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Sending Success email in BAYSTATE_RRB_Extract.sh - Failed (${ENVNAME})"
		MSG="Sending Success email in BAYSTATE_RRB_Extract.sh  has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
fi

#############################################################
# EFT Extract file and check the status of the script
#############################################################
echo "" >> ${LOGNAME}
echo "EFT MEDPAR Baystate RRB SSN Extract.  " >> ${LOGNAME}

${RUNDIR}ProcessFiles2EFT.sh ${S3BUCKET}  >> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script CreateManifestFile.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="EFT file step in BAYSTATE_RRB_Extract.sh - Failed (${ENVNAME})"
	MSG="EFT file step in BAYSTATE_RRB_Extract.sh has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	

#################################################################################
# script clean-up and send success email
#################################################################################
echo "" >> ${LOGNAME}
echo "BAYSTATE_RRB_Extract.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS