#!/usr/bin/bash
############################################################################################################
# Name:  ASC_PTB_Extract.sh
#
# Desc: ASC (Ambulatory Surgical Center PTB extract. Designed to run in Annually in April
#
# Execute as ./ASC_PTB_Extract.sh 
#
#
# Author     : Paul Baranoski	
# Created    : 01/20/2023
#
# Modified:
#
# Paul Baranoski 2023-01-20 Created script.
# Paul Baranoski 2023-07-26 Modify logic to force end_dt parameter to be {CURR_YR}0331.
#                           Modify logic to get filenames and record counts for email.
# Joshua Turner  2023-08-14 Added EFT functionality  
# Paul Baranoski 2024-09-12 Add (${ENVNAME}) to SUBJECT for all emails. 
#                           Changed emails to use CMS_EMAIL_SENDER and ENIGMA_EMAIL_FAILURE_RECIPIENT.
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/ASC_PTB_Extract_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "ASC_PTB_Extract.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}


#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh

source ${RUNDIR}FilenameCounts.bash

#############################################################
# Set variables to appropriate values for Historical processing.
#############################################################
echo " " >> ${LOGNAME}
echo "ASC PTB Extract started. " >> ${LOGNAME}
echo " " >> ${LOGNAME}

S3BUCKET=${ASC_PTB_BUCKET} 

echo "ASC PTB bucket=${S3BUCKET}" >> ${LOGNAME}

#################################################################################
# Create Date parameters for Extract
#     CLM_EFCT_DT Begin and End date parameters. Ex. 20210101 and 20220331
#     CLM_LINE_FROM_DT (Prior Year YYYY)
#################################################################################
echo " " >> ${LOGNAME}

CURR_DAY=`date +%d`
PRIOR_YYYY=`date -d "-1 year" +%Y`
CURR_YYYY=`date +%Y`

CLM_EFCT_DT_BEG="${PRIOR_YYYY}0101"
##CLM_EFCT_DT_END=`date -d "-${CURR_DAY} days" +%Y%m%d `
CLM_EFCT_DT_END="${CURR_YYYY}0301"
CLM_LINE_FROM_DT_YYYY=${PRIOR_YYYY}

echo "CLM_EFCT_DT_BEG=${CLM_EFCT_DT_BEG}" >> ${LOGNAME}
echo "CLM_EFCT_DT_END=${CLM_EFCT_DT_END}" >> ${LOGNAME}
echo "CLM_LINE_FROM_DT_YYYY=${CLM_LINE_FROM_DT_YYYY}" >> ${LOGNAME}


#############################################################
# Execute Python code to extract data.
#############################################################
echo "" >> ${LOGNAME}
echo "Start execution of ASC_PTB_Extract.py program"  >> ${LOGNAME}

# Export environment variables for Python code
export TMSTMP

export CLM_EFCT_DT_BEG
export CLM_EFCT_DT_END
export CLM_LINE_FROM_DT_YYYY
export CURR_YYYY
export PRIOR_YYYY

${PYTHON_COMMAND} ${RUNDIR}ASC_PTB_Extract.py >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script  
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script ASC_PTB_Extract.py failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="ASC_PTB_Extract.sh - Failed (${ENVNAME})"
		MSG="ASC PTB extract has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script ASC_PTB_Extract.py completed successfully. " >> ${LOGNAME}


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

SUBJECT="ASC PTB extract (${ENVNAME})" 
MSG="The Extract for the creation of the ASC PTB file from Snowflake has completed.\n\nThe following file(s) were created:\n\n${S3Files}"

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ASC_PTB_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Error in calling sendEmail.py" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Sending Success email in ASC_PTB_Extract.sh - Failed (${ENVNAME})"
	MSG="Sending Success email in ASC_PTB_Extract.sh  has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	

#############################################################
# EFT Extract files and check status of the extract script
#############################################################
echo " " >> ${LOGNAME}
echo "EFT ASC PTB Extract Files " >> ${LOGNAME}
${RUNDIR}ProcessFiles2EFT.sh ${S3BUCKET}  >> ${LOGNAME} 2>&1

RET_STATUS=$?

if [ $RET_STATUS != 0 ]; then
	echo "" >> ${LOGNAME}
	echo "Shell script ProcessFiles2EFT.sh failed" >> ${LOGNAME}

	#Send Failure email	
	SUBJECT="ASC_PTB_Extract.sh - Failed (${ENVNAME})"
	MSG="EFT for ASC PTB has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

   exit 12
fi


#############################################################
# End script
#############################################################
echo "" >> ${LOGNAME}
echo "ASC_PTB_Extract.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

exit $RET_STATUS