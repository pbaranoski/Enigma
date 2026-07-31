#!/usr/bin/bash
############################################################################################################
# Name:  ASC_PTB_SF_Table_Load.sh
#
# Desc: ASC (Ambulatory Surgical Center PTB extract. Designed to run in Annually in April
#
# Execute as ./ASC_PTB_SF_Table_Load.sh $1
#
#  $1 = EFT_FILENAME
#
#
# Author     : Paul Baranoski	
# Created    : 01/20/2023
#
# Modified:
#
# Paul Baranoski 2025-09-22 Created script.
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/ASC_PTB_SF_Table_Load_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "ASC_PTB_SF_Table_Load.sh started at `date` " >> ${LOGNAME}


#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh


#################################################################################
# Accept EFT Filename parameter
#################################################################################
if ! [[ $# -eq 1 ]]
then
	echo " " >> ${LOGNAME}
	echo "Incorrect # of parameters sent to script. NOF parameters: $#" >> ${LOGNAME}
	exit 12
fi


p_EFT_FILENAME=$1

echo " " >> ${LOGNAME}
echo "Parameters to script: " >> ${LOGNAME}
echo "NOF parameters for script: " $# >> ${LOGNAME}
echo "   p_EFT_FILENAME=${p_EFT_FILENAME} " >> ${LOGNAME}


#############################################################
# Is ASC PTB parameter file in s3? 
#############################################################
echo "" >> ${LOGNAME}
echo "Find EFT File to load into ASC PTB SF table: " >> ${LOGNAME}
aws s3 ls s3://${bucket}EFT_Files/${p_EFT_FILENAME}  >> ${LOGNAME}
		
# Ex. Total Objects: 14 --> " 14" --> "14"
NOF_FILES=`aws s3 ls s3://${bucket}EFT_Files/${p_EFT_FILENAME} --summarize | grep 'Total Objects' | cut -d: -f2 | sed 's/ //g' ` 2>> ${LOGNAME}

RET_STATUS=$?

if [[ $RET_STATUS = 0 ]]; then

	# We have files to process
	if [[ ${NOF_FILES} -eq 1 ]]; then

		echo "" >> ${LOGNAME}
		echo "EFT file ${p_EFT_FILENAME} was found in S3 EFT_Files bucket/folder"  >> ${LOGNAME}
		
	else
		# File not found
		echo "" >> ${LOGNAME}
		echo "ASC_PTB_SF_Table_Load.sh failed - EFT Filename ${p_EFT_FILENAME} not found in EFT_Files folder " >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="ASC_PTB_SF_Table_Load.sh failed (${ENVNAME})"
		MSG="ASC_PTB_SF_Table_Load.sh failed - EFT Filename ${p_EFT_FILENAME} not found in EFT_Files folder"
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12		
	
	fi
	
else

	echo "" >> ${LOGNAME}
	echo "Shell script ASC_PTB_SF_Table_Load.sh failed. (${ENVNAME})" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="ASC_PTB_SF_Table_Load.sh failed (${ENVNAME})"
	MSG="Listing EFT Files in s3://${bucket}EFT_Files/${p_EFT_FILENAME} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12

fi	

#############################################################
# Extract Key values from S3 filename
#############################################################
# Ex. P#EFT.ON.ASCPS.Y2023.MAR24.D240430.T0800051

EXT_FILENAME=${p_EFT_FILENAME}
# Ex. P#EFT.ON.ASCPS.Y2023.MAR24.D240430.T0800051 --> Y2023 --> 2023
EXT_SRVC_YR=`echo ${EXT_FILENAME} | cut -d. -f4 | cut -c2- `

# Ex. P#EFT.ON.ASCPS.Y2023.MAR24.D240430.T0800051 --> D240430 --> 240430
YYMMDD=`echo ${EXT_FILENAME} | cut -d. -f6 | cut -c2-7 `
EXT_RUN_DT="20${YYMMDD}"

echo "" >> ${LOGNAME}
echo "EXT_FILENAME=${EXT_FILENAME}" >> ${LOGNAME}
echo "EXT_SRVC_YR=${EXT_SRVC_YR}" >> ${LOGNAME}
echo "EXT_RUN_DT=${EXT_RUN_DT}" >> ${LOGNAME}

		
#############################################################
# Execute Python code to extract data.
#############################################################
echo "" >> ${LOGNAME}
echo "Start execution of ASC_PTB_SF_Table_Load.py program"  >> ${LOGNAME}

# Export environment variables for Python code
export EXT_SRVC_YR 
export EXT_RUN_DT
export EXT_FILENAME
		

${PYTHON_COMMAND} ${RUNDIR}ASC_PTB_SF_Table_Load.py >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script  
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Python script ASC_PTB_SF_Table_Load.py failed" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="ASC_PTB_SF_Table_Load.sh - Failed (${ENVNAME})"
	MSG="ASC_PTB_SF_Table_Load.sh has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script ASC_PTB_SF_Table_Load.py completed successfully. " >> ${LOGNAME}


#############################################################
# Send Success email.
#############################################################
echo "" >> ${LOGNAME}
echo "Send success email with S3 Extract filename." >> ${LOGNAME}

SUBJECT="ASC_PTB_SF_Table_Load_Driver.py successfully loaded data into ASC_PTB SF table. (${ENVNAME})" 
MSG="ASC_PTB_SF_Table_Load_Driver.py successfully loaded ASC PTB EFT Extract file ${EXT_FILENAME} into SF."

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Error in calling sendEmail.py" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Sending Success email in ASC_PTB_SF_Table_Load.sh - Failed (${ENVNAME})"
	MSG="Sending Success email in ASC_PTB_SF_Table_Load.sh  has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	


#############################################################
# End script
#############################################################
echo "" >> ${LOGNAME}
echo "ASC_PTB_SF_Table_Load.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

exit $RET_STATUS