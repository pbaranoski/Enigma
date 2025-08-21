#!/usr/bin/bash
############################################################################################################
# Name:  SRTR_ENC_SNF_Extract.sh
#
# Desc: SRTR SNF Extract 
#
# Execute as ./SRTR_ENC_SNF_Extract.sh 
#
# NOTE: Script must be /usr/bin/bash instead of /usr/bin/sh because the included FilenameCounts.bash file requires some bash specific syntax for 
#       a paste command which uses "bash Process Substitution". The paste command expects files and not variables as input. However, 
#       "bash Process Substitution" makes variables appear to the command as files.  The purpose of doing it this way instead of using temp files 
#       is because we have multiple scripts using the same data directory that would use the same temp file, possibly interferring with each other. 
#
# Author     : Paul Baranoski	
# Created    : 02/10/2023
#
# Modified:
#
# Paul Baranoski 2023-02-10 Created script.
# Paul Baranoski 2023-03-09 Modified S3 location of SRTR_EXT_YEARS_PARM_FILE to S3 config folder. 
# Paul Baranoski 2023-05-18 Add EFT functionality. 
# Paul Baranoski 2023-06-02 Correct syntax in TMSTMP assignment from "=" to ":="
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP:=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/SRTR_ENC_SNF_Extract_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

SRTR_EXT_YEARS_PARM_FILE=SRTR_EXT_YEARS_PARM_FILE.txt


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "SRTR_ENC_SNF_Extract.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}


#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh

S3BUCKET=${SRTR_ENC_BUCKET} 
echo "SRTR ENC PTA_PTB bucket=${S3BUCKET}" >> ${LOGNAME}
echo "configuration file bucket=${CONFIG_BUCKET}" >> ${LOGNAME}

source ${RUNDIR}FilenameCounts.bash


#################################################################################
# Download SRTR Extract Years Parameter file from S3 to data directory.
# NOTE: Make sure that the last year record in the SRTR_EXT_YEARS_PARM_FILE has an 
#       ending \n. (Press <ENTER> after each year in file). 
#################################################################################
# Copy S3 file to linux
aws s3 cp s3://${CONFIG_BUCKET}${SRTR_EXT_YEARS_PARM_FILE} ${DATADIR}${SRTR_EXT_YEARS_PARM_FILE}  1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Copying S3 SRTR Extract Years Parameter file to Linux failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="SRTR Encounter SNF Extract - Failed"
	MSG="Copying S3 files from ${CONFIG_BUCKET} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${SRTR_ENC_EMAIL_SENDER}" "${SRTR_ENC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	


#################################################################################
# Find SRTR Extract Years Parameter file in data directory.
#################################################################################
ParmFile2Process=`ls ${DATADIR}${SRTR_EXT_YEARS_PARM_FILE}` 1>> ${LOGNAME}  2>&1

echo "" >> ${LOGNAME}
echo "SRTR Year Extract Parameter file: ${ParmFile2Process}" >> ${LOGNAME}


#################################################################################
# Loop thru SRTR Extract Years in parameter file.
# NOTE: The tr command will remove the \r that may appear in parameter file when
#       the file is uploaded from windows to S3. The \r character will 
#       prevent the file from being processed properly.
#################################################################################
years=`cat ${ParmFile2Process} | tr -d '\r' `

for line in $(echo $years ) 
do

		#############################################################
		# Start extract for next parameter year
		#############################################################
		echo " " >> ${LOGNAME}
		echo "-----------------------------------" >> ${LOGNAME}
		
		# Extract Year from Parameter file record
		echo "Parameter record=${line}" >> ${LOGNAME}

		#################################################################################
		# Load parameters for Extract
		#################################################################################
		echo " " >> ${LOGNAME}

		EXT_YEAR=${line}
		CLM_TYPE_LIT=SNF
		EXT_CLM_TYPES="4018,4021,4028"

		echo "CLM_TYPE_LIT=${CLM_TYPE_LIT}" >> ${LOGNAME}
		echo "EXT_CLM_TYPES=${EXT_CLM_TYPES}" >> ${LOGNAME}
		echo "EXT_YEAR=${EXT_YEAR}" >> ${LOGNAME}

		# Export environment variables for Python code
		export TMSTMP
		export EXT_CLM_TYPES
		export EXT_YEAR
		export CLM_TYPE_LIT


		#############################################################
		# Execute Python code to extract data.
		#############################################################
		echo "" >> ${LOGNAME}
		echo "Start execution of SRTR_ENC_SNF_Extract.py program"  >> ${LOGNAME}
		${PYTHON_COMMAND} ${RUNDIR}SRTR_ENC_PTA_Extract.py >> ${LOGNAME} 2>&1


		#############################################################
		# Check the status of python script  
		#############################################################
		RET_STATUS=$?

		if [[ $RET_STATUS != 0 ]]; then
				echo "" >> ${LOGNAME}
				echo "Python script SRTR_ENC_SNF_Extract.py failed" >> ${LOGNAME}
				
				# Send Failure email	
				SUBJECT="SRTR_ENC_SNF_Extract.sh  - Failed"
				MSG="SRTR Encounter SNF extract has failed."
				${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${SRTR_ENC_SNF_EMAIL_SENDER}" "${SRTR_ENC_SNF_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

				exit 12
		fi

		echo "" >> ${LOGNAME}
		echo "Python script SRTR_ENC_SNF_Extract.py completed successfully. " >> ${LOGNAME}

done


#############################################################
# Get list of S3 files for success email.
#############################################################
echo "" >> ${LOGNAME}
echo "Get S3 Extract file list and record counts" >> ${LOGNAME}

getExtractFilenamesAndCounts ${LOGNAME}  >> ${LOGNAME}  2>&1
S3Files="${filenamesAndCounts}"


#############################################################
# Send Success email.
#############################################################
echo "" >> ${LOGNAME}
echo "Send success email with S3 Extract filename." >> ${LOGNAME}
echo "S3Files=${S3Files} "   >> ${LOGNAME}

SUBJECT="SRTR Encounter SNF extract" 
MSG="The SRTR Encounter SNF extract from Snowflake has completed.\n\nThe following file(s) were created:\n\n${S3Files}"

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${SRTR_ENC_EMAIL_SENDER}" "${SRTR_ENC_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Error in calling sendEmail.py" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Sending Success email in SRTR_ENC_SNF_Extract.sh  - Failed"
	MSG="Sending Success email in SRTR_ENC_SNF_Extract.sh  has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${SRTR_ENC_EMAIL_SENDER}" "${SRTR_ENC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	


#############################################################
# EFT Extract files
#############################################################
echo " " >> ${LOGNAME}
echo "EFT SRTR Encounter SNF Extract Files " >> ${LOGNAME}
${RUNDIR}ProcessFiles2EFT.sh ${S3BUCKET}  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of extract script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script ProcessFiles2EFT.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="SRTR Encounter SNF EFT process  - Failed"
	MSG="SRTR Encounter SNF EFT process has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${SRTR_ENC_EMAIL_SENDER}" "${SRTR_ENC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


#############################################################
# End script
#############################################################
echo "" >> ${LOGNAME}
echo "SRTR_ENC_SNF_Extract.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

exit $RET_STATUS