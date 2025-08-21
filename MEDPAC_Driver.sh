#!/usr/bin/sh
############################################################################################################
# Name: MEDPAC_Driver.sh
#
# Desc: MEDPAC HOSPICE Annual Extract
#
# Author     : Joshua Turner	
# Created    : 1/27/2023
#
# Modified:
# Joshua Turner 	2023-01-27 	New script.
############################################################################################################
set +x

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source /app/IDRC/XTR/CMS/scripts/run/SET_XTR_ENV.sh
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
LOGNAME=/app/IDRC/XTR/CMS/logs/MEDPAC_Driver_${TMSTMP}.log
touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "MEDPAC_Driver.sh started at ${TMSTMP} " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# Establish Date Parameters  
#############################################################
YEAR=`date +%Y`
MONTH=`date +%m`
CURR_DATE=`date +%Y%m%d`

echo "MEDPAC HOSPICE File Extract Processing with the following dates:" >> ${LOGNAME}
echo "YEAR: ${YEAR}" >> ${LOGNAME}
echo "MONTH: ${MONTH}" >> ${LOGNAME}
echo "" >> ${LOGNAME}

###########################################################################################
# Execute python script to extract MEDPAC HOSPICE data and load the extract to S3 
###########################################################################################
export YEAR
export TMSTMP

echo ""
echo "Start MEDPAC_Extract.py." >> ${LOGNAME}
${PYTHON_COMMAND} ${RUNDIR}MEDPAC_Extract.py >> ${LOGNAME} 2>&1

RET_STATUS=$?
if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Python script MEDPAC_Extract.py failed." >> ${LOGNAME}
	
	# Send failure email
	SUBJECT="MEDPAC HOSPICE Extract FAILED"
	MSG="MEDPAC HOSPICE extract has failed in MEDPAC_Extract.py."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${MEDPAC_EMAIL_SENDER}" "${MEDPAC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
	exit 12
fi

###########################################################################################
# Get the number of records written to the extract file. This will be included in the success
# email. Use awk to extract this number from the logfile
###########################################################################################
NO_OF_RECS=$(awk -F "," '/rows_unloaded/{getline;print $1}' ${LOGNAME})

###########################################################################################
# Concatenate MEDPAC HOSPICE S3 files into a single file 
###########################################################################################
echo ""
echo "Concatenate S3 files using CombineS3Files.sh." >> ${LOGNAME}
MEDPAC_FILE=MEDPAC_Y${YEAR}_FILE_${TMSTMP}.csv.gz
${RUNDIR}CombineS3Files.sh ${MEDPAC_BUCKET} ${MEDPAC_FILE} 

RET_STATUS=$?
if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script CombineS3Files.sh failed." >> ${LOGNAME}
	
	# Send failure email
	SUBJECT="MEDPAC HOSPICE S3 file concatenation FAILED"
	MSG="MEDPAC HOSPICE extract has failed in the CombineS3Files step of MEDPAC_Driver.sh."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${MEDPAC_EMAIL_SENDER}" "${MEDPAC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
	exit 12
fi

###########################################################################################
# Send Success Email
###########################################################################################
echo ""
echo "Sending success email" >> ${LOGNAME}
SUBJECT="MEDPAC ANNUAL EXTRACT : ${CURR_DATE}"
MSG="THE ANNUAL MEDPAC EXTRACTS HAVE BEEN COMPLETED.\n\n======================================================================\n\nFile Name						No of Records\n=========================================	=======================\n${MEDPAC_FILE}		${NO_OF_RECS}"
${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${MEDPAC_EMAIL_SENDER}" "${MEDPAC_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1


TMSTMP=`date +%Y%m%d.%H%M%S`
echo "" >> ${LOGNAME}
echo "MEDPAC HOSPICE Annual Extract completed successfully." >> ${LOGNAME}
echo "MEDPAC_Driver.sh ended at: ${TMSTMP}" >> ${LOGNAME}
exit $RET_STATUS
