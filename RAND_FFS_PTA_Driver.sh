#!/usr/bin/sh
############################################################################################################
# Name: RAND_FFS_PTA_Driver.sh
#
# Desc: RAND FFS Part A Driver. This script will execute all RAND FFS Part A extracts (OPT, INP, SNF, HHA, HSP)
#       If one step fails, the script will continue but an email will be sent to indicate that one or more
#       extracts failed. The individual driver scripts can be used to rerun the failed portion(s).
#
# Author     : Joshua Turner	
# Created    : 3/3/2023
#
# Modified:
# Joshua Turner 	2023-03-03 	New script.
# Paul Baranoski    2023-12-11 Add $ENVNAME to SUBJECT line of all emails. 
############################################################################################################
set +x

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source /app/IDRC/XTR/CMS/scripts/run/SET_XTR_ENV.sh
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/
#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
LOGNAME=/app/IDRC/XTR/CMS/logs/RAND_FFS_PTA_Driver_${TMSTMP}.log
touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

ERROR="FALSE"

echo "################################################" >> ${LOGNAME}
echo "RAND_FFS_PTA_Driver.sh started at ${TMSTMP}" >> {LOGNAME}
echo "################################################" >> ${LOGNAME}
echo "" >> ${LOGNAME}

###########################################################################################
# Execute INP extract
###########################################################################################
echo "========================================================" >> ${LOGNAME}
echo "Starting INP extract script - RAND_FFS_PTA_INP.sh " >> ${LOGNAME}
bash ${RUNDIR}RAND_FFS_PTA_INP.sh >> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "An error occured during the INP extract process. Refer to the logs for remediation."
	echo "Processing for the remaining extracts will continue."
	ERROR="TRUE"
fi

###########################################################################################
# Execute OPT extract
###########################################################################################
echo "========================================================" >> ${LOGNAME}
echo "Starting OPT extract script - RAND_FFS_PTA_OPT.sh " >> ${LOGNAME}
bash ${RUNDIR}RAND_FFS_PTA_OPT.sh >> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "An error occured during the OPT extract process. Refer to the logs for remediation."
	echo "Processing for the remaining extracts will continue."
	ERROR="TRUE"
fi 
	
###########################################################################################
# Execute SNF extract
###########################################################################################
echo "========================================================" >> ${LOGNAME}
echo "Starting SNF extract script - RAND_FFS_PTA_SNF.sh " >> ${LOGNAME}
bash ${RUNDIR}RAND_FFS_PTA_SNF.sh >> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "An error occured during the SNF extract process. Refer to the logs for remediation."
	echo "Processing for the remaining extracts will continue."
	ERROR="TRUE"
fi 
	
###########################################################################################
# Execute HHA extract
###########################################################################################
echo "========================================================" >> ${LOGNAME}
echo "Starting HHA extract script - RAND_FFS_PTA_HHA.sh " >> ${LOGNAME}
bash ${RUNDIR}RAND_FFS_PTA_HHA.sh >> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "An error occured during the HHA extract process. Refer to the logs for remediation."
	echo "Processing for the remaining extracts will continue."
	ERROR="TRUE"
fi 
	
###########################################################################################
# Execute HOS extract
###########################################################################################
echo "========================================================" >> ${LOGNAME}
echo "Starting HOS extract script - RAND_FFS_PTA_HOS.sh " >> ${LOGNAME}
bash ${RUNDIR}RAND_FFS_PTA_HOS.sh >> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "An error occured during the HOS extract process. Refer to the logs for remediation."
	ERROR="TRUE"
fi 
		
###########################################################################################
# Send success or failure email
###########################################################################################
echo "All extracts have completed." >> ${LOGNAME}
echo "Sending success/failure email" >> ${LOGNAME}
if [[ $ERROR = "TRUE" ]]; then
	# Send Failure email	
	SUBJECT="RAND_FFS_PTA_Driver.sh - Failed (${ENVNAME})"
	MSG="An error was encountered within one or more of the RAND FFS Part A extracts. Please refer to the logs for next steps."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${RAND_FFS_EMAIL_SENDER}" "${RAND_FFS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


SUBJECT="RAND FFS Part A Completed (${ENVNAME})" 
MSG="The RAND FFS Part A extracts from Snowflake have completed."
${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${RAND_FFS_EMAIL_SENDER}" "${RAND_FFS_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

###########################################################################################
# End script
###########################################################################################
echo "" >> ${LOGNAME}
echo "RAND_FFS_PTA_Driver.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

exit $RET_STATUS
