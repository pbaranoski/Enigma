#!/usr/bin/bash
############################################################################################################
# Name: SRTR_FFS_PTB_Driver.sh
#
# Desc: SRTR FFS Part B Driver. This script will execute all SRTR FFS Part B extracts (CAR, DME)
#       If one step fails, the script will continue but an email will be sent to indicate that one or more
#       extracts failed. The individual driver scripts can be used to rerun the failed portion(s).
#
# Author     : Joshua Turner	
# Created    : 2/15/2023
#
# Modified:
# Joshua Turner 	2023-02-15 	New script.
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
LOGNAME=/app/IDRC/XTR/CMS/logs/SSRTR_FFS_PTB_Driver_${TMSTMP}.log
touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

ERROR="FALSE"

echo "################################################" >> ${LOGNAME}
echo "SRTR_FFS_PTB_Driver.sh started at ${TMSTMP}" >> {LOGNAME}

###########################################################################################
# Execute CAR extract
###########################################################################################
${RUNDIR}SRTR_FFS_PTB_CAR.sh >> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "An error occured during the CAR extract process. Refer to the logs for remediation."
	echo "Processing for the remaining extracts will continue."
	ERROR="TRUE"
fi 

###########################################################################################
# Execute DME extract
###########################################################################################
${RUNDIR}SRTR_FFS_PTB_DME.sh >> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "An error occured during the DME extract process. Refer to the logs for remediation."
	ERROR="TRUE"
fi 
		
###########################################################################################
# Send success or failure email
###########################################################################################
echo "Sending success/failure email" >> {LOGNAME}
if [[ $ERROR = "TRUE" ]]; then
	# Send Failure email	
	SUBJECT="SRTR_FFS_PTB_Driver.sh - Failed"
	MSG="An error was encountered within one or more of the SRTR FFS Part B extracts. Please refer to the logs for next steps."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${SRTR_FFS_EMAIL_SENDER}" "${SRTR_FFS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


SUBJECT="SRTR FFS Part B Completed" 
MSG="The SRTR FFS Part B extracts from Snowflake have completed."
${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${SRTR_FFS_EMAIL_SENDER}" "${SRTR_FFS_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

###########################################################################################
# End script
###########################################################################################
echo "" >> ${LOGNAME}
echo "SRTR_FFS_PTB_Driver.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

exit $RET_STATUS
