#!/usr/bin/bash
############################################################################################################
# Name:  SRTR_ENC_PTB_Driver.sh
#
# Desc: SRTR Encounter Carrier Extract 
#
# Execute as ./SRTR_ENC_PTB_Driver.sh 
#
# NOTE: Script must be /usr/bin/bash instead of /usr/bin/sh because the included FilenameCounts.bash file requires some bash specific syntax for 
#       a paste command which uses "bash Process Substitution". The paste command expects files and not variables as input. However, 
#       "bash Process Substitution" makes variables appear to the command as files.  The purpose of doing it this way instead of using temp files 
#       is because we have multiple scripts using the same data directory that would use the same temp file, possibly interferring with each other. 
#
# Author     : Paul Baranoski	
# Created    : 02/17/2023
#
# Modified:
#
# Paul Baranoski 2023-02-17 Created script.
# Paul Baranoski 2023-06-02 export TMSTMP value to be used in child scripts.
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/SRTR_ENC_PTB_Driver_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

export TMSTMP


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "SRTR_ENC_PTB_Driver.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh

S3BUCKET=${SRTR_ENC_BUCKET} 
echo "SRTR ENC PTA_PTB bucket=${S3BUCKET}" >> ${LOGNAME}


#############################################################
# Execute CAR script
#############################################################
echo "" >> ${LOGNAME}
echo "Execute script SRTR_ENC_CAR_Extract.sh"  >> ${LOGNAME}
${RUNDIR}SRTR_ENC_CAR_Extract.sh   2>> ${LOGNAME}


#############################################################
# Check the status of script 
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "SRTR_ENC_CAR_Extract.sh failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="SRTR_ENC_CAR_Extract.sh  - Failed"
		MSG="SRTR_ENC_CAR_Extract.sh failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${SRTR_ENC_EMAIL_SENDER}" "${SRTR_ENC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "SRTR_ENC_CAR_Extract.sh completed successfully. " >> ${LOGNAME}


#############################################################
# Execute DME script
#############################################################
echo "" >> ${LOGNAME}
echo "Execute script SRTR_ENC_DME_Extract.sh"  >> ${LOGNAME}
${RUNDIR}SRTR_ENC_DME_Extract.sh   2>> ${LOGNAME}


#############################################################
# Check the status of script 
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "SRTR_ENC_DME_Extract.sh failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="SRTR_ENC_DME_Extract.sh  - Failed"
		MSG="SRTR_ENC_DME_Extract.sh failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${SRTR_ENC_EMAIL_SENDER}" "${SRTR_ENC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "SRTR_ENC_DME_Extract.sh completed successfully. " >> ${LOGNAME}


#############################################################
# End script
#############################################################
echo "" >> ${LOGNAME}
echo "SRTR_ENC_PTB_Driver.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

exit $RET_STATUS