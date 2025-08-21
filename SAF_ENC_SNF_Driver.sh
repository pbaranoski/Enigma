#!/usr/bin/bash
############################################################################################################
# Name:  SAF_ENC_SNF_Driver.sh
#
# Desc: SAF Encounter Inpatient Driver script 
#
# Execute as ./SAF_ENC_SNF_Driver.sh 
#
# NOTE: Script must be /usr/bin/bash instead of /usr/bin/sh because the included FilenameCounts.bash file requires some bash specific syntax for 
#       a paste command which uses "bash Process Substitution". The paste command expects files and not variables as input. However, 
#       "bash Process Substitution" makes variables appear to the command as files.  The purpose of doing it this way instead of using temp files 
#       is because we have multiple scripts using the same data directory that would use the same temp file, possibly interferring with each other. 
#
# Author     : Paul Baranoski	
# Created    : 06/02/2023
#
# Modified:
#
# Paul Baranoski 2023-06-02 Created script.
# Paul Baranoski 2024-02-28 Add ENVNAME to SUBJECT line for emails. 
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
LOGNAME=/app/IDRC/XTR/CMS/logs/SAF_ENC_SNF_Driver_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

export TMSTMP

touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "SAF_ENC_SNF_Driver.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh


#############################################################
# Execute script for SNF
#############################################################
echo "" >> ${LOGNAME}
echo "Execute script SAF_ENC_INP_SNF_Extract.sh"  >> ${LOGNAME}
${RUNDIR}SAF_ENC_INP_SNF_Extract.sh SNF  2>> ${LOGNAME}


#############################################################
# Check the status of script 
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "SAF_ENC_INP_SNF_Extract.sh failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="SAF_ENC_INP_SNF_Extract.sh - Failed (${ENVNAME})"
		MSG="SAF_ENC_INP_SNF_Extract.sh failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${SAFENC_EMAIL_SENDER}" "${SAFENC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "SAF_ENC_INP_SNF_Extract.sh completed successfully. " >> ${LOGNAME}


#############################################################
# End script
#############################################################
echo "" >> ${LOGNAME}
echo "SAF_ENC_SNF_Driver.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

exit $RET_STATUS