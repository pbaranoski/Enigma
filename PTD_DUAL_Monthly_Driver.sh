#!/usr/bin/sh
############################################################################################################
# Name:  PTD_DUAL_Monthly_Driver.sh
#
# Desc: PTD DUAL Monthly Driver script: Executes PTD_Duals_Extract.sh.
#
# Author     : Paul Baranoski	
# Created    : 12/15/2022
#
# Modified:
#
# Paul Baranoski 2022-12-15 Created script.
# Paul Baranoski 2023-12-07 Add ENVNAME variable to email subject line.
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
LOGNAME=/app/IDRC/XTR/CMS/logs/PTD_DUAL_Monthly_Driver_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "PTD_DUAL_Monthly_Driver.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}


#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh


#############################################################
# Execute Script to Extract PTD Duals data into S3
#############################################################
echo "" >> ${LOGNAME}
echo "Execute script PTD_Duals_Extract.sh for Monthly processing"  >> ${LOGNAME}
${RUNDIR}PTD_Duals_Extract.sh "M"  2>> ${LOGNAME}


#############################################################
# Check the status of script 
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "PTD_Duals_Extract.sh  failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="PTD_Duals_Extract.sh  - Failed (${ENVNAME})"
		MSG="PTD_Duals_Extract.sh  failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PTDDUALMNTH_EMAIL_SENDER}" "${PTDDUALMNTH_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "PTD_Duals_Extract.sh completed successfully. " >> ${LOGNAME}
	

#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "PTD_DUAL_Monthly_Driver.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS