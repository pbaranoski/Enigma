#!/usr/bin/bash
####
#### NOTE: THIS SCRIPT IS OBSOLETE.
###
############################################################################################################
# Name:  MNUP_Extract_Driver.sh
#
# Desc: MNUP Extract Driver script: Executes LOAD_MNUP_FNDR_FILE.sh and MNUP_MED_NONUTIL_ext.sh.
#
# Author     : Paul Baranoski	
# Created    : 11/28/2022
#
# Paul Baranoski 2023-08-03 Update script from bourne shell (sh) to bash. 
# Paul Baranoski 2024-01-10 Add $ENVNAME to SUBJECT line of Emails.         
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
LOGNAME=/app/IDRC/XTR/CMS/logs/MNUP_Extract_Driver_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "MNUP_Extract_Driver.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

# Ensure that child scripts will get same timestamp
export TMSTMP

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh

S3BUCKET=${MNUP_BUCKET} 

echo "MNUP bucket=${S3BUCKET}" >> ${LOGNAME}


#############################################################
# Execute Script to load Finder File table into S3
#############################################################
echo "" >> ${LOGNAME}
echo "Execute script LOAD_MNUP_FNDR_FILE.sh"  >> ${LOGNAME}
${RUNDIR}LOAD_MNUP_FNDR_FILE.sh   2>> ${LOGNAME}


#############################################################
# Check the status of script 
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "LOAD_MNUP_FNDR_FILE.sh failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="LOAD_MNUP_FNDR_FILE.sh  - Failed (${ENVNAME})"
		MSG="LOAD_MNUP_FNDR_FILE.sh has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${MNUP_EMAIL_SENDER}" "${MNUP_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "LOAD_MNUP_FNDR_FILE.sh completed successfully. " >> ${LOGNAME}


#############################################################
# Execute Script to Extract MNUP data into S3
#############################################################
echo "" >> ${LOGNAME}
echo "Execute script MNUP_MED_NONUTIL_ext.sh"  >> ${LOGNAME}
${RUNDIR}MNUP_MED_NONUTIL_ext.sh   2>> ${LOGNAME}


#############################################################
# Check the status of script 
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "MNUP_MED_NONUTIL_ext.sh failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="MNUP_Extract_Driver.sh  - Failed (${ENVNAME})"
		MSG="MNUP_MED_NONUTIL_ext.sh failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${MNUP_EMAIL_SENDER}" "${MNUP_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "MNUP_MED_NONUTIL_ext.sh completed successfully. " >> ${LOGNAME}
	

#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "MNUP_Extract_Driver.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS
