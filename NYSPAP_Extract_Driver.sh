#!/usr/bin/bash
############################################################################################################
# Name:  NYSPAP_Extract_Driver.sh
#
# Desc: NYSPAP Extract Driver script: Executes LOAD_NYSPAP_FNDR_FILE.sh and NYSPAP_Extract_Bene_Info.sh.
#
# Author     : Paul Baranoski	
# Created    : 10/6/2022
#
# Paul Baranoski 2023-08-22 Made script a bash instead of bourne script.   
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP:=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/NYSPAP_Extract_Driver_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "NYSPAP_Extract_Driver.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh

S3BUCKET=${NYSPAP_BUCKET} 

echo "NYSPAP bucket=${S3BUCKET}" >> ${LOGNAME}


#############################################################
# Execute Script to load Finder File table into S3
#############################################################
echo "" >> ${LOGNAME}
echo "Execute script LOAD_NYSPAP_FNDR_FILE.sh"  >> ${LOGNAME}
${RUNDIR}LOAD_NYSPAP_FNDR_FILE.sh   2>> ${LOGNAME}


#############################################################
# Check the status of script 
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "LOAD_NYSPAP_FNDR_FILE.sh failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="LOAD_NYSPAP_FNDR_FILE.sh  - Failed"
		MSG="LOAD_NYSPAP_FNDR_FILE.sh has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${NYSPAP_EMAIL_SENDER}" "${NYSPAP_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "LOAD_NYSPAP_FNDR_FILE.sh completed successfully. " >> ${LOGNAME}


#############################################################
# Execute Script to Extract By into S3
#############################################################
echo "" >> ${LOGNAME}
echo "Execute script NYSPAP_Extract_Bene_Info.sh"  >> ${LOGNAME}
${RUNDIR}NYSPAP_Extract_Bene_Info.sh   2>> ${LOGNAME}


#############################################################
# Check the status of script 
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "NYSPAP_Extract_Bene_Info.sh failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="NYSPAP_Extract_Driver.sh  - Failed"
		MSG="NYSPAP_Extract_Bene_Info.sh failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${NYSPAP_EMAIL_SENDER}" "${NYSPAP_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "NYSPAP_Extract_Bene_Info.sh completed successfully. " >> ${LOGNAME}
	

#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "NYSPAP_Extract_Driver.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS