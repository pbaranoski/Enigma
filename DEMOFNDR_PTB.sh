#!/usr/bin/bash
#
######################################################################################
# Name:  DEMOFNDR_PTB.sh
#
# Desc: Performs Extract of PartB data. 
#
# Created: Sumathi Gayam  09/01/2022
# Modified: 
#
# Paul Baranoski 2023-05-16 Remove CUR_YR, CUR_MN, and PRIOR_MN logic to place in driver script.
######################################################################################

echo "################################### "
set +x
#############################################################
# Establish log file  
#############################################################

########################################################################
# TMSTMP variable is exported from DemoFinderFileExtracts.sh script
# This will allow for creation of a single PTA log file, 
# and all extract files will have the same timestamp, making
# it easier to find them in S3.
########################################################################

LOGNAME=/app/IDRC/XTR/CMS/logs/DEMOFNDR_PTB_Extract_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "DEMOFNDR_PTB.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh >> ${LOGNAME}


############################################
# Execute DEMOFNDR Part A Extract
############################################
echo " " >> ${LOGNAME}
echo "Extract processing. " >> ${LOGNAME}
${PYTHON_COMMAND} ${RUNDIR}DEMOFNDR_PTB.py  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script DEMOFNDR_PTB.py failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Demo Finder Extract - Failed"
		MSG="Demo Finder extract has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DEMO_FINDER_EMAIL_SENDER}" "${DEMO_FINDER_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "DEMOFNDR_Extract.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS
