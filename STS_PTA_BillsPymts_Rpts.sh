#!/usr/bin/bash
#
######################################################################################
# Name:  STS_PTA_BillsPymts_Rpts.sh
#        
# DESC:   This script extracts data for STS PTA Bills Payments by Type of Service report 
#         (legacy A-1 report)
#
# Created: Paul Baranoski  12/31/2024
# Modified: 
#
# Paul Baranoski 2024-12-31 Create script.
######################################################################################
set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`


LOGNAME=/app/IDRC/XTR/CMS/logs/STS_PTA_BillsPymts_Rpts_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "STS_PTA_BillsPymts_Rpts.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}


##################################################################
# Extract can run stand-alone or as a called script. 
##################################################################
if ! [[ $# -eq 0 || $# -eq 1  ]]
then
	echo "Incorrect # of parameters sent to script. NOF parameters: $#" >> ${LOGNAME}
	echo "parm1=$1" >> ${LOGNAME}
	exit 12
fi


#############################################################
# Display parameters passed to script 
#############################################################
ParmOverrideDate=$1

echo " " >> ${LOGNAME}
echo "Parameters to script: " >> ${LOGNAME}
echo "NOF parameters for script: " $# >> ${LOGNAME}
echo "   ParmOverrideDate=${ParmOverrideDate} " >> ${LOGNAME}


#############################################################
# Include modules 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh >> ${LOGNAME}

source ${RUNDIR}FilenameCounts.bash

S3BUCKET=${STS_PTA_BPYMTS_BUCKET} 


#############################################################
# Determine date parameters for extract query in python code
#############################################################
echo "" >> ${LOGNAME}
echo "Determine date parameters for extract query" >> ${LOGNAME}

if [ -n "${ParmOverrideDate}" ];then
	CUR_MM=`date -d "${ParmOverrideDate}" +%m`
	CUR_YYYY=`date -d "${ParmOverrideDate}" +%Y`
	PRIOR_YYYY=`expr ${CUR_YYYY} - 1`
else
	CUR_MM=`date +%m`
	CUR_YYYY=`date +%Y`
	PRIOR_YYYY=`expr ${CUR_YYYY} - 1`
fi

echo "CUR_MM=${CUR_MM}" >> ${LOGNAME} 
echo "CUR_YYYY=${CUR_YYYY}" >> ${LOGNAME} 
echo "PRIOR_YYYY=${PRIOR_YYYY}" >> ${LOGNAME} 


# script should run second Friday of JAN, JUL
if [ "${CUR_MM}" = "07" ];then
	EXT_TO_DATE=${CUR_YYYY}-06-30
	RUN_PRD=JUN
	
elif [ "${CUR_MM}" = "01" ];then
	EXT_TO_DATE=${PRIOR_YYYY}-12-31
	RUN_PRD=DEC
else
	echo "" >> ${LOGNAME}
	echo "Not a valid processing month ${CUR_MM} " >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="STS PTA Bills Pymts Report - Failed (${ENVNAME})"
	MSG="Not a valid processing month: ${CUR_MM}"
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


# Create YYYY range of 9 years
EXT_TO_YYYY=`date -d "${EXT_TO_DATE} " +%Y `  1>> ${LOGNAME} 2>&1
EXT_FROM_YYYY=$((EXT_TO_YYYY-8))  2>> ${LOGNAME}

echo "EXT_TO_YYYY=${EXT_TO_YYYY}" >> ${LOGNAME} 
echo "EXT_FROM_YYYY=${EXT_FROM_YYYY}" >> ${LOGNAME}


#############################################################
# Make variables available for substitution in Python code
#############################################################
export TMSTMP
export RUN_PRD
export EXT_FROM_YYYY
export EXT_TO_DATE


#############################################################
# Execute Python code to Extract claims data.
#############################################################
echo "" >> ${LOGNAME}
echo "Run STS_PTA_BillsPymts_Rpts.py program" >> ${LOGNAME}

${PYTHON_COMMAND} ${RUNDIR}STS_PTA_BillsPymts_Rpts.py  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Python script STS_PTA_BillsPymts_Rpts.py failed" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="STS PTA Bills Pymts Report - Failed (${ENVNAME})"
	MSG="Python script STS_PTA_BillsPymts_Rpts.py failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script STS_PTA_BillsPymts_Rpts.py completed successfully." >> ${LOGNAME}


#############################################################
# Get list of S3 files and record counts for success email.
#############################################################
echo "" >> ${LOGNAME}
echo "Get S3 Extract file list and record counts" >> ${LOGNAME}

getExtractFilenamesAndCounts ${LOGNAME}  >> ${LOGNAME} 2>&1
S3Files="${filenamesAndCounts}" 


#############################################################
# Send success email of STS MED INS Tbl Rpt files
#############################################################
echo "" >> ${LOGNAME}
echo "Send success email." >> ${LOGNAME}

# Send Success email	
SUBJECT="STS PTA Bills Pymts Report - completed ($ENVNAME)"
MSG="STS PTA Bills Pymts Report completed. \n\nThe following extract files were created:\n\n${S3Files}"
${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${STS_PTA_BPYMTS_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}"  >> ${LOGNAME} 2>&1


if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Error in calling sendEmail.py" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Sending Success email in STS_PTA_BillsPymts_Rpts.sh  - Failed (${ENVNAME})"
	MSG="Sending Success email in STS_PTA_BillsPymts_Rpts.sh has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	


#############################################################
# Create Manifest file
#############################################################
echo "" >> ${LOGNAME}
echo "Create Manifest file for STS PTA Bills Pymts Report.  " >> ${LOGNAME}

${RUNDIR}CreateManifestFile.sh ${S3BUCKET} ${TMSTMP} ${STS_PTA_BPYMTS_BOX_RECIPIENT} 


#############################################################
# Check the status of script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script CreateManifestFile.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Create Manifest file in STS_PTA_BillsPymts_Rpts.sh - Failed (${ENVNAME})"
	MSG="Create Manifest file in STS_PTA_BillsPymts_Rpts.sh  has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	


#############################################################
# clean-up linux data directory
#############################################################
echo "" >> ${LOGNAME} 
echo "Remove temp files from data directory" >> ${LOGNAME} 


 
#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "STS_PTA_BillsPymts_Rpts.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS
