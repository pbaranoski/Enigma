#!/usr/bin/bash
#
######################################################################################
# Name:  STS_HHA_AA7.sh
#        
# DESC:   This script extracts data for STS HHA by facility type  and state
#         (legacy A-7 report)  
#
# Created: copied from Paul Baranoski  shell script
# Modified: 
#
# Nat.Tinovsky	2025-02-03 Create script.
######################################################################################
set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`


LOGNAME=/app/IDRC/XTR/CMS/logs/STS_HHA_AA7_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/
RPTNAME="STS HHA AA7 "
SHELL_SCRIPT=`basename ${BASH_SOURCE}`
PYTHON_SCRIPT="STS_HHA_AA7.py"

touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "${SHELL_SCRIPT} started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

##################################################################
# Extract can run stand-alone or as a called script. 
##################################################################
if ! [[ $# -eq 0 || $# -eq 1  ]]
then
	echo "Incorrect # of parameters sent to script. NOF parameters: $#" >> ${LOGNAME}
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

S3BUCKET=${STS_HHA_BUCKET} 
BOX_RECIPIENT=${STS_HHA_BOX_RECIPIENT}
SUCCESS_RECIPIENT=${STS_HHA_BOX_RECIPIENT}

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
# Create YYYY range of 3 years
EXT_TO_YYYY=`date -d "${EXT_TO_DATE} " +%Y `  1>> ${LOGNAME} 2>&1
EXT_FROM_YYYY=$((EXT_TO_YYYY-3))  2>> ${LOGNAME}



# script should run second Friday of JAN, JUL
if [ "${CUR_MM}" = "07" ];then
	EXT_TO_DATE=${CUR_YYYY}-06-30
	RUN_PRD=JUN
	EXT_FROM_DATE=${EXT_FROM_YYYY}-07-01
	
elif [ "${CUR_MM}" = "01" ];then
	EXT_TO_DATE=${PRIOR_YYYY}-12-31
	RUN_PRD=DEC
	EXT_FROM_DATE=${EXT_FROM_YYYY}-01-01
else
	echo "" >> ${LOGNAME}
	echo "Not a valid processing month ${CUR_MM} " >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="${RPTNAME} process- Failed (${ENVNAME})"
	MSG="Not a valid processing month: ${CUR_MM}"
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi

echo "CUR_MM=${CUR_MM}" >> ${LOGNAME} 
echo "CUR_YYYY=${CUR_YYYY}" >> ${LOGNAME} 
echo "PRIOR_YYYY=${PRIOR_YYYY}" >> ${LOGNAME} 
echo "EXT_TO_YYYY=${EXT_TO_YYYY}" >> ${LOGNAME} 
echo "EXT_FROM_YYYY=${EXT_FROM_YYYY}" >> ${LOGNAME}
echo "EXT_FROM_DATE=${EXT_FROM_DATE}" >> ${LOGNAME}


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
echo "Run ${PYTHON_SCRIPT} program" >> ${LOGNAME}
${PYTHON_COMMAND} ${RUNDIR}${PYTHON_SCRIPT}  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Python script ${PYTHON_SCRIPT} failed" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="${RPTNAME} Report - Failed (${ENVNAME})"
	MSG="Python script ${PYTHON_SCRIPT} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script ${PYTHON_SCRIPT} completed successfully." >> ${LOGNAME}


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
SUBJECT="${RPTNAME} Report - completed ($ENVNAME)"
MSG="${RPTNAME} Report completed. \n\nThe following extract files were created:\n\n${S3Files}"
${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}"  >> ${LOGNAME} 2>&1


if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Error in calling sendEmail.py" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Sending Success email in ${SHELL_SCRIPT}  - Failed (${ENVNAME})"
	MSG="Sending Success email in ${SHELL_SCRIPT} has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	


#############################################################
# Create Manifest file
#############################################################
echo "" >> ${LOGNAME}
echo "Create Manifest file for ${RPTNAME} Report.  " >> ${LOGNAME}

${RUNDIR}CreateManifestFile.sh ${S3BUCKET} ${TMSTMP} ${BOX_RECIPIENT} 


#############################################################
# Check the status of script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script CreateManifestFile.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Create Manifest file in ${SHELL_SCRIPT} - Failed (${ENVNAME})"
	MSG="Create Manifest file in ${SHELL_SCRIPT}  has failed."
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
echo "${SHELL_SCRIPT} completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS
