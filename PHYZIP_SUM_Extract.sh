#!/usr/bin/bash
#
######################################################################################
# Name:  PHYZIP_SUM_Extracts.sh
#       
# Execute as ./PHYZIP_SUM_Extracts.sh $1
#
# $1 = Override Run date - execute extract as if it was run on this date.
#    
# DESC:   This script extracts PHYZIP data to replace legacy Mainframe data extract.
#
# Created: Paul Baranoski 4/04/2025
# Modified: 
#
# Paul Baranoski 2025-04-04 Created program.
# Paul Baranoski 2025-04-11 Changed constant name PHYZIP_BOX_RECIPIENT to PHYZIP_BOX_RECIPIENTS.
######################################################################################
set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`


LOGNAME=/app/IDRC/XTR/CMS/logs/PHYZIP_SUM_Extracts_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "PHYZIP_SUM_Extracts.sh started at `date` " >> ${LOGNAME}
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

S3BUCKET=${PHYZIP_BUCKET} 


#############################################################
# Determine 1st day of current Month
#############################################################
echo "" >> ${LOGNAME}
echo "Determine date parameters for extract query" >> ${LOGNAME}

# Ex. 2025-07-01
if [ -n "${ParmOverrideDate}" ];then
	FIRST_DAY_CUR_MONTH=`date -d "${ParmOverrideDate}" +%Y-%m-01`  2>> ${LOGNAME}
else
	FIRST_DAY_CUR_MONTH=`date +%Y-%m-01`
fi

#RUN_PRD=`date -d "${EXT_FROM_DT} " +%Y%b`

echo "FIRST_DAY_CUR_MONTH=${FIRST_DAY_CUR_MONTH}" >> ${LOGNAME} 


#############################################################
# Get Current and Prior Year
#############################################################
CUR_YYYY=`echo ${FIRST_DAY_CUR_MONTH} | cut -c1-4 ` 2>> ${LOGNAME}
PRIOR_YYYY=$(( ${CUR_YYYY} - 1 ))  2>> ${LOGNAME}

echo "CUR_YYYY=${CUR_YYYY}" >> ${LOGNAME}
echo "PRIOR_YYYY=${PRIOR_YYYY}" >> ${LOGNAME}

#############################################################
# Build arrays of Extracts to execute
#############################################################
EXT_RUNS=()
EXT_RUNS+=("${PRIOR_YYYY}Q1,${PRIOR_YYYY}-01-01,${PRIOR_YYYY}-03-31")
EXT_RUNS+=("${PRIOR_YYYY}Q2,${PRIOR_YYYY}-04-01,${PRIOR_YYYY}-06-30")
EXT_RUNS+=("${PRIOR_YYYY}Q3,${PRIOR_YYYY}-07-01,${PRIOR_YYYY}-09-30")
EXT_RUNS+=("${PRIOR_YYYY}Q4,${PRIOR_YYYY}-10-01,${PRIOR_YYYY}-12-31")
EXT_RUNS+=("${CUR_YYYY}Q1,${CUR_YYYY}-01-01,${CUR_YYYY}-03-31")
EXT_RUNS+=("${CUR_YYYY}Q2,${CUR_YYYY}-04-01,${CUR_YYYY}-06-30")

echo "EXT_RUNS=${EXT_RUNS[*]}"   >> ${LOGNAME}

#PHYZIP_SUM24Q1_{TMSTMP}.txt.gz=P#EFT.ON.PHYZIP.SUM24Q1.D250715.T1200221

#PHYZIP_SUM_{FILE_LIT}_{TIMESTAMP}.txt.gz=P#EFT.ON.PHYZIP.SUM24Q1.{TIMESTAMP}
#P#EFT.ON.PHYZIP.SUM24Q1.{TIMESTAMP}=P#XTR.XTR.SUM24Q1.PHYZIP.{TIMESTAMP}
#P#IDR.XTR.SUM24Q1.PHYZIP.D250715.T1200221


#################################################################################
# Loop thru NOF DAYS reporting periods  
#################################################################################
for (( idx=0 ; idx < ${#EXT_RUNS[@]}; idx++ ))
do

	echo "" >> ${LOGNAME}
	echo "*-----------------------------------*" >> ${LOGNAME}
	
	#############################################################
	EXT_INFO=${EXT_RUNS[idx]}	
	echo "EXT_INFO=${EXT_INFO}" >> ${LOGNAME}
	
	# FILE_LIT=24Q1
	FILE_LIT=`echo ${EXT_INFO} | cut -d, -f1 | cut -c3-`   2>> ${LOGNAME}
	EXT_FROM_DT=`echo ${EXT_INFO} | cut -d, -f2 `   2>> ${LOGNAME}
	EXT_TO_DT=`echo ${EXT_INFO} | cut -d, -f3 `   2>> ${LOGNAME}
	
	echo "FILE_LIT=${FILE_LIT}" >> ${LOGNAME}
	echo "EXT_FROM_DT=${EXT_FROM_DT}"  >> ${LOGNAME}
	echo "EXT_TO_DT=${EXT_TO_DT}"  >> ${LOGNAME}

	#############################################################
	# Make variables available for substitution in Python code
	#############################################################
	export TMSTMP
	export FILE_LIT
	export EXT_FROM_DT
	export EXT_TO_DT

	#############################################################
	# Execute Python code to Extract claims data.
	#############################################################
	echo "" >> ${LOGNAME}
	echo "Run PHYZIP_SUM_Extracts.py program" >> ${LOGNAME}

	${PYTHON_COMMAND} ${RUNDIR}PHYZIP_SUM_Extract.py  >> ${LOGNAME} 2>&1


	#############################################################
	# Check the status of python script
	#############################################################
	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Python script PHYZIP_SUM_Extract.py failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="PHYZIP SUM Extract - Failed (${ENVNAME})"
		MSG="Python script PHYZIP_SUM_Extract.py failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi

	echo "" >> ${LOGNAME}
	echo "Python script PHYZIP_SUM_Extracts.py completed successfully." >> ${LOGNAME}

	
done


#############################################################
# Get list of S3 files and record counts for success email.
#############################################################
echo "" >> ${LOGNAME}
echo "Get S3 Extract file list and record counts" >> ${LOGNAME}

getExtractFilenamesAndCounts ${LOGNAME}  >> ${LOGNAME} 2>&1
S3Files="${filenamesAndCounts}" 


#############################################################
# Send success email 
#############################################################
echo "" >> ${LOGNAME}
echo "Send success email." >> ${LOGNAME}

# Send Success email	
SUBJECT="PHYZIP SUM Extract - completed ($ENVNAME)"
MSG="PHYZIP SUM Extract completed. \n\nThe following extract files were created:\n\n${S3Files}"
${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${PHYZIP_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}"  >> ${LOGNAME} 2>&1


if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Error in calling sendEmail.py" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Sending Success email in PHYZIP_SUM_Extracts.sh  - Failed (${ENVNAME})"
	MSG="Sending Success email in PHYZIP_SUM_Extracts.sh has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	


#############################################################
# EFT Extract files
#############################################################
echo " " >> ${LOGNAME}
echo "EFT PHYZIP Sum Extract Files " >> ${LOGNAME}
${RUNDIR}ProcessFiles2EFT.sh ${S3BUCKET}  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of extract script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script ProcessFiles2EFT.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="PHYZIP SUM Extract EFT process  - Failed (${ENVNAME})"
	MSG="PHYZIP SUM Extract EFT process has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi

#############################################################
# Create Manifest file
#############################################################
#echo "" >> ${LOGNAME}
#echo "Create Manifest file for PHYZIP SUM Extract.  " >> ${LOGNAME}

#${RUNDIR}CreateManifestFile.sh ${S3BUCKET} ${TMSTMP} ${PHYZIP_BOX_RECIPIENTS} 


#############################################################
# Check the status of script
#############################################################
#RET_STATUS=$?

#if [[ $RET_STATUS != 0 ]]; then
#	echo "" >> ${LOGNAME}
#	echo "Shell script CreateManifestFile.sh failed." >> ${LOGNAME}
#	
#	# Send Failure email	
#	SUBJECT="Create Manifest file in PHYZIP_SUM_Extracts.sh - Failed (${ENVNAME})"
#	MSG="Create Manifest file in PHYZIP_SUM_Extracts.sh  has failed."
#	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
#
#	exit 12
#fi	


#############################################################
# clean-up linux data directory
#############################################################
echo "" >> ${LOGNAME} 
echo "Remove temp files from data directory" >> ${LOGNAME} 

 
#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "PHYZIP_SUM_Extracts.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS
