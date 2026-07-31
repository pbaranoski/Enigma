#!/usr/bin/bash
############################################################################################################
# Name:  DoD_NPI_extract.sh
#
# Desc: DoD_NPI Extract - Load DoD finder file into IDR. Extract data from IDR that matches SSNs in FF.
#
# Author     : Paul Baranoski	
# Created    : 09/13/2023
#
# Modified:
#
# Paul Baranoski   2025-09-11  Create script.
# Paul Baranoski   2026-06-25  ${TESTLOG}Modify to Add "TESTING" functionality.
############################################################################################################

set +x

#############################################################
# Include module that includes all constants 
#############################################################
TESTING="N"
export TESTING

source /app/IDRC/XTR/CMS/scripts/run/SET_XTR_ENV.sh 

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/${TESTLOG}DoD_NPI_extract_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "DoD_NPI_extract.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}FilenameCounts.bash

S3BUCKET=${DOD_NPI_BUCKET} 

echo "DoD_NPI bucket=${S3BUCKET}" >> ${LOGNAME}
echo "Finder files bucket=${FINDER_FILE_BUCKET}" >> ${LOGNAME}


#############################################################
# Execute Script to load DoD_NPI Finder File table in SF
#############################################################
echo "" >> ${LOGNAME}
echo "Execute script LOAD_DoD_NPI_FNDR_FILE.sh"  >> ${LOGNAME}
${RUNDIR}LOAD_DoD_NPI_FNDR_FILE.sh   2>> ${LOGNAME}


#############################################################
# Check the status of script 
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then

	if [[ $RET_STATUS -eq 4 ]]; then
		echo "" >> ${LOGNAME}
		echo "LOAD_DoD_NPI_FNDR_FILE.sh ended. No Finder Files found." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="LOAD_DoD_NPI_FNDR_FILE.sh ended. No Finder Files found. (${ENVNAME}${TESTEMAIL})"
		MSG="LOAD_DoD_NPI_FNDR_FILE.sh ended. No Finder Files found."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 4	
	else
		echo "" >> ${LOGNAME}
		echo "LOAD_DoD_NPI_FNDR_FILE.sh failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="LOAD_DoD_NPI_FNDR_FILE.sh  - Failed (${ENVNAME}${TESTEMAIL})"
		MSG="LOAD_DoD_NPI_FNDR_FILE.sh has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi	
fi

echo "" >> ${LOGNAME}
echo "LOAD_DoD_NPI_FNDR_FILE.sh completed successfully. " >> ${LOGNAME}


#############################################################
# Execute Python code to extract DoD_NPI data.
#############################################################
echo "" >> ${LOGNAME}
echo "Start execution of DoD_NPI_extract.py program"  >> ${LOGNAME}

# Export environment variables for Python code
export TMSTMP

${PYTHON_COMMAND} ${RUNDIR}DoD_NPI_extract.py >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script  
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Python script DoD_NPI_extract.py failed" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="DoD_NPI_extract.sh  - Failed (${ENVNAME}${TESTEMAIL})"
	MSG="DoD_NPI Extract has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script DoD_NPI_extract.py completed successfully. " >> ${LOGNAME}


#############################################################
# Get list of S3 files and record counts for success email.
#############################################################
echo "" >> ${LOGNAME}
echo "Get S3 Extract file list and record counts" >> ${LOGNAME}

getExtractFilenamesAndCounts ${LOGNAME}  >> ${LOGNAME} 2>&1
S3Files="${filenamesAndCounts}" 


#############################################################
# Send Success email.
#############################################################
echo "" >> ${LOGNAME}
echo "Send success email with S3 Extract filename." >> ${LOGNAME}
echo "S3Files=${S3Files} "   >> ${LOGNAME}

SUBJECT="Weekly DoD_NPI extract (${ENVNAME}${TESTEMAIL})" 
MSG="The Extract for the creation of the DoD NPI file has completed.\n\nThe following file(s) were created:\n\n${S3Files}"

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${DOD_NPI_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Error in calling sendEmail.py" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Sending Success email in DoD_NPI_extract.sh  - Failed (${ENVNAME}${TESTEMAIL})"
		MSG="Sending Success email in DoD_NPI_extract.sh  has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
fi	


#############################################################
# EFT Extract files
#############################################################
echo " " >> ${LOGNAME}
echo "EFT DoD_NPI Extract file " >> ${LOGNAME}
${RUNDIR}ProcessFiles2EFT.sh ${S3BUCKET}  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of extract script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script ProcessFiles2EFT.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT=" DoD_NPI EFT process  - Failed (${ENVNAME}${TESTEMAIL})"
	MSG=" DoD_NPI EFT process has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME} 


#############################################################
# end script
#############################################################
echo "" >> ${LOGNAME}
echo "DoD_NPI_extract.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS