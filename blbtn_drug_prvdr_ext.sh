#!/usr/bin/bash
######################################################################################
# Name:  blbtn_drug_prvdr_ext.sh
#
# Desc: Extract Blue Button drug/provider data (IDR#BLB3/IDR#BLB4). 
#
# Created: Paul Baranoski  06/09/2022
# Modified:
#
# Paul Baranoski 2022-11-02 Added call to CreateManifestFile.sh
# Paul Baranoski 2022-11-03 Added code to send Success emails with filenames from script
#                           instead of python code.
# Paul Baranoski 2023-07-14 Modify logic in getting extract filenames that include record counts. 
# Paul Baranoski 2023-07-19 Comment out Box functionality (we may use it in the future),
#                           and add EFT functionality.
# Paul Baranoski 2023-12-11 Add $ENVNAME to SUBJECT line of all emails.
######################################################################################
set +x


#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
LOGNAME=/app/IDRC/XTR/CMS/logs/blbtn_drug_prvdr_ext_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/

touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "blbtn_drug_prvdr_ext.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh >> ${LOGNAME}

source ${RUNDIR}FilenameCounts.bash

S3BUCKET=${BLBTN_BUCKET} 

#############################################################
# Build date parameters for blbtn_drug_prvdr_ext script
#############################################################
echo "" >> ${LOGNAME}
echo "No Parms for script blbtn_drug_prvdr_ext.sh --> " >> ${LOGNAME}

#############################################################
# Make variables available for substitution in Python code
#############################################################
export TMSTMP

#############################################################
# Execute Python code - Drug Extract
#############################################################
echo "" >> ${LOGNAME}
echo "Start execution of blbtn_drug_ext.py program" >> ${LOGNAME}

${PYTHON_COMMAND} ${RUNDIR}blbtn_drug_ext.py  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script blbtn_drug_ext.py failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Weekly Blue Button Drug Extract - Failed (${ENVNAME})"
		MSG="The weekly Blue Button Drug extract has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${BLBTN_EMAIL_SENDER}" "${BLBTN_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
		
        exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script blbtn_drug_ext.py completed successfully." >> ${LOGNAME} 

#############################################################
# Execute Python code - Provider Extract
#############################################################
echo "" >> ${LOGNAME}
echo "start execution of blbtn_prvdr_ext.py program" >> ${LOGNAME}

${PYTHON_COMMAND} ${RUNDIR}blbtn_prvdr_ext.py  >> ${LOGNAME}  2>&1


#############################################################
# Check the status of python script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script blbtn_prvdr_ext.py failed" >> ${LOGNAME}

		# Send Failure email	
		SUBJECT="Weekly Blue Button Provider Extract - Failed (${ENVNAME})"
		MSG="The weekly Blue Button Provider extract has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${BLBTN_EMAIL_SENDER}" "${BLBTN_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
		
        exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script blbtn_prvdr_ext.py completed successfully." >> ${LOGNAME}


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

SUBJECT="Weekly Blue Button drug/provider extract (${ENVNAME})" 
MSG="The Weekly Blue Button drug/provider extract has completed.\n\nThe following file(s) were created:\n\n${S3Files}"

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${BLBTN_EMAIL_SENDER}" "${BLBTN_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Error in calling sendEmail.py" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Sending Success email in blbtn_drug_prvdr_ext.sh - Failed"
		MSG="Sending Success email in blbtn_drug_prvdr_ext.sh has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${BLBTN_EMAIL_SENDER}" "${BLBTN_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
fi	


##\/ Keep BOX/manifest file logic for the future
#############################################################
# Create Manifest file
#############################################################
#echo "" >> ${LOGNAME}
#echo "Create Manifest file for Blbtn drug/provider Extract.  " >> ${LOGNAME}
#
#${RUNDIR}CreateManifestFile.sh ${S3BUCKET} ${TMSTMP} ${BLBTN_EMAIL_SUCCESS_RECIPIENT} 


#############################################################
# Check the status of script
#############################################################
#RET_STATUS=$?

#if [[ $RET_STATUS != 0 ]]; then
#		echo "" >> ${LOGNAME}
#		echo "Shell script CreateManifestFile.sh failed." >> ${LOGNAME}
#		
#		# Send Failure email	
#		SUBJECT="Create Manifest file in blbtn_drug_prvdr_ext.sh - Failed"
#		MSG="Create Manifest file in blbtn_drug_prvdr_ext.sh has failed."
#		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${BLBTN_EMAIL_SENDER}" "${BLBTN_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
#
#		exit 12
#fi	


#############################################################
# EFT Extract files
#############################################################
echo " " >> ${LOGNAME}
echo "EFT Blue Button Drug/Provider Extract File " >> ${LOGNAME}
${RUNDIR}ProcessFiles2EFT.sh ${S3BUCKET}  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of extract script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script ProcessFiles2EFT.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT=" Blue Button Drug/Provider Extract EFT process  - Failed (${ENVNAME})"
	MSG=" Blue Button Drug/Provider Extract EFT process has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${BLBTN_EMAIL_SENDER}" "${BLBTN_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "blbtn_drug_prvdr_ext.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS
