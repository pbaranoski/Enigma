#!/usr/bin/bash
######################################################################################
# Name:  PECOS_Extract.sh
#
# Desc: Extract DEA PECOS (Provider Enrollment, Chain, and Ownership System) data. 
#
# Created: Sumathi Gayam  06/14/2022  
# Modified:   
#
# Paul Baranoski 2022-11-09 Added call to CreateManifestFile.sh
# Paul Baranoski 2022-11-09 Added code to send Success emails with filenames from script
#                           instead of python code. 
# Paul Baranoski 2023-07-24 Modify logic in getting extract filenames that include record counts.  
# Paul Baranoski 2023-07-24 Comment out Box functionality (we may use it in the future),
#                           and add EFT functionality.    
# Paul Baranoski 2024-02-02 Add ENVNAME to email SUBJECT lines.  
#                           Add EFT filename mask  
# Paul Baranoski   2025-02-04  Modify Email constants to use CMS_EMAIL_SENDER and ENIGMA_EMAIL_FAILURE_RECIPIENT.
######################################################################################
set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
LOGNAME=/app/IDRC/XTR/CMS/logs/DEA_PECOS_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/

touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "DEA_PECOS.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh 

source ${RUNDIR}FilenameCounts.bash

S3BUCKET=${PECOS_BUCKET} 
echo "PECOS bucket=${S3BUCKET}" >> ${LOGNAME}

EFT_FILEMASK=P#EFT.ON.DEAPECOS.DYYMMDD.THHMMSS

	
#############################################################
# Make variables available for substitution in Python code
#############################################################
export TMSTMP


#############################################################
# Execute Python code
#############################################################
echo "" >> ${LOGNAME}
echo "Start execution of DEA_PECOS.py program" >> ${LOGNAME}

${PYTHON_COMMAND} ${RUNDIR}DEA_PECOS.py >> ${LOGNAME} 2>&1

#############################################################
# Check the status of python script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script DEA_PECOS.py failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="DEA PECOS Extract - Failed (${ENVNAME}) "
		MSG="DEA PECOS extract has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
		
        exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script DEA_PECOS.py completed successfully. " >> ${LOGNAME}


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

SUBJECT="Monthly PECOS extract (${ENVNAME})" 
MSG="The Extract for the creation of the monthly DEA PECOS file from Snowflake has completed.\n\nEFT version of the below file was created using the following file mask ${EFT_FILEMASK}.\n\nThe following file(s) were created:\n\n${S3Files}"

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${PECOS_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Error in calling sendEmail.py" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Sending Success email in DEA_PECOS.sh - Failed (${ENVNAME})"
		MSG="Sending Success email in DEA_PECOS.sh  has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
fi	


##\/ Keep BOX/manifest file logic for the future
#############################################################
# Create Manifest file
#############################################################
#echo "" >> ${LOGNAME}
#echo "Create Manifest file for PECOS Extract.  " >> ${LOGNAME}

#${RUNDIR}CreateManifestFile.sh ${S3BUCKET} ${TMSTMP} ${PECOS_EMAIL_SUCCESS_RECIPIENT} 


#############################################################
# Check the status of script
#############################################################
#RET_STATUS=$?
#
#if [[ $RET_STATUS != 0 ]]; then
#		echo "" >> ${LOGNAME}
#		echo "Shell script CreateManifestFile.sh failed." >> ${LOGNAME}
#		
#		# Send Failure email	
#		SUBJECT="Create Manifest file in DEA_PECOS.sh  - Failed"
#		MSG="Create Manifest file in DEA_PECOS.sh  has failed."
#		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
#
#		exit 12
#fi	
##/\ Keep manifest logic for the future


#############################################################
# EFT Extract files
#############################################################
echo " " >> ${LOGNAME}
echo "EFT PECOS Extract File " >> ${LOGNAME}
${RUNDIR}ProcessFiles2EFT.sh ${S3BUCKET}  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of extract script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script ProcessFiles2EFT.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="PECOS Extract EFT process - Failed (${ENVNAME})"
	MSG=" PECOS Extract EFT process has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "DEA_PECOS.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS
