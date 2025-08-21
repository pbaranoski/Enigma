#!/usr/bin/bash
############################################################################################################
# Name:  NYSPAP_Extract_Bene_Info.sh
#
# Desc: NYSPAP Extract of Bene Contract Coverage Information
#
# Author     : Paul Baranoski	
# Created    : 10/5/2022
#
# Modified:
#
# Paul Baranoski 2022-11-09 Added call to CreateManifestFile.sh
# Paul Baranoski 2022-11-10 Added code to send Success emails with filenames from script
#                           instead of python code.  
# Paul Baranoski 2023-08-22 Use FilenameCounts.bash to get filenames and record counts for email. 
#                           Change Finder File location to S3 Finder_Files folder.  
# Paul Baranoski 2024-02-02 Add ENVNAME to SUBJECT line of emails.      
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP:=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/NYSPAP_Extract_Bene_Info_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "NYSPAP_Extract_Bene_Info.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh

source ${RUNDIR}FilenameCounts.bash

S3BUCKET=${NYSPAP_BUCKET} 

echo "NYSPAP bucket=${S3BUCKET}" >> ${LOGNAME}


#################################################################################
# Create Extract Date parameter
#################################################################################
echo " " >> ${LOGNAME}
echo "Create date parameter for the Python Extract program." >> ${LOGNAME}

# Create date parameter for prior month with day being set to "01" --> "2022-08-01" 
BENE_RNG_DT=`date -d "-1 month" +%Y-%m`-01  >> ${LOGNAME}  2>&1
echo "BENE_RNG_DT=${BENE_RNG_DT}" >> ${LOGNAME}


#############################################################
# Execute Python code to extract data.
#############################################################
echo "" >> ${LOGNAME}
echo "Start execution of NYSPAP_Extract_Bene_Info.py program"  >> ${LOGNAME}

# Export environment variables for Python code
export TMSTMP
export BENE_RNG_DT

${PYTHON_COMMAND} ${RUNDIR}NYSPAP_Extract_Bene_Info.py >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script  
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script NYSPAP_Extract_Bene_Info.py failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="NYSPAP_Extract_Bene_Info.sh - Failed (${ENVNAME})"
		MSG="NYSPAP Extracting Bene Info has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${NYSPAP_EMAIL_SENDER}" "${NYSPAP_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script NYSPAP_Extract_Bene_Info.py completed successfully. " >> ${LOGNAME}


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

SUBJECT="Monthly NYSPAP extract (${ENVNAME})" 
MSG="The Extract for the creation of the monthly NYSPAP file from Snowflake has completed.\n\nThe following file(s) were created:\n\n${S3Files}"

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${NYSPAP_EMAIL_SENDER}" "${NYSPAP_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Error in calling sendEmail.py" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Sending Success email in NYSPAP_Extract_Bene_Info.sh - Failed (${ENVNAME})"
		MSG="Sending Success email in NYSPAP_Extract_Bene_Info.sh  has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${NYSPAP_EMAIL_SENDER}" "${NYSPAP_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
fi	


#############################################################
# Create Manifest file
#############################################################
echo "" >> ${LOGNAME}
echo "Create Manifest file for NYSPAP Extract.  " >> ${LOGNAME}

${RUNDIR}CreateManifestFile.sh ${S3BUCKET} ${TMSTMP} ${NYSPAP_EMAIL_SUCCESS_RECIPIENT} 


#############################################################
# Check the status of script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Shell script CreateManifestFile.sh failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Create Manifest file in NYSPAP_Extract_Bene_Info.sh - Failed (${ENVNAME})"
		MSG="Create Manifest file in NYSPAP_Extract_Bene_Info.sh  has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${NYSPAP_EMAIL_SENDER}" "${NYSPAP_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
fi	


#############################################################
# Move Finder File(s) processed to archive directory
#
# NOTE: The tempNYSPAP.txt file was created in LOAD_NYSPAP_FNDR_FILE.sh
#       and contains the list of S3 NYSPAP Finder Files
#############################################################

# Extract just the filenames from the S3 filename information
filename=`awk '{print $4}' ${DATADIR}tempNYSPAP.txt` 

echo "" >> ${LOGNAME}
echo "NYSPAP Finder file found: ${filename}" >> ${LOGNAME}


#################################################################################
# Delete finder file in linux. 
#################################################################################
echo "" >> ${LOGNAME}
echo "Delete finder file ${filename} in linux" >> ${LOGNAME}
rm ${DATADIR}${filename} 2>> ${LOGNAME} 


#################################################################################
# Move finder file in S3 to archive folder. 
#################################################################################
echo "" >> ${LOGNAME}

# Move S3 file to archive folder
aws s3 mv s3://${FINDER_FILE_BUCKET}${filename} s3://${FINDER_FILE_BUCKET}archive/${filename}  1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Moving S3 NYSPAP Finder files to S3 archive folder." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="NYSPAP_Extract_Bene_Info.sh - Failed (${ENVNAME})"
	MSG="Moving S3 files to s3 folder ${FINDER_FILE_BUCKET}archive failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${NYSPAP_EMAIL_SENDER}" "${NYSPAP_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	
	

#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "NYSPAP_Extract_Bene_Info.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS