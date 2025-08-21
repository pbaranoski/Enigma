#!/usr/bin/bash
############################################################################################################
# Name:  VAPTD_Driver.sh
#
# Desc: VA Part D Quaterly Extract
#
# Author     : Joshua Turner	
# Created    : 12/19/2022
#
# Modified:
# Joshua Turner    2022-12-19  New script.
# Paul Baranoski   2023-11-28  Add exit 12 and failure email when script run in incorrect time period. 
#                              Add call to CreateManifestFile.sh with S3 mainfest file override bucket.
#                              Add ENVNAME to email Subject line. 
# Paul Baranoski   2023-12-04  Add FilenameCounts.bash and update extract filename logic for email.
# Joshua Turner    2024-05-21  Changed date and filename parms for Q1 to be -le '05' in case FF is later than usual
#                              Modified manifest bucket to VA_PBM 
# Joshua Turner    2024-05-22  For a more standard process and for file count standards, I am splitting the finder file load for Q1 out
#                              of this script to it's own process. When scheduled; Q1 will execute the finder
#                              file load, then extract. Q2 - Q4 will execute just the extract portion. This will be controlled by rundeck
#                              or other scheduler tool 
# Paul Baranoski   2024-11-05  Modified ending line to be "Ended at.." because Dashboard script is looking for that to know if extract ended successfully.  
############################################################################################################
set +x

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source /app/IDRC/XTR/CMS/scripts/run/SET_XTR_ENV.sh
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
LOGNAME=/app/IDRC/XTR/CMS/logs/VAPTD_Driver_${TMSTMP}.log
touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "VAPTD_Driver.sh started at ${TMSTMP} " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#include script for getting extract filenames and record counts
source ${RUNDIR}FilenameCounts.bash


#############################################################
# Establish Date Parameters  
#############################################################
CURR_YEAR=`date +%Y`
MONTH=`date +%m`

if [ $MONTH -le "05" ]; then
	PREV_YEAR=`expr ${CURR_YEAR} - 1`
	START_DATE="${PREV_YEAR}-10-01"
	END_DATE="${PREV_YEAR}-12-31"
	QTR="FY${CURR_YEAR}Q1"
elif [ $MONTH = "06" ]; then
	START_DATE="${CURR_YEAR}-01-01"
	END_DATE="${CURR_YEAR}-03-31"
	QTR="FY${CURR_YEAR}Q2"
elif [ $MONTH = "09" ]; then
	START_DATE="${CURR_YEAR}-04-01"
	END_DATE="${CURR_YEAR}-06-30"
	QTR="FY${CURR_YEAR}Q3"
elif [ $MONTH = "12" ]; then
	START_DATE="${CURR_YEAR}-07-01"
	END_DATE="${CURR_YEAR}-09-30"
	QTR="FY${CURR_YEAR}Q4"
else
    echo "Extract is processed quarterly for months March, June, September, and December. " >> ${LOGNAME}
	echo "Extract is not scheduled to run for this time period. " >> ${LOGNAME}
	echo "Processing completed." >> ${LOGNAME}

	# Send failure email
	SUBJECT="VAPTD_Driver.sh - FAILED ($ENVNAME)"
	MSG="Extract is processed quarterly for months March, June, September, and December. Extract is not scheduled to run for this time period."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${VAPTD_EMAIL_SENDER}" "${VAPTD_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
	
	exit 12
		
fi

echo "VAPTD Extract Processing with the following dates:" >> ${LOGNAME}
echo "QTR: ${QTR}" >> ${LOGNAME}
echo "START DATE: ${START_DATE}" >> ${LOGNAME}
echo "END DATE: ${END_DATE}" >> ${LOGNAME}
echo " " >> ${LOGNAME}

###########################################################################################
# Execute python script to extract VA Part D data and load the extract to S3 
###########################################################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
export START_DATE
export END_DATE
export QTR
export TMSTMP

echo ""
echo "Starting VAPTD_Extract.py." >> ${LOGNAME}
${PYTHON_COMMAND} ${RUNDIR}VAPTD_Extract.py >> ${LOGNAME} 2>&1

RET_STATUS=$?
if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Python script VAPTD_Extract.py failed." >> ${LOGNAME}
	
	# Send failure email
	SUBJECT="VA Part D Extract FAILED ($ENVNAME)"
	MSG="VA Part D Extract has failed in VAPTD_Driver.sh."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${VAPTD_EMAIL_SENDER}" "${VAPTD_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
	exit 12
fi


###########################################################################################
# Concatenate VA PTD S3 files into a single file 
###########################################################################################
echo ""
echo "Concatenate S3 files using CombineS3Files.sh." >> ${LOGNAME}
VAPTD_FILE=MOA_VAPARTD_${QTR}_${TMSTMP}.csv.gz
${RUNDIR}CombineS3Files.sh ${VAPTD_BUCKET} ${VAPTD_FILE} 

RET_STATUS=$?
if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script CombineS3Files.sh failed." >> ${LOGNAME}
	
	# Send failure email
	SUBJECT="VA Part D concatenation FAILED ($ENVNAME)"
	MSG="VA Part D Extract has failed in VAPTD_Driver.sh."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${VAPTD_EMAIL_SENDER}" "${VAPTD_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
	exit 12
fi


#############################################################
# Get list of S3 files and record counts for success email.
#############################################################
echo "" >> ${LOGNAME}
echo "Get S3 Extract file list and record counts" >> ${LOGNAME}

getExtractFilenamesAndCounts ${LOGNAME}  >> ${LOGNAME} 2>&1
S3Files="${filenamesAndCounts}" 


###########################################################################################
# Send Success Email
###########################################################################################
echo ""
echo "Sending success email" >> ${LOGNAME}
SUBJECT="VA Part D Extract Complete ($ENVNAME)"
MSG="VA Part D quarterly extract completed successfully.\n\nThe following file was generated:\n\n ${S3Files}"

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${VAPTD_EMAIL_SENDER}" "${VAPTD_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1


###########################################################################################
# Create manifest file for Box delivery (Supply ManifestFileFolder override parameter).
###########################################################################################
echo "" >> ${LOGNAME}
echo "" >> "Creating Manifest file for: ${VAPTD_FILE}" >> ${LOGNAME}

${RUNDIR}CreateManifestFile.sh ${VAPTD_BUCKET} ${TMSTMP} ${VAPTD_EMAIL_BOX_RECIPIENT} ${MANIFEST_VA_PBM_BUCKET}

RET_STATUS=$?
if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script CreateManifestFile.sh failed." >> ${LOGNAME}
	
	# Send failure email
	SUBJECT="Create Manifest file in VAPTD_Driver.sh - Failed ($ENVNAME)"
	MSG="VA Part D Extract has failed in the CreateManifestFile step of VAPTD_Driver.sh."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${VAPTD_EMAIL_SENDER}" "${VAPTD_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
	exit 12
fi


#############################################################
# script clean-up
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
echo "" >> ${LOGNAME}
echo "VA Part D Quarterly Extract completed successfully." >> ${LOGNAME}
echo "Ended at: ${TMSTMP}" >> ${LOGNAME}
exit $RET_STATUS
