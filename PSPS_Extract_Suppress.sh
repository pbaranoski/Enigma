#!/usr/bin/bash
#
######################################################################################
# Name:  PSPS_Extract_Suppress.sh
#
# Desc: PSPS Extract for Q6 Suppresion file. 
#
# Created: Paul Baranoski  07/15/2022
# Modified:
#
# Paul Baranoski 2022-11-10 Added call to CreateManifestFile.sh
# Paul Baranoski 2022-11-10 Added code to send Success emails with filenames from script
#                           instead of python code.
# Paul Baranoski 2023-09-01 Update logic to to get record counts using FilenameCounts.bash 
# Paul Baranoski 2023-09-19 Modify to use PSPS_SUPPRESSION_EMAIL constants instead of PSPS_EMAIL.
#                           Modify email text to include MF filename.
#                           Create CSV file from extract file and place in S3 DDOM folder (for Jag).
#                           Replace manifest file code with code to create EFT file.
# Paul Baranoski 2024-01-25 Add $ENVNAME to SUBJECT line of emails.
# Paul Baranoski 2024-08-15 Create new set of date parameters for SERV_CYQ and PROC_CYQ. 
# Paul Baranoski 2024-09-12 Add $ENVNAME to SUBJECT line of success email.
#                           Changed emails to use CMS_EMAIL_SENDER and ENIGMA_EMAIL_FAILURE_RECIPIENT.
######################################################################################
set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
LOGNAME=/app/IDRC/XTR/CMS/logs/PSPS_Extract_Supress_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

EMAIL_MF_FILENAME="P#IDR.XTR.PBAR.PSPSQ6.SUPRESS(0)"
gz_filename=PSPSQ6_SUPPRESS_${TMSTMP}.txt.gz
mapping_filename=PSPS_CSV_File_Mapping.csv

touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "PSPS_Extract_Suppression.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh >> ${LOGNAME}

source ${RUNDIR}FilenameCounts.bash

S3BUCKET=${PSPS_BUCKET} 

echo "PSPS bucket=${S3BUCKET}" >> ${LOGNAME}
echo "DDOM bucket=${DDOM_BUCKET}" >> ${LOGNAME}


############################################
# Extract current and prior year
############################################
CUR_YR=`date +%Y`
PRIOR_YR=`expr ${CUR_YR} - 1` 

echo "CUR_YR=${CUR_YR}" >> ${LOGNAME}
echo "PRIOR_YR=${PRIOR_YR}" >> ${LOGNAME}


############################################
# Determine Processing Qtr
############################################
MM=`date +%m`
if [ $MM = "07" -o $MM = "08" -o $MM = "09" ]; then
	SERV_CYQ_BEG_DT="CY${PRIOR_YR}Q1"
	SERV_CYQ_END_DT="CY${PRIOR_YR}Q4"

	PROC_CYQ_BEG_DT="CY${PRIOR_YR}Q1"
	PROC_CYQ_END_DT="CY${CUR_YR}Q2"

else
	echo "Extract is processed each July with Q6 data. " >> ${LOGNAME}
	echo "Extract is not scheduled to run for this time period. " >> ${LOGNAME}
	echo "Processing completed." >> ${LOGNAME}

	# Send Did not run email	
	SUBJECT="PSPS Extract did not run. (${ENVNAME})"
	MSG="Extract is processed each July with Q6 data. Extract is not scheduled to run for this time period."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 0 
fi


echo "SERV_CYQ_BEG_DT=${SERV_CYQ_BEG_DT}" >> ${LOGNAME}
echo "SERV_CYQ_END_DT=${SERV_CYQ_END_DT}" >> ${LOGNAME}

echo "PROC_CYQ_BEG_DT=${PROC_CYQ_BEG_DT}" >> ${LOGNAME}
echo "PROC_CYQ_END_DT=${PROC_CYQ_END_DT}" >> ${LOGNAME}

#############################################################
# Make variables available to Python code module.
#############################################################
export TMSTMP	
export SERV_CYQ_BEG_DT
export SERV_CYQ_END_DT

export PROC_CYQ_BEG_DT
export PROC_CYQ_END_DT


#############################################################
# Execute Python code
#############################################################
echo "" >> ${LOGNAME}
echo "Start execution of PSPS_Extract_Suppress.py program" >> ${LOGNAME}

${PYTHON_COMMAND} ${RUNDIR}PSPS_Extract_Suppress.py  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script PSPS_Extract_Suppress.py failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="PSPS Extract Supress - Failed (${ENVNAME})"
		MSG="The PSPS Extract Suppress script has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script PSPS_Extract_Suppress.py completed successfully." >> ${LOGNAME}


#############################################################
# Get list of extract files and record counts for success email.
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

SUBJECT="PSPS Q6 Suppression Extract (${ENVNAME})" 
MSG="The Extract for the creation of the PSPS Q6 Suppression file has completed.\n\nA mainframe version of the below file will be created as ${EMAIL_MF_FILENAME}.\n\nThe following file(s) were created:\n\n${S3Files}"

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${PSPS_SUPPRESSION_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Error in calling sendEmail.py" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Sending Success email in PSPS_Extract_Suppress.sh  - Failed (${ENVNAME})"
		MSG="Sending Success email in PSPS_Extract_Suppress.sh  has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
fi	


#############################################################
# EFT Extract file(s)
#############################################################
echo " " >> ${LOGNAME}
echo "EFT PSPS Q6 Suppression Extract File at `date`" >> ${LOGNAME}
${RUNDIR}ProcessFiles2EFT.sh ${S3BUCKET}  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of extract script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script ProcessFiles2EFT.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="PSPS_Extract_Suppress.sh - Failed (${ENVNAME})"
	MSG="PSPS Q6 Suppression Extract EFT process has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


#############################################################
# Create CSV file from PSPS Q6 Suppression extract file
#############################################################
echo "" >> ${LOGNAME}
echo "Start process to create CSV file at `date`"  >> ${LOGNAME} 


#############################################################
# Download Extract file from S3 to linux.
#############################################################
echo "" >> ${LOGNAME}
echo "Download S3 Extract file ${gz_filename} to linux"  >> ${LOGNAME} 

aws s3 cp s3://${S3BUCKET}archive/${gz_filename} ${DATADIR}${gz_filename}  1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Copying S3 file ${S3BUCKET}${gz_filename} to Linux failed." >> ${LOGNAME}
	
	# Send Failure email
	SUBJECT="PSPS_Extract_Suppress.sh - Failed (${ENVNAME})"
	MSG="Download S3 Extract file ${S3BUCKET}${gz_filename} to linux failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


#############################################################
# Unzip gzip file
#############################################################
echo " " >> ${LOGNAME}
echo "Unzip PSPS Q6 Suppression File ${gz_filename} on linux " >> ${LOGNAME}

gzip -d ${DATADIR}${gz_filename}

txt_filename=`echo ${gz_filename} | sed s/.gz// ` 2>> ${LOGNAME}


#############################################################
# Download Extract mapping config file.
#############################################################
echo "" >> ${LOGNAME}
echo "Download S3 Extract mapping config file ${mapping_filename} to linux"  >> ${LOGNAME} 

aws s3 cp s3://${CONFIG_BUCKET}${mapping_filename} ${DATADIR}${mapping_filename}  1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Copying S3 file ${CONFIG_BUCKET}${mapping_filename} to Linux failed." >> ${LOGNAME}
	
	# Send Failure email
	SUBJECT="PSPS_Extract_Suppress.sh - Failed (${ENVNAME})"
	MSG="Download S3 Extract file ${CONFIG_BUCKETT}${mapping_filename} to linux failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


#############################################################
# Create CSV file from Extract file
#############################################################
echo " " >> ${LOGNAME}
echo "Create PSPS Q6 Suppression CSV File at `date` " >> ${LOGNAME}

csv_filename=`echo ${txt_filename} | sed s/.txt/.csv/ ` 2>> ${LOGNAME}

echo "mapping_filename=${mapping_filename} "  >> ${LOGNAME}
echo "txt_filename=${txt_filename} "  >> ${LOGNAME}
echo "csv_filename=${csv_filename} "  >> ${LOGNAME}

parmInFileMapping=${DATADIR}${mapping_filename}
parmInfilename=${DATADIR}${txt_filename}
parmOutfilename=${DATADIR}${csv_filename}

${RUNDIR}createCSVFile.exe ${parmInFileMapping} ${parmInfilename} ${parmOutfilename}  >> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Creating PSPS Q6 Suppression CSV file ${csv_filename} failed." >> ${LOGNAME}
	
	# Send Failure email
	SUBJECT="PSPS_Extract_Suppress.sh - Failed (${ENVNAME})"
	MSG="Creating PSPS Q6 Suppression CSV file ${csv_filename} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi

	
#############################################################
# Upload CSV file to S3.
#############################################################
echo "" >> ${LOGNAME}
echo "Upload CSV extract file ${csv_filename} to S3 at `date`"  >> ${LOGNAME}

aws s3 mv  ${DATADIR}${csv_filename} s3://${DDOM_BUCKET}${csv_filename}  1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Copying linux file ${csv_filename} to S3 ${DDOM_BUCKET}${csv_filename} failed." >> ${LOGNAME}
	
	# Send Failure email
	SUBJECT="ProcessFiles2EFT.sh - Failed (${ENVNAME})"
	MSG="Copying linux file ${csv_filename} to S3 ${DDOM_BUCKET}${csv_filename} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "PSPS_Extract_Suppress.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS
