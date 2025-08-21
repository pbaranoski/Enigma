#!/usr/bin/bash
#
######################################################################################
# Name:  PSPS_Extract.sh
#
# Desc: PSPS Extract for Q1 thru Q6 extracts. 
#
# Created: Viren Khanna  07/15/2022
# Modified: 
# Paul Baranoski 2022-08-04 Added "else" statement to send message to log that
#                           Extract is not schedule to run in non-Qtr months.
# Paul Baranoski 2022-08-16 Added code to unzip .gz Q6 file (needed by Split files script)
#
# Paul Baranoski 2022-11-09 Modify code in assigning Qtr value by including month ranges 
#                           instead of using a single month.
# Paul Baranoski 2022-11-09 Create functions to make processing flow simpler and eliminate
#                           duplicate code.
# Paul Baranoski 2022-11-09 Added call to CreateManifestFile.sh
# Paul Baranoski 2022-11-09 Added code to send Success emails with filenames from script
#                           instead of python code.           
######################################################################################
set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
LOGNAME=/app/IDRC/XTR/CMS/logs/PSPS_Extract_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "PSPS_Extract.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh >> ${LOGNAME}

S3BUCKET=${PSPS_BUCKET} 

echo "PSPS bucket=${S3BUCKET}" >> ${LOGNAME}


#############################################################
# function definitions  
#############################################################
sendSuccessEmail() {


	#############################################################
	# Get list of S3 files for success email.
	#############################################################
	echo "" >> ${LOGNAME}
	echo "Get S3 Extract file list" >> ${LOGNAME}

	S3Files=`aws s3 ls s3://${S3BUCKET} | awk '{print $4}' | grep ${TMSTMP} | tr ' ' '\n' `  2>> ${LOGNAME}

	RET_STATUS=$?

	if [ $RET_STATUS != 0 ]; then
			echo "" >> ${LOGNAME}
			echo "Error in getting S3 Extract file list" >> ${LOGNAME}

			# Send Failure email	
			SUBJECT="Get S3 Extract file list in PSPS_Extract.sh - Failed"
			MSG="S3 Extract file list in PSPS_Extract.sh has failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_EMAIL_SENDER}" "${PSPS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
	fi


	#############################################################
	# Send Success email.
	#############################################################
	echo "" >> ${LOGNAME}
	echo "Send success email with S3 Extract filename." >> ${LOGNAME}
	echo "S3Files=${S3Files} "   >> ${LOGNAME}

	SUBJECT="PSPS Quarterly Extract" 
	MSG="The PSPS Quarterly Extract has completed.\n\nThe following file(s) were created:\n\n${S3Files}"

	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_EMAIL_SENDER}" "${PSPS_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Error in calling sendEmail.py" >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="Sending Success email in PSPS_Extract.sh  - Failed"
			MSG="Sending Success email in PSPS_Extract.sh  has failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_EMAIL_SENDER}" "${PSPS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
	fi
	
}


createManifestFile()  {

	#############################################################
	# Create Manifest file
	#############################################################
	echo "" >> ${LOGNAME}
	echo "Create Manifest file for PSPS Extract.  " >> ${LOGNAME}

	${RUNDIR}CreateManifestFile.sh ${S3BUCKET} ${TMSTMP} ${PSPS_EMAIL_SUCCESS_RECIPIENT} 


	#############################################################
	# Check the status of script
	#############################################################
	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Shell script CreateManifestFile.sh failed." >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="Create Manifest file in PSPS_Extract.sh  - Failed"
			MSG="Create Manifest file in PSPS_Extract.sh  has failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_EMAIL_SENDER}" "${PSPS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
	fi	

}


copyS3File2Linux() {

	echo "" >> ${LOGNAME}
	echo "Starting copy of S3 ${QTR} file to Linux." >> ${LOGNAME}
			
	S3Filename=PSPS_Extract_${QTR}_${TMSTMP}.csv.gz
	aws s3 cp s3://${S3BUCKET}${S3Filename} ${DATADIR}PBAR_PSPS${QTR}_${TMSTMP}.txt.gz  1>> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Copying ${QTR} S3 file to Linux failed." >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="PSPS Extract - Failed"
			MSG="PSPS Extract copy ${QTR} S3 file failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_EMAIL_SENDER}" "${PSPS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
	fi	
	
	##############################
	# gunzip Q4/Q6 file from S3
	##############################
	echo " " >> ${LOGNAME}
	echo "Unzip .gz ${QTR} file" >> ${LOGNAME}
	echo "Started --> `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}

	gzip -d ${DATADIR}PBAR_PSPS${QTR}_${TMSTMP}.txt.gz  2>>  ${LOGNAME}
	

}


######################################################################################
# PSPS File Schedule Example
# For demonstration purposes, the 8th of the month following the end of the 
#     quarter will be used as the day on which the PSPS files are run.
#
# 4/08/2020
# PSPS 2020Q1 is generated. This file will contain claims incurred in Q1 2020 and processed on 1/1/2020 through 3/31/2020
#
# 7/08/2020
# PSPS 2020Q2 is generated. This file will contain claims incurred in Q1 and Q2 2020, and processed on 1/1/2020 through 6/30/2020
#
# 10/08/2020
# PSPS 2020Q3 is generated. This file will contain claims incurred in Q1, Q2, and Q3 2020, and processed on 1/1/2020 through 9/30/2020
#
# 1/08/2021
# PSPS 2020Q4 is generated. This file will contain claims incurred in Q1, Q2, Q3 and Q4 2020, and processed on 1/1/2020 through 12/31/2020
#
# 4/08/2021
# PSPS 2020Q5 is generated. This file will contain claims incurred in 2020, and processed on 1/1/2020 through 3/31/2021
# PSPS 2021Q1 is generated. This file will contain claims incurred in Q1 2021 and processed on 1/1/2021 through 3/31/2021
#
# 7/08/2021
# PSPS 2020Q6 is generated. This file will contain claims incurred in 2020, and processed on 1/1/2020 through 6/30/2021. 
#      This represents 6 quarters of 2020.
# PSPS 2021Q2 is generated. This file will contain claims incurred in Q1 and Q2 2021, and processed on 1/1/2021 through 6/30/2021
#
# 10/08/2021
# PSPS 2021Q3 is generated. This file will contain claims incurred in Q1, Q2, and Q3 2021, and processed on 1/1/2021 through 9/30/2021
######################################################################################

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
MM="07"
if [   $MM = "04" -o $MM = "05" -o $MM = "06" ]; then
	QTR=Q1
elif [ $MM = "07" -o $MM = "08" -o $MM = "09" ]; then
	QTR=Q2
elif [ $MM = "10" -o $MM = "11" -o $MM = "12" ]; then
	QTR=Q3	
elif [ $MM = "01" -o $MM = "02" -o $MM = "03" ]; then
	QTR=Q4	
else
	echo "Extract is processed quarterly for months April, July, October, and January. " >> ${LOGNAME}
	echo "Extract is not scheduled to run for this time period. " >> ${LOGNAME}
	echo "Processing completed." >> ${LOGNAME}
	
	# Send Did not run email	
	SUBJECT="PSPS Extract did not run."
	MSG="Extract is processed quarterly for months April, July, October, and January. Extract is not scheduled to run for this time period. "
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_EMAIL_SENDER}" "${PSPS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
	
	exit 0 	
fi

echo "QTR=${QTR}" >> ${LOGNAME}


############################################
# Build parms for appropriate Qtr
############################################
if [ $QTR == "Q1" ]; then
	CLNDR_CYQ_BEG_DT="CY${CUR_YR}Q1"
	CLNDR_CYQ_END_DT="CY${CUR_YR}Q1"
elif [ $QTR = "Q2" ]; then	
	CLNDR_CYQ_BEG_DT="CY${CUR_YR}Q1"
	CLNDR_CYQ_END_DT="CY${CUR_YR}Q2"
elif [ $QTR = "Q3" ]; then	
	CLNDR_CYQ_BEG_DT="CY${CUR_YR}Q1"
	CLNDR_CYQ_END_DT="CY${CUR_YR}Q3"
elif [ $QTR = "Q4" ]; then	
	CLNDR_CYQ_BEG_DT="CY${PRIOR_YR}Q1"
	CLNDR_CYQ_END_DT="CY${PRIOR_YR}Q4"
fi	

echo "CLNDR_CYQ_BEG_DT=${CLNDR_CYQ_BEG_DT}" >> ${LOGNAME}
echo "CLNDR_CYQ_END_DT=${CLNDR_CYQ_END_DT}" >> ${LOGNAME}
	
export CLNDR_CYQ_BEG_DT
export CLNDR_CYQ_END_DT
export QTR
export TMSTMP


############################################
# Execute appropriate Qtr Extract
############################################
echo " " >> ${LOGNAME}
echo "Extract processing for appropriate Qtr between Q1-Q4. " >> ${LOGNAME}
${PYTHON_COMMAND} ${RUNDIR}PSPS_Extract.py  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script PSPS_Extract.py failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="PSPS Extract (Q1-Q4) - Failed"
		MSG="PSPS extract (Q1-Q4) has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_EMAIL_SENDER}" "${PSPS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi


#################################################################################
# Copy Q4 (EARLY cut file to Linux as .txt file for use in PSPS_Split_files.bash
#################################################################################
if [ $QTR == "Q4" ]; then
	copyS3File2Linux
fi	

############################################
# If Q5/6 processing --> continue
# Else --> send email; create manifest file
############################################
if [ $QTR == "Q1" ]; then
	CLNDR_CYQ_BEG_DT="CY${PRIOR_YR}Q1"
	CLNDR_CYQ_END_DT="CY${CUR_YR}Q1"
    QTR="Q5" 
elif [ $QTR == "Q2" ]; then	
	CLNDR_CYQ_BEG_DT="CY${PRIOR_YR}Q1"
	CLNDR_CYQ_END_DT="CY${CUR_YR}Q2"
    QTR="Q6"
else

	sendSuccessEmail
	
	createManifestFile

	echo "" >> ${LOGNAME}
	echo "PSPS_Extract.sh completed successfully." >> ${LOGNAME}

	echo "Ended at `date` " >> ${LOGNAME}
	echo "" >> ${LOGNAME}
	
	exit 0 	
fi


############################################
# Perform Qtr 5/6 processing 
############################################
echo " " >> ${LOGNAME}
echo "Extract processing for appropriate Qtr between Q5-Q6. " >> ${LOGNAME}

echo "CLNDR_CYQ_BEG_DT=${CLNDR_CYQ_BEG_DT}" >> ${LOGNAME}
echo "CLNDR_CYQ_END_DT=${CLNDR_CYQ_END_DT}" >> ${LOGNAME}
	
export CLNDR_CYQ_BEG_DT
export CLNDR_CYQ_END_DT
export QTR
export TMSTMP

############################################
# Execute appropriate Qtr 5 or 6 Extract
############################################
${PYTHON_COMMAND} ${RUNDIR}PSPS_Extract.py  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script PSPS_Extract.py failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="PSPS Extract (Q5-Q6) - Failed"
		MSG="PSPS extract (Q5-Q6) has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_EMAIL_SENDER}" "${PSPS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script PSPS_Extract.py completed successfully. " >> ${LOGNAME}


######################################################################
# Copy Q6 file to Linux as .txt file for use in PSPS_Split_files.bash
######################################################################
if [ $QTR == "Q6" ]; then
	copyS3File2Linux
fi	

#############################################################
# Send Success Email
#############################################################
sendSuccessEmail

#############################################################
# Create Manifest File
#############################################################
createManifestFile

#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "PSPS_Extract.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS
