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
# Paul Baranoski 2023-04-26 Update comments to indicate that extracts will run the 25th of the month
#                           instead of the 8th.  
# Paul Baranoski 2023-08-22 Update sendEmail logic to get record counts using FilenameCounts.bash  
# Paul Baranoski 2023-09-20 Change manifest file logic to EFT logic.
# Paul Baranoski 2023-10-25 Correct code that displays 1 or 2 MF filenames in email text. Not working correctly.
# Paul Baranoski 2024-01-25 Add $ENVNAME to SUBJECT line of emails.
# Paul Baranoski 2024-07-26 Create new set of date parameters for SERV_CYQ and PROC_CYQ. Correct logic how the SERV_CYQ is built
#                           for Q5 and Q6.
######################################################################################
set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
LOGNAME=/app/IDRC/XTR/CMS/logs/PSPS_Extract_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

EMAIL_MF_FILENAME2=""

touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "PSPS_Extract.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh >> ${LOGNAME}

source ${RUNDIR}FilenameCounts.bash

S3BUCKET=${PSPS_BUCKET} 

echo "PSPS bucket=${S3BUCKET}" >> ${LOGNAME}


#############################################################
# function definitions  
#############################################################
sendSuccessEmail() {


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

	if [ "${EMAIL_MF_FILENAME2}" = "" ];then
		EMAIL_MF_FILENAMES=${EMAIL_MF_FILENAME1}
	else
		EMAIL_MF_FILENAMES="${EMAIL_MF_FILENAME1} and ${EMAIL_MF_FILENAME2}" 
	fi
	echo "EMAIL_MF_FILENAMES=${EMAIL_MF_FILENAMES}" >> ${LOGNAME}

	SUBJECT="PSPS Quarterly Extract (${ENVNAME})" 
	MSG="The PSPS Quarterly Extract has completed.\n\nA mainframe version(s) of the below file(s) will be created as ${EMAIL_MF_FILENAMES}.\n\nThe following file(s) were created:\n\n${S3Files}"

	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_EMAIL_SENDER}" "${PSPS_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Error in calling sendEmail.py" >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="Sending Success email in PSPS_Extract.sh - Failed (${ENVNAME})"
			MSG="Sending Success email in PSPS_Extract.sh  has failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_EMAIL_SENDER}" "${PSPS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
	fi
	
}

createEFTFile()  {

	#############################################################
	# EFT Extract file(s)
	#############################################################
	echo " " >> ${LOGNAME}
	echo "EFT PSPS Extract File at `date`" >> ${LOGNAME}
	${RUNDIR}ProcessFiles2EFT.sh ${S3BUCKET}  >> ${LOGNAME} 2>&1


	#############################################################
	# Check the status of extract script
	#############################################################
	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Shell script ProcessFiles2EFT.sh failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="EFT process in PSPS_Extract.sh - Failed (${ENVNAME})"
		MSG="EFT process in PSPS_Extract.sh failed"
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_EMAIL_SENDER}" "${PSPS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi

}





######################################################################################
# PSPS File Schedule Example
# For demonstration purposes, the 25th of the month following the end of the 
#     quarter will be used as the day on which the PSPS files are run.
#
# 4/25/2020
# PSPS 2020Q1 is generated. This file will contain claims incurred in Q1 2020 and processed on 1/1/2020 through 3/31/2020
#
# 7/25/2020
# PSPS 2020Q2 is generated. This file will contain claims incurred in Q1 and Q2 2020, and processed on 1/1/2020 through 6/30/2020
#
# 10/25/2020
# PSPS 2020Q3 is generated. This file will contain claims incurred in Q1, Q2, and Q3 2020, and processed on 1/1/2020 through 9/30/2020
#
# 1/25/2021
# PSPS 2020Q4 is generated. This file will contain claims incurred in Q1, Q2, Q3 and Q4 2020, and processed on 1/1/2020 through 12/31/2020
#
# 4/25/2021
# PSPS 2020Q5 is generated. This file will contain claims incurred in 2020, and processed on 1/1/2020 through 3/31/2021
# PSPS 2021Q1 is generated. This file will contain claims incurred in Q1 2021 and processed on 1/1/2021 through 3/31/2021
#
# 7/25/2021
# PSPS 2020Q6 is generated. This file will contain claims incurred in 2020, and processed on 1/1/2020 through 6/30/2021. 
#      This represents 6 quarters of 2020.
# PSPS 2021Q2 is generated. This file will contain claims incurred in Q1 and Q2 2021, and processed on 1/1/2021 through 6/30/2021
#
# 10/25/2021
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
	SUBJECT="PSPS Extract did not run. (${ENVNAME})"
	MSG="Extract is processed quarterly for months April, July, October, and January. Extract is not scheduled to run for this time period. "
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_EMAIL_SENDER}" "${PSPS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
	
	exit 0 	
fi

echo "QTR=${QTR}" >> ${LOGNAME}


############################################
# Build parms for appropriate Qtr
############################################
if [ $QTR == "Q1" ]; then
	SERV_CYQ_BEG_DT="CY${CUR_YR}Q1"
	SERV_CYQ_END_DT="CY${CUR_YR}Q1"
elif [ $QTR = "Q2" ]; then	
	SERV_CYQ_BEG_DT="CY${CUR_YR}Q1"
	SERV_CYQ_END_DT="CY${CUR_YR}Q2"
elif [ $QTR = "Q3" ]; then	
	SERV_CYQ_BEG_DT="CY${CUR_YR}Q1"
	SERV_CYQ_END_DT="CY${CUR_YR}Q3"
elif [ $QTR = "Q4" ]; then	
	SERV_CYQ_BEG_DT="CY${PRIOR_YR}Q1"
	SERV_CYQ_END_DT="CY${PRIOR_YR}Q4"
fi	

# For Q1 thru Q4 the SERV and PROC date ranges are the same
PROC_CYQ_BEG_DT=${SERV_CYQ_BEG_DT}
PROC_CYQ_END_DT=${SERV_CYQ_END_DT}

echo "SERV_CYQ_BEG_DT=${SERV_CYQ_BEG_DT}" >> ${LOGNAME}
echo "SERV_CYQ_END_DT=${SERV_CYQ_END_DT}" >> ${LOGNAME}

echo "PROC_CYQ_BEG_DT=${PROC_CYQ_BEG_DT}" >> ${LOGNAME}
echo "PROC_CYQ_END_DT=${PROC_CYQ_END_DT}" >> ${LOGNAME}

EMAIL_MF_FILENAME1="P#IDR.XTR.PBAR.PSPS${QTR}(0)"
echo "EMAIL_MF_FILENAME1=${EMAIL_MF_FILENAME1}" >> ${LOGNAME}
	
export SERV_CYQ_BEG_DT
export SERV_CYQ_END_DT

export PROC_CYQ_BEG_DT
export PROC_CYQ_END_DT
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
		SUBJECT="PSPS Extract (Q1-Q4) - Failed (${ENVNAME})"
		MSG="PSPS extract (Q1-Q4) has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_EMAIL_SENDER}" "${PSPS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi


############################################
# If Q5/6 processing --> continue
# Else --> send email; create manifest file
############################################
if [ $QTR == "Q1" ]; then
	SERV_CYQ_BEG_DT="CY${PRIOR_YR}Q1"
	SERV_CYQ_END_DT="CY${PRIOR_YR}Q4"
	
	PROC_CYQ_BEG_DT="CY${PRIOR_YR}Q1"
	PROC_CYQ_END_DT="CY${CUR_YR}Q1"

    QTR="Q5" 
	
elif [ $QTR == "Q2" ]; then	
	SERV_CYQ_BEG_DT="CY${PRIOR_YR}Q1"
	SERV_CYQ_END_DT="CY${PRIOR_YR}Q4"

	PROC_CYQ_BEG_DT="CY${PRIOR_YR}Q1"
	PROC_CYQ_END_DT="CY${CUR_YR}Q2"

    QTR="Q6"
else

	sendSuccessEmail
	
	createEFTFile

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

echo "SERV_CYQ_BEG_DT=${SERV_CYQ_BEG_DT}" >> ${LOGNAME}
echo "SERV_CYQ_END_DT=${SERV_CYQ_END_DT}" >> ${LOGNAME}

echo "PROC_CYQ_BEG_DT=${PROC_CYQ_BEG_DT}" >> ${LOGNAME}
echo "PROC_CYQ_END_DT=${PROC_CYQ_END_DT}" >> ${LOGNAME}

EMAIL_MF_FILENAME2="P#IDR.XTR.PBAR.PSPS${QTR}(0)"
echo "EMAIL_MF_FILENAME2=${EMAIL_MF_FILENAME2}" >> ${LOGNAME}
	
export SERV_CYQ_BEG_DT
export SERV_CYQ_END_DT

export PROC_CYQ_BEG_DT
export PROC_CYQ_END_DT

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
		SUBJECT="PSPS Extract (Q5-Q6) - Failed (${ENVNAME})"
		MSG="PSPS extract (Q5-Q6) has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_EMAIL_SENDER}" "${PSPS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script PSPS_Extract.py completed successfully. " >> ${LOGNAME}


#############################################################
# Send Success Email
#############################################################
sendSuccessEmail

#############################################################
# Create Manifest File
#############################################################
createEFTFile

#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "PSPS_Extract.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS
