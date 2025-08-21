#!/usr/bin/bash
############################################################################################################
# Name: SAFENC_CAR_Extract.sh
#
# Desc: SAF ENC CAR Extract
##
#
#  $1 = CAR
#
# Author     : Viren Khanna
# Created    : 5/30/2023
#
# Modified: 
# Viren Khanna 	 2023-05-30 	New script.
# Paul Baranoski 2024-02-28 Add ENVNAME to SUBJECT line for emails. 
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/SAFENC_CAR_Extract_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/




touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "SAFENC_CAR_Extract.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

source ${RUNDIR}FilenameCounts.bash


#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh

S3BUCKET=${SAFENC_CAR_BUCKET} 
echo "SAF ENC CAR bucket=${S3BUCKET}" >> ${LOGNAME}
echo "configuration file bucket=${CONFIG_BUCKET}" >> ${LOGNAME}


#################################################################################
# Download SAF Parameter file from S3 to data directory.
# NOTE: Make sure that the last record in the SAF_ENC_PARM_FILE has an 
#       ending \n. (Press <ENTER> after each record in file). 
#################################################################################
echo "" >> ${LOGNAME}
echo "Copy SAF Encounter ${SAF_ENC_PARM_FILE} from S3 to linux" >> ${LOGNAME}

SAF_ENC_PARM_FILE=SAF_ENC_CAR_PARM_FILE.txt

# Copy S3 file to linux
aws s3 cp s3://${CONFIG_BUCKET}${SAF_ENC_PARM_FILE} ${DATADIR}${SAF_ENC_PARM_FILE}  1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Copying S3 SAF ${SAF_ENC_PARM_FILE} parameter file to Linux failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="SAF Encounter CAR Extract - Failed (${ENVNAME})"
	MSG="Copying S3 SAF ${SAF_ENC_PARM_FILE} parameter file from ${CONFIG_BUCKET} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${SAFENC_EMAIL_SENDER}" "${SAFENC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	


#################################################################################
# Find SAF ENC CAR Extract Years Parameter file in data directory.
#################################################################################
ParmFile2Process=`ls ${DATADIR}${SAF_ENC_PARM_FILE}` 1>> ${LOGNAME}  2>&1

echo "" >> ${LOGNAME}
echo "SAF Encounter CAR Extract Parameter file on linux: ${ParmFile2Process}" >> ${LOGNAME}



#################################################################################
# Loop thru SAF Encounter CAR Date Ranges in parameter file.
# 
# Configuration file format: YYYY-MM-DD,YYYY-MM-DD,RNG_LIT,STUS_LIT
#  where STUS_LIT = [EARLY,FINAL]  RNG_LIT = [Ex: QTR1,QTR2 or JANWK1,JANWK2]
#
# NOTE: The tr command will remove the \r that may appear in parameter file when
#       the file is uploaded from windows to S3. The \r character will 
#       prevent the file from being processed properly.
#################################################################################
record=`cat ${ParmFile2Process} | tr -d '\r' `

for rec in $(echo $record ) 
do

		#############################################################
		# Start extract for next parameter year
		#############################################################
		echo " " >> ${LOGNAME}
		echo "-----------------------------------" >> ${LOGNAME}
		
		# Display Parameter file record
		echo "Parameter record=${rec}" >> ${LOGNAME}

		#################################################################################
		# Load parameters for Extract
		#################################################################################
		echo " " >> ${LOGNAME}

		EXT_FROM_DT=`echo ${rec} | cut -d, -f1`  2>> ${LOGNAME}
		EXT_TO_DT=`echo ${rec} | cut -d, -f2`    2>> ${LOGNAME}
		YYYY=`echo ${EXT_FROM_DT} | cut -c1-4 `  2>> ${LOGNAME}
		
		RNG_LIT=`echo ${rec} | cut -d, -f3`    2>> ${LOGNAME}
		STUS_LIT=`echo ${rec} | cut -d, -f4`    2>> ${LOGNAME}
		
		echo "EXT_FROM_DT=${EXT_FROM_DT}" >> ${LOGNAME}
		echo "EXT_TO_DT=${EXT_TO_DT}" >> ${LOGNAME}
		echo "RNG_LIT=${RNG_LIT}" >> ${LOGNAME}
		echo "STUS_LIT=${STUS_LIT}" >> ${LOGNAME}
				

		# Export environment variables for Python code
		export TMSTMP
		export EXT_FROM_DT
		export EXT_TO_DT
		export RNG_LIT
		export STUS_LIT
		export YYYY

                export CLM_TYPE_LIT
		



                #############################################################
                # Execute Python code to extract data.
                #############################################################
		echo "" >> ${LOGNAME}
		echo "Start execution of SAFENC_CAR_Extract.py program"  >> ${LOGNAME}
		${PYTHON_COMMAND} ${RUNDIR}SAFENC_CAR_Extract.py >> ${LOGNAME} 2>&1


		#############################################################
		# Check the status of python script  
		#############################################################
		RET_STATUS=$?

		if [[ $RET_STATUS != 0 ]]; then
				echo "" >> ${LOGNAME}
				echo "Python script SAFENC_CAR_Extract.py failed" >> ${LOGNAME}
				
				# Send Failure email	
				SUBJECT="SAFENC_CAR_Extract.sh - Failed (${ENVNAME})"
				MSG="SAF ENC CAR extract has failed."
				${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${SAFENC_CAR_EMAIL_SENDER}" "${SAFENC_CAR_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

				exit 12
		fi

		echo "" >> ${LOGNAME}
		echo "Python script SAFENC_CAR_Extract.py completed successfully. " >> ${LOGNAME}

                ####################################################################
		# Concatenate S3 files
		# NOTE: Multiple files with suffix "n_n_n.csv.gz" are created. 
		#       Will concatenate them into single file.
		#
		# Example --> blbtn_clm_ex_20220922.084321.csv.gz_0_0_0.csv.gz 
		#         --> blbtn_clm_ex_20220922.084321.csv.gz
		####################################################################
		echo "" >> ${LOGNAME}
		echo "Concatenate S3 files using CombineS3Files.sh   " >> ${LOGNAME}

		echo "S3BUCKET=${S3BUCKET} " >> ${LOGNAME} 

		concatFilename=SAFENC_CAR_${STUS_LIT}_${RNG_LIT}_${TMSTMP}.txt.gz
		echo "concatFilename=${concatFilename}" >> ${LOGNAME} 

		${RUNDIR}CombineS3Files.sh ${S3BUCKET} ${concatFilename} 


		#############################################################
		# Check the status of script
		#############################################################
		RET_STATUS=$?

		if [[ $RET_STATUS != 0 ]]; then
				echo "" >> ${LOGNAME}
				echo "Shell script CombineS3Files.sh failed." >> ${LOGNAME}
				
				# Send Failure email	
				SUBJECT="Combining S3 files in SAF_ENC_CAR_Extract.sh - Failed (${ENVNAME})"
				MSG="Combining S3 files in SAF_ENC_CAR_Extract.sh has failed."
				${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${SAFENC_EMAIL_SENDER}" "${SAFENC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

				exit 12
		fi	


done

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

SUBJECT="SAF ENC CAR extract (${ENVNAME})" 
MSG="The SAF ENC CAR extract from Snowflake has completed.\n\nThe following file(s) were created:\n\n${S3Files}"

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${SAFENC_CAR_EMAIL_SENDER}" "${SAFENC_CAR_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Error in calling sendEmail.py" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Sending Success email in SAFENC_CAR_Extract.sh - Failed (${ENVNAME})"
	MSG="Sending Success email in SAFENC_CAR_Extract.sh  has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${SAFENC_CAR_EMAIL_SENDER}" "${SAFENC_CAR_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	

#############################################################
# EFT Extract files
#############################################################
echo " " >> ${LOGNAME}
echo "EFT SAF Encounter CAR Extract Files " >> ${LOGNAME}
${RUNDIR}ProcessFiles2EFT.sh ${S3BUCKET}  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of extract script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script ProcessFiles2EFT.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="SAF Encounter CAR EFT process - Failed (${ENVNAME})"
	MSG="SAF Encounter CAR EFT process has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${SAF_ENC_EMAIL_SENDER}" "${SAF_ENC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi




#############################################################
# remove unused files from linux data directory
#############################################################
echo " " >> ${LOGNAME}
echo "Delete parameter file ${SAF_ENC_PARM_FILE} from linux data directory " >> ${LOGNAME}
rm ${DATADIR}${SAF_ENC_PARM_FILE}  2>> ${LOGNAME}


#############################################################
# End script
#############################################################
echo "" >> ${LOGNAME}
echo "SAFENC_CAR_Extract.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

exit $RET_STATUS
