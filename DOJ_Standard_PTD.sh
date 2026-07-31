#!/usr/bin/bash
############################################################################################################
# Name:  DOJ_DOJ_PartD_Standard.sh
#
# Desc: Extract for DOJ_PartD_Standard
#
# Author     : Natalya Tinovsky	
# Copied    : 12/31/2024 from DOJ_Travis_v_GileadSciences.sh
#
# Modified:
#
# Paul Baranoski 2023-12-20 Create script.
############################################################################################################

set +x
set -x
#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP:=`date +%Y%m%d.%H%M%S`}
PTD_SHELL=`basename $BASH_SOURCE`
PTD_NAME=`echo ${PTD_SHELL}|cut -d. -f1`
#PYTHON_SCRIPT="DOJ_PartD_Standard.py"
PYTHON_SCRIPT="${PTD_NAME}.py"
LOGNAME=/app/IDRC/XTR/CMS/logs/${PTD_NAME}_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "${PTD_SHELL} started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# Import common functions
############################################################
source ${RUNDIR}FilenameCounts.bash

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh

S3BUCKET=${DOJ_BUCKET} 

echo "${PTD_NAME} bucket=${S3BUCKET}" >> ${LOGNAME}

#############################################################
# function
#############################################################
function ExecuteExtract {

	FROM_DT=$1
	TO_DT=$2
	REQUESTOR=$3
	NDC_CODES=$4
    OUTPUT_FILE="DOJ_PARTD_${REQUESTOR}_${UNIQUE_FILE_TMSTMP}"
	
	echo "NDC_CODES=${NDC_CODES}" >> ${LOGNAME}
	echo "REQUESTOR=${REQUESTOR}" >> ${LOGNAME}
	echo "FROM_DT=${FROM_DT}" >> ${LOGNAME}
	echo "TO_DT=${TO_DT}" >> ${LOGNAME}
	echo "OUTPUT_FILE=${OUTPUT_FILE}" >> ${LOGNAME}
	#############################################################
	# Export environment variables for Python code
	#
	# NOTE: Need a unique Timestamp for each extract so that we can
	#       create a single manifest file for each extract file.
	#       Apparently, BOX has concurrency issues, and possible
	#       download size limitations. 
	#############################################################
	UNIQUE_FILE_TMSTMP=`date +%Y%m%d.%H%M%S`

	export UNIQUE_FILE_TMSTMP
	export NDC_CODES
	export REQUESTOR
	export FROM_DT
	export TO_DT
	export OUTPUT_FILE
	
	#############################################################
	# Execute python script  
	#############################################################
	echo "Start execution of ${PYTHON_SCRIPT} program"  >> ${LOGNAME}
	${PYTHON_COMMAND} ${RUNDIR}${PYTHON_SCRIPT} >> ${LOGNAME} 2>&1


	#############################################################
	# Check the status of python script  
	#############################################################
	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Python script ${PYTHON_SCRIPT} failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="${PYTHON_SCRIPT} - Failed ($ENVNAME)"
		MSG="Python script ${PYTHON_SCRIPT} failed."
#		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi

	echo "" >> ${LOGNAME}
	echo "Python script ${PYTHON_SCRIPT} completed successfully. " >> ${LOGNAME}
	

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

	concatFilename=${PTD_NAME}_${REQUESTOR}_${UNIQUE_FILE_TMSTMP}.txt.gz

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
			SUBJECT="Combining S3 files in ${PTD_SHELL} - Failed ($ENVNAME)"
			MSG="Combining S3 files in ${PTD_SHELL} has failed."
#			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
	fi	

	#############################################################
	# Create Manifest file
	#############################################################
	echo "" >> ${LOGNAME}
	echo "Create Manifest file for ${PTD_NAME}_${REQUESTOR} Extract.  " >> ${LOGNAME}

	#####################################################
	# S3BUCKET --> points to location of extract file. 
	#          --> S3 folder is key token to config file to determine of manifest file is in HOLD status   
	# TMSTMP   --> uniquely identifies extract file(s) 
	# DOJ_EMAIL_SUCCESS_RECIPIENT --> manifest file recipients
	#####################################################
	${RUNDIR}CreateManifestFile.sh ${S3BUCKET} ${UNIQUE_FILE_TMSTMP} ${DOJ_EMAIL_SUCCESS_RECIPIENT} 


	#############################################################
	# Check the status of script
	#############################################################
	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Shell script CreateManifestFile.sh failed." >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="Create Manifest file in ${PTD_SHELL} for ${PTD_NAME}_${REQUESTOR}  - Failed ($ENVNAME)"
			MSG="Create Manifest file in ${PTD_SHELL} for ${PTD_NAME}_${REQUESTOR}  has failed."
#			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
	fi	

}


#############################################################
# Run Extract for Harvoni 
#############################################################
echo "" >> ${LOGNAME}
echo "Run Extract for Harvoni" >> ${LOGNAME}

P_FROM_DT=2022-10-10
P_TO_DT=2022-12-31
P_REQUESTOR=Harvoni
P_NDC_CODES="'61958180101','61958180201','61958180301','61958180401','61958180501'"

ExecuteExtract ${P_FROM_DT} ${P_TO_DT} ${P_REQUESTOR} ${P_NDC_CODES}
	
	
#############################################################
# Run Extract for Sovaldi
#############################################################
echo "" >> ${LOGNAME}
echo "Run Extract for Sovaldi" >> ${LOGNAME}

P_FROM_DT=2022-12-06
P_TO_DT=2022-12-31
P_REQUESTOR=Sovaldi
P_NDC_CODES="'61958150101','61958150301','61958150401','61958150501'"

#ExecuteExtract ${P_FROM_DT} ${P_TO_DT} ${P_REQUESTOR} ${P_NDC_CODES}
		

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

SUBJECT="${PTD_NAME}_${REQUESTOR} extract ($ENVNAME) " 
MSG="The Extract for the creation of the ${PTD_SHELL} for ${PTD_NAME}_${REQUESTOR} data pull has completed.\n\nThe following file(s) were created:\n\n${S3Files}"

#${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Error in calling sendEmail.py" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Sending Success email in ${PTD_SHELL}  - Failed ($ENVNAME)"
		MSG="Sending Success email in ${PTD_SHELL}  has failed."
#		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
fi	


#############################################################
# end script
#############################################################
echo "" >> ${LOGNAME}
echo "${PTD_SHELL} completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS
