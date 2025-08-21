#!/usr/bin/bash
############################################################################################################
# Name: OPMHI_PDE_HIST.sh
#
# Desc: OPM-HI PART D HISTORICAL LOAD
#
# Author     : Joshua Turner	
# Created    : 09/21/2023
#
# Modified:             Date:        Description:
# --------------------  -----------  -------------------------------------------------------------------
# Joshua Turner         2023-12-27   Updated for coding standards
############################################################################################################
set +x

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source /app/IDRC/XTR/CMS/scripts/run/SET_XTR_ENV.sh
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/
S3BUCKET=${OPMHI_PDE_BUCKET}

#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
RUNDATE=`date +%Y%m%d`
LOGNAME=/app/IDRC/XTR/CMS/logs/OPMHI_PDE_HIST_${TMSTMP}.log
touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

#############################################################
# Included to produce filenames and counts from the extracts
#############################################################
source ${RUNDIR}FilenameCounts.bash

echo "################################### " >> ${LOGNAME}
echo "OPMHI_PDE_HIST.sh started at `date`" >> ${LOGNAME}
echo "" >> ${LOGNAME}

###########################################################################################
# Establish Parameters - Download years parameter file from S3 to local data folder
###########################################################################################
PARM_FILE=OPMPDE_HIST_PARMS.txt
aws s3 cp s3://${CONFIG_BUCKET}${PARM_FILE} ${DATADIR}${PARM_FILE}  1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Copying OPM-HI PDE HIST Years Parameter file to Linux failed." >> ${LOGNAME}
	
	#Send Failure email	
	SUBJECT="OPM-HI PDE HIST Extract - Failed (${ENVNAME})"
	MSG="Copying S3 files from ${FINDER_FILE_BUCKET} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${OPMHI_HIST_EMAIL_SENDER}" "${OPMHI_HIST_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
	exit 12
fi

###########################################################################################
# Loop through the years parameter file and call OPMHI_PDE_HIST.py for each 
# year indicated in the file.
# NOTE: The tr command will remove the \r that may appear in parameter file when
#       the file is uploaded from windows to S3. The \r character will 
#       prevent the file from being processed properly.
###########################################################################################	
ParmFile2Process=`ls ${DATADIR}${PARM_FILE}` 1>> ${LOGNAME}  2>&1

echo "" >> ${LOGNAME}
echo "OPM-HI PDE Parameter file: ${ParmFile2Process}" >> ${LOGNAME}

years=`cat ${ParmFile2Process} | tr -d '\r' `

for line in $(echo $years )  
do
	#############################################################
	# Start extract for next parameter year
	#############################################################
	echo " " >> ${LOGNAME}
	echo "-----------------------------------" >> ${LOGNAME}
	
	# Extract Year from Parameter file record
	echo "Parameter record=${line}" >> ${LOGNAME}

	#################################################################################
	# Load parameters for Extract
	#################################################################################
	echo " " >> ${LOGNAME}

	EXT_YEAR=${line}
	
	echo "EXT_YEAR=${EXT_YEAR}" >> ${LOGNAME}

	# Export environment variables for Python code
	export EXT_YEAR
	export S3BUCKET

	#############################################################
	# Execute Python code to extract data.
	#############################################################
	echo "" >> ${LOGNAME}
	echo "Start execution of OPMHI_PDE_HIST.py program"  >> ${LOGNAME}
	${PYTHON_COMMAND} ${RUNDIR}OPMHI_PDE_HIST.py >> ${LOGNAME} 2>&1


	#############################################################
	# Check the status of python script  
	#############################################################
	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Python script OPMHI_PDE_HIST.py failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="OPMHI_PDE_HIST.sh  - Failed (${ENVNAME})"
		MSG="OPM-HI PDE HIST extract has failed in extract step."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${OPMHI_HIST_EMAIL_SENDER}" "${OPMHI_HIST_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
		exit 12
	fi

	echo "" >> ${LOGNAME}
	echo "Python script OPMHI_PDE_HIST.py completed successfully for year ${EXT_YEAR}. " >> ${LOGNAME}
done

###########################################################################################
# Get a list of all S3 files for success email
###########################################################################################
echo "" >> ${LOGNAME}
echo "Get S3 Extract file list" >> ${LOGNAME}

getExtractFilenamesAndCounts ${LOGNAME}  >> ${LOGNAME}  2>&1
S3Files="${filenamesAndCounts}"
echo "" >> ${LOGNAME} 

###########################################################################################
# Send Success Email
###########################################################################################
echo "" >> ${LOGNAME}
echo "Send success email with S3 Extract filename." >> ${LOGNAME}
echo "S3Files=${S3Files} "   >> ${LOGNAME}

SUBJECT="OPM HI PDE Extract Completed (${ENVNAME})" 
MSG="OPM HI PDE Extract from Snowflake has completed.\n\nThe following file(s) were created:\n\n${S3Files}"

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${OPMHI_HIST_EMAIL_SENDER}" "${OPMHI_HIST_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Error in calling sendEmail.py" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="OPMHI_PDE_HIST.sh - Failed (${ENVNAME})"
	MSG="Sending Success email in OPMHI_PDE_HIST.sh has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${OPMHI_HIST_EMAIL_SENDER}" "${OPMHI_HIST_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi

###########################################################################################
# move all extracts back to main folder from the HOLD folder (manifest file creation part)
###########################################################################################
echo "" >> ${LOGNAME}
echo "Moving all extracts back to ${S3BUCKET} from the HOLD folder" >> ${LOGNAME}
aws s3 mv s3://{S3BUCKET}HOLD s3://${S3BUCKET} --recursive
	

###########################################################################################
# End script
###########################################################################################
echo "" >> ${LOGNAME}
echo "OPMHI_PDE_HIST.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

exit $RET_STATUS
