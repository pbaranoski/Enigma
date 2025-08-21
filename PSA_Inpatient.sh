#!/usr/bin/bash
############################################################################################################
# Name:  PSA_Inpatient.sh
#
# Desc: Primary Service Area (PSA) Data for SSP ACOs (Physicians)
#
# Author     : Paul Baranoski	
# Created    : 12/07/2023
#
# Modified:
#
# Paul Baranoski 2023-12-07 Created script.
#
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/PSA_Inpatient_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

PSA_FF_INP_PREFIX=PSA_FINDER_FILE_DRG_MDC


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "PSA_Inpatient.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh

source ${RUNDIR}FilenameCounts.bash

S3BUCKET=${PSA_BUCKET} 

echo "PSA bucket=${S3BUCKET}" >> ${LOGNAME}
echo "Finder files bucket=${FINDER_FILE_BUCKET}" >> ${LOGNAME}


#################################################################################
# Remove any residual files in data directory.
#################################################################################
echo " " >> ${LOGNAME}
echo "Remove any residual Temp Files/Finder Files in data directory." >> ${LOGNAME}

rm ${DATADIR}tempPSAINP.txt  >> ${LOGNAME} 2>&1


#################################################################################
# Does Finder file exist?
#################################################################################
echo "" >> ${LOGNAME}

echo "List ${PSA_FF_INP_PREFIX}_* finder files in S3 " >> ${LOGNAME}

S3_FINDER_FILES=`aws s3 ls s3://${FINDER_FILE_BUCKET}${PSA_FF_INP_PREFIX}`  >> ${LOGNAME} 

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Listing S3 Finder Files failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="PSA_Inpatient.sh - Failed (${ENVNAME})"
	MSG="Listing S3 finder files from ${FINDER_FILE_BUCKET}${PSA_FF_INP_PREFIX} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSA_EMAIL_SENDER}" "${PSA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	

echo "S3_FINDER_FILES=${S3_FINDER_FILES}" >> ${LOGNAME} 


#################################################################################
# Copy Finder file from S3 to data directory
#################################################################################
FINDER_FILE=`echo ${S3_FINDER_FILES} | awk '{print $4}' `  2>> ${LOGNAME}

echo "" >> ${LOGNAME}
echo "Copy finder file ${FINDER_FILE} from S3 to data directory."  >> ${LOGNAME}

aws s3 cp s3://${FINDER_FILE_BUCKET}${FINDER_FILE}  ${DATADIR}${FINDER_FILE}  >> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script PSA_Inpatient.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Copying ${FINDER_FILE} from S3 to data directory - Failed (${ENVNAME})"
	MSG="Copying ${FINDER_FILE} from S3 to data directory has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi

echo "" >> ${LOGNAME}
echo "Remove any CR in finder file ${FINDER_FILE}"  >> ${LOGNAME}
# remove any CR characters from file (in-place)
sed -i 's/\r//g' ${DATADIR}${FINDER_FILE}   2>> ${LOGNAME}


#################################################################################
# Build column Name/Desc lists for PIVOT SQL (skip header)
# Ex. PIV_COL_DESC_LIST = 'Diseases & Disorders of the Ear, Nose, Mouth & Throat'
#     PIV_COL_NAME_LIST = "Diseases & Disorders of the Ear, Nose, Mouth & Throat"
#
# NOTE: Columns are in alphabetical order. An exception is to move one column to the front
#################################################################################
echo "" >> ${LOGNAME}
echo "Build Pivot table column list" >> ${LOGNAME}

COL2MOVE="'Pre-MDC'"

PIV_COL_DESC_LIST=""
PIV_COL_NAME_LIST=""

# 1) Skip header; 2) Finder file has 3 fields; 3rd field has descriptions
tail -n +2 ${DATADIR}${FINDER_FILE} | cut -d, -f3- | sort -u > ${DATADIR}tempPSAINP.txt  

while read NEXT_COL_DESC
do
	##echo "NEXT_COL_DESC=${NEXT_COL_DESC}" >> ${LOGNAME}

	# Remove any initial double quotes
	NEXT_COL_DESC=`echo ${NEXT_COL_DESC} | tr -d '"' `
	
	# First time thru --> not appending and no comma
	if  [ -z "${PIV_COL_DESC_LIST}" ];then
		PIV_COL_DESC_LIST="'${NEXT_COL_DESC}'"
	else
		PIV_COL_DESC_LIST="${PIV_COL_DESC_LIST},'${NEXT_COL_DESC}'"
	fi
	
done <  ${DATADIR}tempPSAINP.txt 

echo "PIV_COL_DESC_LIST=${PIV_COL_DESC_LIST}"  >> ${LOGNAME}

# Move column to be first column
echo "" >> ${LOGNAME}
echo "Ensure column ${COL2MOVE} is in list of columns " >> ${LOGNAME}
ColumnIsThere=`echo ${PIV_COL_DESC_LIST} | grep ${COL2MOVE}`
echo "ColumnIsThere: ${ColumnIsThere}" >> ${LOGNAME}

# If column is present, make it first in list; otherwise don't move any columns
if [ -n "${ColumnIsThere}" ];then
	echo "" >> ${LOGNAME}
	echo "Remove column ${COL2MOVE}" >> ${LOGNAME}
	PIV_COL_DESC_LIST=`echo ${PIV_COL_DESC_LIST} | sed "s/,${COL2MOVE}//"`
	
	echo "Add column ${COL2MOVE} to first in list" >> ${LOGNAME}
	PIV_COL_DESC_LIST=`echo "${COL2MOVE},${PIV_COL_DESC_LIST}" `
	echo  "${PIV_COL_DESC_LIST}"  >> ${LOGNAME}
fi  

echo "" >> ${LOGNAME}
echo "Create double-quote list using single-quote list" >> ${LOGNAME}

# Convert single quotes to double quotes in string 
PIV_COL_NAME_LIST=`echo ${PIV_COL_DESC_LIST}  | sed "s_'_\"_g" ` 	2>> ${LOGNAME}
echo "PIV_COL_NAME_LIST=${PIV_COL_NAME_LIST}"  >> ${LOGNAME}


#############################################################
# Create parameter YYYY value (Prior Year).
#############################################################
echo "" >> ${LOGNAME}
echo "Calculate Prior Year parameter" >> ${LOGNAME}

YYYY=`date -d "-1 year" +%Y`  2>> ${LOGNAME}
echo "YYYY=${YYYY}"  >> ${LOGNAME}


#############################################################
# Execute Python code to extract PSA data.
#############################################################
echo "" >> ${LOGNAME}
echo "Start execution of PSA_Inpatient.py program"  >> ${LOGNAME}

# Export environment variables for Python code
export TMSTMP
export YYYY
export PIV_COL_DESC_LIST
export PIV_COL_NAME_LIST

${PYTHON_COMMAND} ${RUNDIR}PSA_Inpatient.py >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script  
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Python script PSA_Inpatient.py failed" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="PSA_Inpatient.sh  - Failed ${ENVNAME}"
	MSG="PSA Inpatient Extract has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSA_EMAIL_SENDER}" "${PSA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script PSA_Inpatient.py completed successfully. " >> ${LOGNAME}


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

SUBJECT="PSA Inpatient extract (${ENVNAME})" 
MSG="The Extract for the creation of the PSA Inpatient extract file from Snowflake has completed.\n\nThe following file(s) were created:\n\n${S3Files}"

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSA_EMAIL_SENDER}" "${PSA_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Error in calling sendEmail.py" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Sending Success email in PSA_Inpatient.sh  - Failed ${ENVNAME}"
		MSG="Sending Success email in PSA_Inpatient.sh  has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSA_EMAIL_SENDER}" "${PSA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
fi	


#################################################################################
# Move Finder Files in S3 to archive directory.
#################################################################################
echo "" >> ${LOGNAME}
echo "Move processed S3 PSA Inpatient Finder File to archive directory in S3." >> ${LOGNAME}

aws s3 mv s3://${FINDER_FILE_BUCKET}${FINDER_FILE} s3://${FINDER_FILE_BUCKET}archive/${FINDER_FILE}   1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Moving S3 Finder file ${FINDER_FILE} to S3 archive folder failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="PSA_Inpatient.sh  - Failed ${ENVNAME}"
	MSG="Moving S3 finder file ${FINDER_FILE} from ${FINDER_FILE_BUCKET} to S3 archive folder failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSA_EMAIL_SENDER}" "${PSA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME} 
echo "Remove tempPSAINP.txt from data directory" >> ${LOGNAME} 

rm ${DATADIR}tempPSAINP.txt >> ${LOGNAME} 2>&1
rm ${DATADIR}${PSA_FF_INP_PREFIX}*  >> ${LOGNAME} 2>&1


#############################################################
# end script
#############################################################
echo "" >> ${LOGNAME}
echo "PSA_Inpatient.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS