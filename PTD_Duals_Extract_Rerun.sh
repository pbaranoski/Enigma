#!/usr/bin/bash
############################################################################################################
# Name:  PTD_Duals_Extract_Rerun.sh
#
# Desc: PTD Duals Rerun extract.  
#
# Execute as ./PTD_Duals_Extract_Rerun.sh 
#
# Follow these steps before executing script:
# 1) Download the state parameter file PTDDualsHistStParms.txt in S3
# 2) Modify the file for state to be processed
# 3) Upload modified PTDDualsHistStParms.txt to S3  
# 6) Execute script thru rundeck, indicating date parm values to use   
#
# Author     : Paul Baranoski	
# Created    : 01/05/2023
#
# Modified:
#
# Joshua Turner  2024-05-21 Cloned from PTD_Duals_Historical.sh - Updated to get & set parameters
#                           from the Rundeck job.
#                           This modified script will continue to use the 'Historical' path for creating the files
#                2024-06-10 Temp change bucket to PTD MONTHLY due to S3 deep freeze archival of other files
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/PTD_Duals_Extract_Rerun_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME}

PTDDUAL_EMAIL_SENDER="BIA_SUPPORT@cms.hhs.gov" 
PTDDUAL_EMAIL_SUCCESS_RECIPIENT="jturner-con@index-analytics.com"
PTDDUAL_EMAIL_FAILURE_RECIPIENT="jturner-con@index-analytics.com"

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh 

echo "################################### " >> ${LOGNAME}
echo "PTD_Duals_Extract_Rerun.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# This Rerun script requires the following fields to be sent
# from the Rundeck job: 
#	- CLNDR_CY_MO_NUM_ENDDT
#	- CLM_SUBMSN_DT_START_DT
#	- CLM_SUBMSN_DT_END_DT
#############################################################
if [ -z $1 ]; then
	# Send Failure email	
	SUBJECT="PTD_Duals_Extract_Rerun.sh - Failed (${ENVNAME})"
	MSG="The process was called without any paramters."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PTDDUAL_EMAIL_SENDER}" "${PTDDUAL_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
else
	CLNDR_CY_MO_NUM_ENDDT=$1
	CLM_SUBMSN_DT_START_DT=$2
	CLM_SUBMSN_DT_END_DT=$3
fi

# create variables used in filename
YYYY=`echo ${CLM_SUBMSN_DT_END_DT} | cut -c1-4 `
MM=`echo ${CLM_SUBMSN_DT_END_DT} | cut -c6-7 `

EXTRACT_TYPE=Historical
ST_PARMFILE=PTDDualsHistStParms.txt
S3_EXTRACT_FILE="PTDDUALS_HIST_Y${YYYY}M${MM}_${TMSTMP}.csv.gz"
ST_EXT_FNAME_MODEL="PTDDUALS_HIST_XX_Y${YYYY}M${MM}_${TMSTMP}.txt"

echo "Parameters to script: " >> ${LOGNAME}
echo "   CLNDR_CY_MO_NUM_ENDDT=${CLNDR_CY_MO_NUM_ENDDT}" >> ${LOGNAME}
echo "   CLM_SUBMSN_DT_START_DT=${CLM_SUBMSN_DT_START_DT}" >> ${LOGNAME}
echo "   CLM_SUBMSN_DT_END_DT=${CLM_SUBMSN_DT_END_DT}" >> ${LOGNAME}
echo "   PTDDUAL_EMAIL_SENDER=${PTDDUAL_EMAIL_SENDER}" >> ${LOGNAME}
echo "   PTDDUAL_EMAIL_SUCCESS_RECIPIENT=${PTDDUAL_EMAIL_SUCCESS_RECIPIENT}" >> ${LOGNAME}
echo "   PTDDUAL_EMAIL_FAILURE_RECIPIENT=${PTDDUAL_EMAIL_FAILURE_RECIPIENT}" >> ${LOGNAME}

#############################################################
# Set variables to appropriate values for Historical processing.
#############################################################
echo " " >> ${LOGNAME}
echo "Historical processing started. " >> ${LOGNAME}
echo " " >> ${LOGNAME}

S3BUCKET=${PTDDUALMNTH_BUCKET} 

echo "PTD_Duals bucket=${S3BUCKET}" >> ${LOGNAME}

#################################################################################
# Remove residual duals linux files
#################################################################################
echo " " >> ${LOGNAME}
echo "Remove residual PTD_DUALS_* files on linux data directory." >> ${LOGNAME}

rm "${DATADIR}"PTD_DUALS_* 2>>  ${LOGNAME}

#################################################################################
# Download PTD Dual State parameter file to Linux.
#################################################################################
echo "" >> ${LOGNAME}

## Copy PTD Dual State parameter file to Linux
aws s3 cp s3://${CONFIG_BUCKET}${ST_PARMFILE} ${DATADIR}${ST_PARMFILE}   1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Copying ${ST_PARMFILE} from S3 to Linux failed." >> ${LOGNAME}

	# Send Failure email	
	SUBJECT="PTD_Duals_Extract_Rerun.sh - Failed (${ENVNAME})"
	MSG="Copying ${ST_PARMFILE} from S3 to Linux failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PTDDUAL_EMAIL_SENDER}" "${PTDDUAL_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi

#################################################################################
# Create State IN-Phrase for Python program  
#################################################################################
echo " " >> ${LOGNAME}
echo "Create State In-Phrase parameter for the Python Extract program." >> ${LOGNAME}
echo " " >> ${LOGNAME}

# Get list of states from state param file: get first 2 bytes, remove comments, remove lines that start with Z
STATE_LIST=`cat ${DATADIR}${ST_PARMFILE} | cut -c1-2 | grep -v '^#' | grep -v '^Z' `
echo "STATE_LIST=${STATE_LIST}" >> ${LOGNAME}

# Add quotes aound states, and add commas between states
STATE_PARTIAL_CSV=`echo $STATE_LIST | sed -e "s/ /','/g" `
echo "STATE_PARTIAL_CSV=${STATE_PARTIAL_CSV}" >> ${LOGNAME}

#Add first and last quote
STATE_IN_PHRASE="'$STATE_PARTIAL_CSV'"
echo "STATE_IN_PHRASE=${STATE_IN_PHRASE} " >> ${LOGNAME}


#############################################################
# Execute Python code to extract data.
#############################################################
echo "" >> ${LOGNAME}
echo "Start execution of PTD_Duals_Extract_Historical.py program"  >> ${LOGNAME}

# Export environment variables for Python code
export TMSTMP

export CLNDR_CY_MO_NUM_ENDDT
export CLM_SUBMSN_DT_START_DT
export CLM_SUBMSN_DT_END_DT
export STATE_IN_PHRASE
export S3_EXTRACT_FILE

${PYTHON_COMMAND} ${RUNDIR}PTD_Duals_Extract_Historical.py >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script  
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script PTD_Duals_Extract_Historical.py failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="PTD_Duals_Extract_Rerun.sh - Failed (${ENVNAME})"
		MSG="PTD Duals extract has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PTDDUAL_EMAIL_SENDER}" "${PTDDUAL_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script PTD_Duals_Extract_Historical.py completed successfully. " >> ${LOGNAME}


#############################################################
# Copy Extract file from S3 to Linux
#############################################################
echo "" >> ${LOGNAME}
echo "Copy Extract file from S3 to Linux " >> ${LOGNAME}

aws s3 cp s3://${S3BUCKET}${S3_EXTRACT_FILE} ${DATADIR}${S3_EXTRACT_FILE}  1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Copying S3 Extract file to Linux failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="PTD_Duals_Extract_Rerun.sh - Failed (${ENVNAME})"
	MSG="PTD Duals Extract copy S3 file to Linux failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PTDDUAL_EMAIL_SENDER}" "${PTDDUAL_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	


#############################################################
# gunzip extract file on Linux 
#############################################################
echo " " >> ${LOGNAME}
echo "Unzip .gz file" >> ${LOGNAME}
echo "Started --> `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}

gzip -d ${DATADIR}${S3_EXTRACT_FILE}  2>>  ${LOGNAME}
	
	
#############################################################
# Split file by state code. 
# Suppress fields for specific states.
#############################################################
echo " " >> ${LOGNAME}
echo "Split extract file into separate state files." >> ${LOGNAME}
echo "Started --> `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}

# Remove .gz from S3 filename (after gunzip)
ExtractFilename=`echo ${S3_EXTRACT_FILE} | sed -e 's/.gz//g' `
echo "ExtractFilename: ${ExtractFilename}" >> ${LOGNAME}

${RUNDIR}PTD_Duals_St_Split.awk -v outfile_model="${DATADIR}${ST_EXT_FNAME_MODEL}" ${DATADIR}${ST_PARMFILE} ${DATADIR}${ExtractFilename}  >> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "awk script PTD_Duals_St_Split.awk failed." >> ${LOGNAME}
	echo "Splitting PartD Duals file into separate files by State failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="PTD Duals Split files - Failed (${ENVNAME})"
	MSG="The PTD Duals Split files awk script has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PTDDUAL_EMAIL_SENDER}" "${PTDDUAL_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


#############################################################
# get list of State .txt files
#############################################################
echo " " >> ${LOGNAME}
echo "Get list of State .txt files" >> ${LOGNAME}

wildCardStFiles=`echo ${ST_EXT_FNAME_MODEL} | sed -e 's/XX/*/g' `

stExtractFiles=`ls ${DATADIR}${wildCardStFiles}` 2>>  ${LOGNAME}
echo ${stExtractFiles} >>  ${LOGNAME}


#############################################################
# Get record count of PTD DUALS files.
#############################################################
echo " " >> ${LOGNAME}
echo "Get record counts for state files " >> ${LOGNAME}

#REC_CNTS=`ls ${DATADIR}${wildCardStFiles} | xargs wc -l | awk '{print $2 " " $1}' | cut -d/ -f7 | xargs printf "%s %'14d\n"` 2>> ${LOGNAME}

REC_CNT_INFO=`ls ${DATADIR}${wildCardStFiles} | xargs wc -l | awk '{print $2 " " $1}' | cut -d/ -f7 ` 2>> ${LOGNAME}
REC_CNTS=`echo "${REC_CNT_INFO}" | xargs printf "%s %'14d\n" ` 2>> ${LOGNAME}

echo "REC_CNTS=${REC_CNTS} "   >> ${LOGNAME}


#############################################################
# Add "RECORD COUNT" trailer record to each state file
#############################################################
echo "" >> ${LOGNAME}
echo "Add Trailer record to each state file" >> ${LOGNAME}

for pathAndFilename in ${stExtractFiles}
do
	echo "pathAndFilename:${pathAndFilename}"  >>  ${LOGNAME}

	filename=`basename ${pathAndFilename}`
	echo "filename: ${filename}" >>  ${LOGNAME}
	
	# format record count info as 10 digit number with leading zeroes like MF file
	FILE_REC_COUNT=`echo "${REC_CNT_INFO}" |  awk -v search="${filename}" '$0 ~ search {print $2}' | xargs printf "%010d" `
	echo "FILE_REC_COUNT: ${FILE_REC_COUNT}" >> ${LOGNAME}
	
	# append Trailer record to end of State extract file
	echo "RECORD COUNT ${FILE_REC_COUNT}"   >> ${pathAndFilename}

done


#############################################################
# gzip txt files
#############################################################
echo " " >> ${LOGNAME}
echo "gzip txt files" >> ${LOGNAME}
echo "Started --> `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}

rm "${DATADIR}${wildCardStFiles}".gz 2>>  ${LOGNAME}

echo " " >> ${LOGNAME} 
		
for pathAndFilename in ${stExtractFiles}
do
	echo "gzip ${pathAndFilename}" >>  ${LOGNAME}
	# remove file before issuing gzip to avoid prompt "Do you want to overwrite existing file?"

	gzip ${pathAndFilename} 2>>  ${LOGNAME}

	RET_STATUS=$?	

	if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "creating .gz file ${pathAndFilename} failed." >> ${LOGNAME}
		
		## Send Failure email	
		SUBJECT="PTD_Duals_Extract_Rerun.sh - Failed (${ENVNAME})"
		MSG="Compressing the PTD Duals State files with gzip failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PTDDUAL_EMAIL_SENDER}" "${PTDDUAL_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
	fi

done


#############################################################
# get list of .gz files
#############################################################
echo " " >> ${LOGNAME}
echo "Get list of gz files" >> ${LOGNAME}
echo "Started --> `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}

gzFiles=`ls ${DATADIR}${wildCardStFiles}.gz`  >> ${LOGNAME}
echo "gzFiles: ${gzFiles}" >> ${LOGNAME} 


#############################################################
# put .gz files to s3
#############################################################
echo " " >> ${LOGNAME}
echo "Copy gz files to s3" >> ${LOGNAME}
echo "Started --> `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}


for pathAndFilename in ${gzFiles}
do
	echo "pathAndFilename:${pathAndFilename}"  >>  ${LOGNAME}
	filename=`basename ${pathAndFilename}`
	
	aws s3 cp ${pathAndFilename} s3://${S3BUCKET}${filename} 1>> ${LOGNAME} 

	RET_STATUS=$?	

	if [[ $RET_STATUS != 0 ]]; then
        echo " " >> ${LOGNAME}
        echo "Copying ${pathAndFilename} to s3 failed." >> ${LOGNAME}
		echo "S3 bucket: ${bucket}" >> ${LOGNAME}
		
		## Send Failure email	
		SUBJECT="PTD_Duals_Extract_Rerun.sh - Failed (${ENVNAME})"
		MSG="Copying PTD Duals State files to S3 has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PTDDUAL_EMAIL_SENDER}" "${PTDDUAL_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

       exit 12
	fi	

done


#############################################################
# Archive Full Extract file - not sent to states
#############################################################
echo "" >> ${LOGNAME}
echo "Move S3 Extract file to s3 archive directory" >> ${LOGNAME}

aws s3 mv s3://${S3BUCKET}${S3_EXTRACT_FILE} s3://${S3BUCKET}archive/${S3_EXTRACT_FILE}   >> ${LOGNAME} 2>&1

RET_STATUS=$?

if [ $RET_STATUS != 0 ]; then
        echo "" >> ${LOGNAME}
        echo "Error moving S3 Extract file t" >> ${LOGNAME}

		# Send Failure email	
		SUBJECT="Move S3 Extract file to archive directory in PTD_Duals_Extract_Rerun.sh - Failed (${ENVNAME})"
		MSG="Move S3 Extract file to archive directory in PTD_Duals_Extract_Rerun.sh has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PTDDUAL_EMAIL_SENDER}" "${PTDDUAL_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

       exit 12
fi


#############################################################
# Send Success email.
#############################################################
echo "" >> ${LOGNAME}
echo "Send success email with S3 Extract filename." >> ${LOGNAME}
echo "REC_CNTS=${REC_CNTS} "   >> ${LOGNAME}

SUBJECT="PTD Duals ${EXTRACT_TYPE} extract (${ENVNAME})" 
MSG="The Extract for the creation of the PTD Duals ${EXTRACT_TYPE} file from Snowflake has completed.\n\nThe following file(s) were created:\n\n${REC_CNTS}"

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PTDDUAL_EMAIL_SENDER}" "${PTDDUAL_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Error in calling sendEmail.py" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Sending Success email in PTD_Duals_Extract_Rerun.sh  - Failed (${ENVNAME})"
	MSG="Sending Success email in PTD_Duals_Extract_Rerun.sh  has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PTDDUAL_EMAIL_SENDER}" "${PTDDUAL_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	


#############################################################
# EFT Extract files
#############################################################
echo " " >> ${LOGNAME}
echo "EFT PTD Duals ${EXTRACT_TYPE} Extract Files " >> ${LOGNAME}
${RUNDIR}ProcessFiles2EFT.sh ${S3BUCKET}  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of extract script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script ProcessFiles2EFT.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="PTD Duals ${EXTRACT_TYPE} EFT process  - Failed (${ENVNAME})"
	MSG="PTD Duals ${EXTRACT_TYPE} EFT process has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PTDDUAL_EMAIL_SENDER}" "${PTDDUAL_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


#############################################################
# script clean-up
#############################################################
echo " " >> ${LOGNAME}
echo "Remove residual files from linux data directory." >> ${LOGNAME}
rm "${DATADIR}"PTD_DUALS_*  2>>  ${LOGNAME}



#############################################################
# End script
#############################################################
echo "" >> ${LOGNAME}
echo "PTD_Duals_Extract_Rerun.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

exit $RET_STATUS