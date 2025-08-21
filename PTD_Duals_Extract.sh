#!/usr/bin/sh
############################################################################################################
# Name:  PTD_Duals_Extract.sh
#
# Desc: PTD Duals Monthly/Daily extract 
#
# Execute as ./PTD_Duals_Extract.sh $1
#
# $1 = M/D/H --> Monthly/Daily   
#
#
# Author     : Paul Baranoski	
# Created    : 12/09/2022
#
# Modified:
#
# Paul Baranoski 2022-12-09 Created script.
# Paul Baranoski 2022-12-21 Added Daily logic for submission dates.
# Paul Baranoski 2022-12-27 Modify program to handle both PTD Duals Monthly and Daily processing.
# Paul Baranoski 2023-01-06 Add code to handle daily processing graceful exit. 
# Paul Baranoski 2023-01-18 Modify filename to remove '.' before timestamp to align with standard.
# Paul Baranoski 2023-01-31 Modify formula to calculate CLNDR_CY_MO_NUM_ENDDT for when job is run next
#                           to last day of month. 
# Paul Baranoski 2023-02-01 Add code to get record counts for state files to display in email.
# Paul Baranoski 2023-03-07 1) Remove old code to get list of files from S3 for email. (not being used) 
#                           2) Add code to add a Trailer record to each state file with the record count.  
#                           3) Updated NOF_ROWS logic to use single awk command instead of multiple commands.
# Paul Baranoski 2023-03-08 Add code to point to new S3 configuration file location.
# Paul Baranoski 2023-05-10 Move code creating date parameters before filename creation. Add code to use  
#                           parameter dates to create new variables to be used in the extract filenames.
#                           This was done to create necessary file nodes/tokens needed to create the EFT filenames. 
# Paul Baranoski 2023-05-10 Add EFT functionality.
# Paul Baranoski 2023-07-27 Modify success email message to include EFT filename mask.
# Paul Baranoski 2023-11-01 Modify date calculation for CLM_SUBMSN_DT_START_DT. Modified to get current date, and format as 1st of month 
#                           before subtracting 1 month. When its Oct 31, and you subtract 1 month, you don't get Sept 30, but Oct 1.
# Paul Baranoski 2023-12-12 Add ENVNAME variable to email subject line.
# Paul Baranoski 2024-01-19 Update display of record counts to have label "filenamesAndCounts:" to be in line with standards,
#                           and allow DashboardInfo.sh to correctly get record counts from script.
#                           Added function "ScriptSuccessfulEnd" to display proper End script log messages. Done for 
#                           DashboardInfo.sh.	
# Paul Baranoski 11/8/2024  Modify logic to get filenames and record counts to make it simpler. Add logic to use wc -lc to get record and byte counts 
#                           with DASHBOARD_INFO: label for DashboardInfo_MS.sh to get extract files, record counts, and byte counts.
# Paul Baranoski 11/9/2024  Update filename to delete from PTD_DUALS_* to PTDDUALS_*. 
# Paul Baranoski 12/3/2024  Change on 11/8/2024 accidentally broke code to get record count as last record in file. Add REC_CNT_INFO reference  
#                           back, and use as input into variable REC_CNTS. REC_CNT_INFO needs record counts without commas.
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP:=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/PTD_Duals_Extract_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

PROCESSING_TYPE=""

MONTHLY_ST_PARMFILE=PTDDualsMonthlyStParms.txt
DAILY_ST_PARMFILE=PTDDualsDailyStParms.txt
ST_PARMFILE=""

S3_EXTRACT_FILE=""
ST_EXT_FNAME_MODEL=""

SNOWFLAKE_STG=""

PTDDUAL_EMAIL_SENDER=""
PTDDUAL_EMAIL_SUCCESS_RECIPIENT=""
PTDDUAL_EMAIL_FAILURE_RECIPIENT=""

         
function ScriptSuccessfulEnd {

	echo "" >> ${LOGNAME}
	echo "PTD_Duals_Extract.sh completed successfully." >> ${LOGNAME}

	echo "Ended at `date` " >> ${LOGNAME}
	echo "" >> ${LOGNAME}

	exit 0

}


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "PTD_Duals_Extract.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}


#############################################################
# Verify that required NOF parameters have been sent 
#############################################################
if [ $# != 1 ]; then
	echo "Incorrect # of parameters sent to script. NOF parameters: $#" >> ${LOGNAME}
	exit 12
fi

PROCESSING_TYPE=$1

echo "Parameters to script: " >> ${LOGNAME}
echo "   PROCESSING_TYPE=${PROCESSING_TYPE} " >> ${LOGNAME}


#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh


#################################################################################
# Create CLNDR_CY_MO_NUM_ENDDT parameter date (YYYYMM)
#################################################################################
echo " " >> ${LOGNAME}
echo "Create CLNDR_CY_MO_NUM_ENDDT date parameter for the Python Extract program." >> ${LOGNAME}

if [ ${PROCESSING_TYPE} = "M" ]; then
	# Current Date + 2 day - 8 years -> format date 'YYYYMM'
	# +2 ensures we find a date in the next month for formula to work correctly
	CLNDR_CY_MO_NUM_ENDDT=`date -d "+2 day -8 year" +%Y%m`
else
	#Current Date - 1 month
	CLNDR_CY_MO_NUM_ENDDT=`date -d "-1 month" +%Y%m`
fi

echo "CLNDR_CY_MO_NUM_ENDDT=${CLNDR_CY_MO_NUM_ENDDT}" >> ${LOGNAME}


#################################################################################
# Create Submission Date parameters  (YYYY-MM-DD)
#################################################################################
echo " " >> ${LOGNAME}
echo "Create Claim Submission Start and End date parameters for the Python Extract program." >> ${LOGNAME}

if [ ${PROCESSING_TYPE} = "M" ]; then
	# Process for entire prior month; cur_MM - 1 day -> last day of prior month
	### --> Does not always work -> CLM_SUBMSN_DT_START_DT=`date -d "-1 month" +%Y-%m-01`
	CLM_SUBMSN_DT_START_DT=`date -d "$(date +%Y-%m-01) - 1 month" +%Y-%m-%d`
	CLM_SUBMSN_DT_END_DT=`date -d "$(date +%Y-%m-01) - 1 day" +%Y-%m-%d`
	
	#CLM_SUBMSN_DT_START_DT=2023-09-01
	#CLM_SUBMSN_DT_END_DT=2023-09-30
	
	# create variables used in filename
	YYYY=`echo ${CLM_SUBMSN_DT_END_DT} | cut -c1-4 `
	MM=`echo ${CLM_SUBMSN_DT_END_DT} | cut -c6-7 `
else
	ONE_DAY_AGO=`date -d "-1 day" +%Y-%m-%d`
	THREE_DAYS_AGO=`date -d "-3 day" +%Y-%m-%d`
	DOW=`date +%A`

	# Daily CDS.CLM_SUBMSN_DT filter --> Prior day for Tue-Fri; (Fri, Sat, Sun) for Mon run.
	CLM_SUBMSN_DT_END_DT=${ONE_DAY_AGO}
	if [ ${DOW} = "Monday" ]; then
		CLM_SUBMSN_DT_START_DT=${THREE_DAYS_AGO}
	else
		CLM_SUBMSN_DT_START_DT=${ONE_DAY_AGO}
	fi

	# create variables used in filename
	YYMMDD=`echo ${CLM_SUBMSN_DT_END_DT} | cut -c3-4,6-7,9-10 `
fi

#CLM_SUBMSN_DT_START_DT=2023-09-01
#CLM_SUBMSN_DT_END_DT=2023-09-30
#CLNDR_CY_MO_NUM_ENDDT=202212
	
echo "CLM_SUBMSN_DT_START_DT=${CLM_SUBMSN_DT_START_DT}" >> ${LOGNAME}
echo "CLM_SUBMSN_DT_END_DT=${CLM_SUBMSN_DT_END_DT}" >> ${LOGNAME}


#############################################################
# Set variables to appropriate values for monthly/daily processing.
#############################################################
S3CONFIG_BUCKET=${bucket}config/

if [ ${PROCESSING_TYPE} = "M" ]; then
	echo " " >> ${LOGNAME}
	echo "Monthly processing started. " >> ${LOGNAME}
	echo " " >> ${LOGNAME}
	
	EXTRACT_TYPE="Monthly"
	SNOWFLAKE_STG="PTDDUALMNTH_STG"

	S3BUCKET=${PTDDUALMNTH_BUCKET} 
	ST_PARMFILE=${MONTHLY_ST_PARMFILE}
	
	ST_EXT_FNAME_MODEL=PTDDUALS_MONTHLY_XX_Y${YYYY}M${MM}_${TMSTMP}.txt
	S3_EXTRACT_FILE=PTDDUALS_MONTHLY_Y${YYYY}M${MM}_${TMSTMP}.csv.gz
	
	PTDDUAL_EMAIL_SENDER=${PTDDUALMNTH_EMAIL_SENDER}
	PTDDUAL_EMAIL_SUCCESS_RECIPIENT=${PTDDUALMNTH_EMAIL_SUCCESS_RECIPIENT}
	PTDDUAL_EMAIL_FAILURE_RECIPIENT=${PTDDUALMNTH_EMAIL_FAILURE_RECIPIENT}

	EFT_FILEMASK=P#EFT.ON.G{ST}.IDRPD.Y${YYYY}M${MM}.{TIMESTAMP}

else
	echo " " >> ${LOGNAME}
	echo "Daily processing started. " >> ${LOGNAME}
	echo " " >> ${LOGNAME}

	EXTRACT_TYPE="Daily"
	SNOWFLAKE_STG="PTDDUALDLY_STG"
		
	S3BUCKET=${PTDDUALDAILY_BUCKET} 
	ST_PARMFILE=${DAILY_ST_PARMFILE}

	ST_EXT_FNAME_MODEL=PTDDUALS_DAILY_XX_R${YYMMDD}_${TMSTMP}.txt
	S3_EXTRACT_FILE=PTDDUALS_DAILY_R${YYMMDD}_${TMSTMP}.csv.gz
	
	PTDDUAL_EMAIL_SENDER=${PTDDUALDAILY_EMAIL_SENDER}
	PTDDUAL_EMAIL_SUCCESS_RECIPIENT=${PTDDUALDAILY_EMAIL_SUCCESS_RECIPIENT}	
	PTDDUAL_EMAIL_FAILURE_RECIPIENT=${PTDDUALDAILY_EMAIL_FAILURE_RECIPIENT}

	EFT_FILEMASK=P#EFT.ON.G{ST}.IDRPD.R${YYMMDD}.{TIMESTAMP}

fi

echo "S3 config bucket=${CONFIG_BUCKET}" >> ${LOGNAME}
echo "PTD_Duals bucket=${S3BUCKET}" >> ${LOGNAME}
echo "State Parameter File=${ST_PARMFILE}" >> ${LOGNAME}
echo "State Extract Model filename=${ST_EXT_FNAME_MODEL}"  >> ${LOGNAME}
echo "S3 Extract filename=${S3_EXTRACT_FILE}"  >> ${LOGNAME}

echo "PTDDUAL_EMAIL_SENDER=${PTDDUAL_EMAIL_SENDER}"  >> ${LOGNAME}
echo "PTDDUAL_EMAIL_SUCCESS_RECIPIENT=${PTDDUAL_EMAIL_SUCCESS_RECIPIENT}"  >> ${LOGNAME}
echo "PTDDUAL_EMAIL_FAILURE_RECIPIENT=${PTDDUAL_EMAIL_FAILURE_RECIPIENT}"  >> ${LOGNAME}


#################################################################################
# Remove residual duals linux files
#################################################################################
echo " " >> ${LOGNAME}
echo "Remove residual PTDDUALS_* files on linux data directory." >> ${LOGNAME}

rm "${DATADIR}"PTDDUALS_* 2>>  ${LOGNAME}


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
	SUBJECT="PTD Duals Extract - Failed (${ENVNAME})"
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
echo "Start execution of PTD_Duals_Extract.py program"  >> ${LOGNAME}

# Export environment variables for Python code
export TMSTMP
export SNOWFLAKE_STG
export S3_EXTRACT_FILE
export CLNDR_CY_MO_NUM_ENDDT
export CLM_SUBMSN_DT_START_DT
export CLM_SUBMSN_DT_END_DT
export STATE_IN_PHRASE

${PYTHON_COMMAND} ${RUNDIR}PTD_Duals_Extract.py >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script  
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script PTD_Duals_extract.py failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="PTD_Duals_extract.sh  - Failed (${ENVNAME})"
		MSG="PTD Duals extract has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PTDDUAL_EMAIL_SENDER}" "${PTDDUAL_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script PTD_Duals_extract.py completed successfully. " >> ${LOGNAME}


#############################################################
# Were zero rows extracted?
#############################################################
echo "" >> ${LOGNAME}
echo "Find out NOF Rows extracted from SQL. " >> ${LOGNAME}

NOF_ROWS=`awk -F "," '/rows_unloaded/{getline;print $1}' ${LOGNAME} `
echo "NOF_ROWS=${NOF_ROWS}" >> ${LOGNAME}


#############################################################
# Daily processing doesn't always have data to extract
# --> end gracefully if there is no data to extract.
# Monthly processing should extract data
# --> no extracted data should be "hard error"
#############################################################
if [ ${NOF_ROWS} == 0 ]; then
	if [ ${PROCESSING_TYPE} = "D" ]; then

		echo "" >> ${LOGNAME}
		echo "Python script PTD_Duals_extract.py - No data available" >> ${LOGNAME}

		# Send No data available email	
		SUBJECT="PTD Duals ${EXTRACT_TYPE} Extract  - No data available. (${ENVNAME})"
		MSG="PTD Duals ${EXTRACT_TYPE} Extract  - No data available."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PTDDUAL_EMAIL_SENDER}" "${PTDDUAL_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		ScriptSuccessfulEnd

	else
		echo "" >> ${LOGNAME}
		echo "Python script PTD_Duals_extract.py failed - No data extracted" >> ${LOGNAME}

		# Send No data extracted email	
		SUBJECT="PTD Duals Extract - Failed (${ENVNAME})"
		MSG="PTD Duals extract failed. No data extracted."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PTDDUAL_EMAIL_SENDER}" "${PTDDUAL_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12	
	
	fi
fi	


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
	SUBJECT="PTD Duals Extract - Failed (${ENVNAME})"
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
echo "Change working directory to ${DATADIR} "    >> ${LOGNAME}

cd ${DATADIR}  2>> ${LOGNAME}
echo "pwd:" `pwd`   >> ${LOGNAME}

echo " " >> ${LOGNAME}
echo "Get record counts for state files " >> ${LOGNAME}

REC_CNT_INFO=`wc -l ${wildCardStFiles} | grep -v 'total' | awk '{print $2,$1}' `
REC_CNTS=`echo ${REC_CNT_INFO} | xargs printf "%s %'14d\n"  ` 2>> ${LOGNAME}

echo "filenamesAndCounts: ${REC_CNTS} "   >> ${LOGNAME}


# New way for DashboardInfo.sh to get filenames, record counts, and byte counts
DASHBOARD_INFO=`wc -lc ${wildCardStFiles} | grep -v 'total' | awk '{print $3,$1,$2}' | xargs printf "DASHBOARD_INFO:%s %s %s \n" `  2>> ${LOGNAME}

# print DASHBOARD Info to log 	
echo ""  >> ${LOGNAME}
echo "${DASHBOARD_INFO}" >> ${LOGNAME}

echo " " >> ${LOGNAME}
echo "Change working directory to ${RUNDIR} "    >> ${LOGNAME}

cd ${RUNDIR}  2>> ${LOGNAME}
echo "pwd:" `pwd`   >> ${LOGNAME}


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
		SUBJECT="PTD Duals Extract - Failed (${ENVNAME})"
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
		SUBJECT="PTD Duals State Files - Failed (${ENVNAME})"
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
		SUBJECT="Move S3 Extract file to archive directory in PTD_Duals_Extract.sh - Failed (${ENVNAME})"
		MSG="Move S3 Extract file to archive directory in PTD_Duals_Extract.sh has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PTDDUAL_EMAIL_SENDER}" "${PTDDUAL_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

       exit 12
fi


#############################################################
# Send Success email.
#############################################################
echo "" >> ${LOGNAME}
echo "Send success email with S3 Extract filename." >> ${LOGNAME}
echo "REC_CNTS=${REC_CNTS} "   >> ${LOGNAME}

SUBJECT="PartD Duals ${EXTRACT_TYPE} extract (${ENVNAME})" 
MSG="The Extract for the creation of the PartD Duals ${EXTRACT_TYPE} file(s) from Snowflake has completed.\n\nEFT versions of the below files were created using the following file mask ${EFT_FILEMASK}.\n\nThe following file(s) were created:\n\n${REC_CNTS}"

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PTDDUAL_EMAIL_SENDER}" "${PTDDUAL_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Error in calling sendEmail.py" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Sending Success email in PTD_Duals_Extract.sh  - Failed (${ENVNAME})"
	MSG="Sending Success email in PTD_Duals_Extract.sh  has failed."
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
rm "${DATADIR}"PTDDUALS_*  2>>  ${LOGNAME}


#############################################################
# End script
#############################################################
echo "" >> ${LOGNAME}
echo "PTD_Duals_Extract.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

exit $RET_STATUS