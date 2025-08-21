#!/usr/bin/bash
#
######################################################################################
# Name:  DSH_Extracts.sh
#
# Desc: Create DSH Extract file for each request record in a request file.
#
#
# Created: Paul Baranoski  04/23/2024
# Modified: 
#
# Paul Baranoski 2024-04-23 Create script.
# Paul Baranoski 2024-06-12 Change recipient email contants to DSH_EMAIL_FAILURE_RECIPIENT
#                           for Finder file errors.
# Paul Baranoski 2024-06-12 Corrected error email messages.
# Paul Baranoski 2024-06-25 Unnecessary change to trigger GitHub.
# Paul Baranoski 2024-06-28 Remove dashes in PRVDR_ID. (They shouldn't be there).
# Paul Baranoski 2024-07-10 Remove overide destination for manifest file. We want manifest files to be released automatically. 
# Paul Baranoski 2024-07-13 Add code to remove non-display binary characters from file. Add BCC to email message with invalid email address.
# Paul Baranoski 2024-07-15 Modify code calculating FY values from requested dates. 
#                           Add code to verify filename is in correct format (no double underscores).
#                           Make email addresses edit better. No spaces or brackets in email address. 
# Paul Baranoski 2024-07-16 Added code to bypass creation of manifest file, when no extract files were created due to no data for request.
#                           Also, added filename format to invalid filename error message.  
#                           Added newline character at end of request file. Some request files created in Windows were missing the end-of-record marker
#                           which prevented the last record to be processed. 
# Paul Baranoski 2024-07-18 Modified and improved code to bypass creation of manifest file when there were no records found for any of the requested extracts.
# Paul Baranoski 2024-08-15 Added Error handling for SendEmail calls.
#                           Also, added phrase "Ended at " when there are no finder files to process so that the Dashboard extract will see that script/job
#                           completed successfully.  
# Paul Baranoski 2024-09-20 Modified error message: changed "Too many fields" to "Incorrect NOF fields". 
# Paul Baranoski 2024-10-15 1) When getting Files2Process from S3, if request filename contains spaces, the "awk $4" will only get part of the filename. Added $5 and $6 fields 
#                           to awk command logic to get full filename when it contains spaces.
#                           2) Change IFS to "newline only" before for-loop to properly process request filenames which could contain spaces.
#                           3) Add double quotes around ${FF_EXT} references since request filename can contain spaces.
#                           4) in archiveRequestFile function, add double quotes around S3 filenames in s3 mv command since request filenames may contain spaces.
#                           5) Modify invalid filename error message to include what is allowed for {UNIQ-ID).  
# Paul Baranoski 2025-04-08 Add lower case command when extracting extension so that .CSV is the same as .csv in edit of request file extension. Add {csv|CSV) to egrep
#                           egrep regular expression.
# Paul Baranoski 2025-04-25 Modify egrep edit for request filename to use "\." instead of "." since a single period can represent any character where by the "\." is looking 
#                           for a literal period (like .csv). 
#                           Modify egrep email edit to allow a dash in email before and after the '@'. 
# Paul Baranoski 2025-05-08 Add call to DSH_AddReqEmails.py to capture DSH Requestor-UNIQ-ID and Requestor-Email into SF table.
# Paul Baranoski 2025-08-13 Modify success email verbiage to say request is in-process and not complete, and files will be available once they receive an email with a link to their Box account.
######################################################################################
set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`

# Export TMSTMP variable for child scripts
export TMSTMP 

LOGNAME=/app/IDRC/XTR/CMS/logs/DSH_Extracts_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "DSH_Extracts.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh >> ${LOGNAME}

LOGDIR=${LOG_PATH}/

S3BUCKET=${DSH_BUCKET} 

# DSH_REQUEST_{Sender}_DYYYYMMDD.csv	
PREFIX=DSH_REQUEST_

echo "DSH bucket=${S3BUCKET}" >> ${LOGNAME}
echo "Finder file bucket=${FINDER_FILE_BUCKET}" >> ${LOGNAME}

source ${RUNDIR}FilenameCounts.bash

# Variables for extracting logfile entries
LOG_FROM_LINE=1
LOG_TO_LINE=1
TMP_DSH_FF_LOGFILE=tmpDSHFFLOG.txt

BAD_FILE_SW=N

function archiveRequestFile() { 

	#############################################################
	# Move Finder File in S3 to archive folder
	#############################################################
	echo " " >> ${LOGNAME}
	echo "Moving S3 DSH Finder file ${FF} to S3 archive folder." >> ${LOGNAME}
	
	aws s3 mv "s3://${FINDER_FILE_BUCKET}${FF}" "s3://${FINDER_FILE_BUCKET}archive/${FF}"  1>> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Moving S3 DSH Finder file to S3 archive folder failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="DSH Extract - Failed ($ENVNAME)"
		MSG="Moving S3 Finder file to S3 archive folder failed.  ( ${FINDER_FILE_BUCKET}${S3Filename} to ${FINDER_FILE_BUCKET}archive/${S3Filename} )"
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi	

	
	#############################################################
	# Delete Finder File in Linux
	#############################################################
	echo "" >> ${LOGNAME}
	echo "Delete finder file ${DATADIR}${FF} from linux data directory." >> ${LOGNAME}
	rm ${DATADIR}${FF} 2>> ${LOGNAME}

}

function getNOFFILES4ManifestFile() { 

	#############################################################
	# Get list of S3 files to include in manifest.
	#############################################################
	echo "" >> ${LOGNAME}
	echo "Count NOF extract files to include in the manifest file " >> ${LOGNAME}

	NOF_FILES_4_MANIFEST=`aws s3 ls s3://${S3BUCKET} | grep ${FF_TMSTMP} | wc -l `

	RET_STATUS=$?

	if [ $RET_STATUS != 0 ]; then
		echo "" >> ${LOGNAME}
		echo "Error in getting count of extract files to include in manifest file. S3 Bucket ${S3BucketAndFldr} " >> ${LOGNAME}

		exit 12
	fi

	echo "NOF_FILES_4_MANIFEST=${NOF_FILES_4_MANIFEST}" >> ${LOGNAME}
	
}


#################################################################################
# Are there any DSH Extract/Finder files in S3?
#################################################################################
echo "" >> ${LOGNAME}
echo "Count NOF DSH Request/Finder files found in ${FINDER_FILE_BUCKET}" >> ${LOGNAME}

NOF_FILES=`aws s3 ls s3://${FINDER_FILE_BUCKET}${PREFIX} --summarize | grep 'Total Objects' | cut -d: -f2 | sed 's/ //g' ` 2>> ${LOGNAME}

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Counting NOF S3 DSH Finder files in s3://${FINDER_FILE_BUCKET}${PREFIX} failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="DSH Extract - Failed ($ENVNAME)"
	MSG="Counting NOF S3 DSH Finder files in s3://${FINDER_FILE_BUCKET}${PREFIX} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	

echo "NOF_FILES=${NOF_FILES}"  >> ${LOGNAME}


#################################################
# If 0 finder files --> end gracefully		
#################################################
if [ ${NOF_FILES} -eq 0 ];then 
	echo "" >> ${LOGNAME}
	echo "There are no S3 DSH Finder files to process in s3://${FINDER_FILE_BUCKET}${PREFIX}." >> ${LOGNAME}
	
	# Send Info email	
	SUBJECT="DSH Extract ended - nothing to process ($ENVNAME)"
	MSG="There are no S3 DSH Finder files to process in s3://${FINDER_FILE_BUCKET}${PREFIX}."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	
	echo "" >> ${LOGNAME}
	echo "DSH_Extracts.sh completed successfully." >> ${LOGNAME}

	echo "Ended at `date` " >> ${LOGNAME}
	echo "" >> ${LOGNAME}

	exit 0

fi 


#################################################
# Get list of S3 DSH Extract/Finder Files.		
#################################################
echo "" >> ${LOGNAME}
echo "Get list of DSH Finder Files in S3 " >> ${LOGNAME}

########################################################################################################################################
# NOTE: awk default delimiter is spaces; awk counts contiguous occurrences of a delimiter as a single delimiter.  
#       aws s3 ls command will return --> Ex. "2024-10-15 10:24:44       1783 DSH_REQUEST_Alfred Aghajani_20170930.csv", and 
#       we want field 4 which is the filename. When the filename contains spaces, awk will see $4='DSH_REQUEST_Alfred' and $5='Aghajani_20170930.csv' 
#       So, to capture the full filename when it contains spaces, we need to capture all the possible fields. I've coded for 6. This is 
#       two non-contiguous spaces in the filename (different locations).
#
# NOTE: awk NF = NOF fields 
#       if filename has no spaces, NF==4. if filename contains one space, NF==5. if filename contains 2 spaces, NF==6.
#       To capture entire filename when it contains spaces, need to account for how many fields awk sees, and "print" those fields.
#       The comma between awk variable fields (Ex. $4) in print command maintains a space between the variables, while spaces between the awk variables 
#       in print command does not include any space.
########################################################################################################################################
Files2Process=`aws s3 ls s3://${FINDER_FILE_BUCKET}${PREFIX} | awk {'if (NF == 4) {print $4} else if (NF == 5) {print $4,$5} else if (NF == 6) {print $4,$5,$6 } }' ` 2>> ${LOGNAME}

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Counting NOF S3 DSH Finder files in s3://${FINDER_FILE_BUCKET}${PREFIX} failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="DSH Extract - Failed ($ENVNAME)"
	MSG="Counting NOF S3 DSH Finder files in s3://${FINDER_FILE_BUCKET}${PREFIX} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	

echo "Files2Process=${Files2Process}"  >> ${LOGNAME}
		
		
#################################################################################
# Loop thru DSH Extract/Finder files in data directory.
#################################################################################
echo "" >> ${LOGNAME}

# Save default IFS setting
save_IFS=$IFS

# Set IFS to newline only so that it will properly read filenames that contain spaces. IFS normally is spaces, tabs, and newlines.
IFS=$'\n'

for FF in ${Files2Process}
do

	echo "" >> ${LOGNAME}
	echo "******************************" >> ${LOGNAME}
	echo "Processing ${FF}" >> ${LOGNAME}

 	#################################################
	# Create separate timestamp for all files created from a FF/request file
 	#################################################
	FF_TMSTMP=`date +%Y%m%d.%H%M%S`

 	#################################################
	# Re-set BAD_FILE_SW
 	#################################################
	BAD_FILE_SW=N

 	#################################################
	# Verify that file extension is a .csv file	
	#################################################
	 
	FF_EXT=`echo "${FF}" | cut -d. -f2 |  tr '[A-Z]' '[a-z]' `
	echo "FF_EXT=${FF_EXT}"  >> ${LOGNAME}
	
	if [ "${FF_EXT}" != "csv" ];then
		echo "" >> ${LOGNAME}
		echo "Request file ${FF} has incorrect file extension. File cannot be processed. " >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="DSH Extract - Failed ($ENVNAME)"
		MSG="Request file ${FF} has incorrect file extension. File cannot be processed. Please correct and re-submit file as csv file."
		
		echo "DSH_EMAIL_BCC=${DSH_EMAIL_BCC}" >> ${LOGNAME}
		echo "DSH_EMAIL_REPLY_MSG=${DSH_EMAIL_REPLY_MSG}"  >> ${LOGNAME}
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${DSH_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" "${DSH_EMAIL_BCC}" "${DSH_EMAIL_REPLY_MSG}" >> ${LOGNAME} 2>&1

		# migrate finder file to archive folder
		archiveRequestFile
		
		# process next Finder File
		continue
		
	fi	

 	#################################################
	# Verify Request filename matches expected format
	# NOTE: 1) No double underscores.
	#       2) Unique ID contains no spaces or special character except dash
 	#################################################
	VALID_FILE_FORMAT=`echo "${FF}" | egrep -c '^DSH_REQUEST_[a-zA-Z0-9-]+_[0-9]+\.(csv|CSV)$' ` 
	
	
	if [ ${VALID_FILE_FORMAT} -eq 0 ];then
		echo "" >> ${LOGNAME}
		echo "Request file ${FF} is named incorrectly. " >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="DSH Extract - Failed ($ENVNAME)"
		MSG="Request file ${FF} is named incorrectly. Please ensure that filename follows this pattern: DSH_REQUEST_{UNIQ-ID}_YYYYMMDD.csv. {UNIQ-ID} can only contain letters, numbers, and dash. Please correct and re-submit file with proper filename."
		
		echo "DSH_EMAIL_BCC=${DSH_EMAIL_BCC}" >> ${LOGNAME}
		echo "DSH_EMAIL_REPLY_MSG=${DSH_EMAIL_REPLY_MSG}"  >> ${LOGNAME}
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${DSH_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" "${DSH_EMAIL_BCC}" "${DSH_EMAIL_REPLY_MSG}" >> ${LOGNAME} 2>&1

		# migrate finder file to archive folder
		archiveRequestFile
		
		# process next Finder File
		continue
		
	fi
	
 	#################################################
	# Extract FF ID NODE to use for extract files
 	#################################################
	FF_ID_NODE=`echo ${FF} | cut -d_ -f3 ` 	2>> ${LOGNAME}
	echo "FF_ID_NODE=${FF_ID_NODE}"  >> ${LOGNAME}
	
 	#################################################
	# Save logfile start line num for current FF
    # NOTE: This is used to extract filenames and 
	#       record counts for respective success emails.	
	#################################################
	echo "" >> ${LOGNAME}
	
	LOG_FROM_LINE=`wc -l ${LOGNAME} | awk '{print $1}' `  2>> ${LOGNAME}
	echo "LOG_FROM_LINE=${LOG_FROM_LINE}" >> ${LOGNAME}

	
	#################################################
	# Copy DSH Extract/Finder File to linux.		
	#################################################
	echo "" >> ${LOGNAME}
	echo "Copy S3 Finder File s3://${FINDER_FILE_BUCKET}${FF} to linux " >> ${LOGNAME}

	aws s3 cp s3://${FINDER_FILE_BUCKET}${FF} ${DATADIR}${FF}  1>> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Copying S3 Finder File s3://${FINDER_FILE_BUCKET}${FF} to ${DATADIR}${FF} failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="DSH Extract - Failed ($ENVNAME)"
		MSG="Copying S3 Finder File s3://${FINDER_FILE_BUCKET}${FF} to linux datadir failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi	

	##############################################
	# Cleansing of request file
	##############################################
	# Perform sed on file to remove carrier returns
	sed -i 's/\r//g' ${DATADIR}${FF} 2>> ${LOGNAME}

	# Perform sed to remove any non-display characters, non-UTF characters. Thanks Monica!
	LC_ALL=C sed -i 's/[\x80-\xff]//g' ${DATADIR}${FF} 2>> ${LOGNAME}

	# Add ending newline character in case its missing for last record. (Occurs with files created in Windows). 
	printf "\n" >>  ${DATADIR}${FF}	
	
	##############################################
	# Process each record in Finder File
	##############################################
	while read ExtRecord
	do

		echo "" >> ${LOGNAME}
		echo "----------------------------" >> ${LOGNAME}
		echo "ExtRecord: ${ExtRecord}" >> ${LOGNAME}
		
		# skip blank lines - zero length
		if [ -z "${ExtRecord}" ];then
			continue
		fi

		# skip "blank lines" containing only spaces and commas 
        testExtRec=`echo "${ExtRecord}" | sed 's/ //g' | sed 's/,//g' `	
		if [ "${testExtRec}" = "" ];then
			echo "Skip blank record"  >> ${LOGNAME}
			continue
		fi
		
		# skip comment lines
		FIRST_CHAR=`echo "${ExtRecord}" | cut -c1 ` 
		echo "FIRST_CHAR: ${FIRST_CHAR}"  >> ${LOGNAME}
		
		if [ "${FIRST_CHAR}" = "#" ];then
			echo "Skip comment records"  >> ${LOGNAME}
			continue
		fi

		# skip Header record
		FIRST_HDR_COL=`echo "${ExtRecord}" | cut -c1-5` 
		echo "FIRST_HDR_COL: ${FIRST_HDR_COL}"  >> ${LOGNAME}
		if [ "${FIRST_HDR_COL}" = "PRVDR" ];then
			echo "Skip header record"  >> ${LOGNAME}
			continue
		fi
		
		##############################################
		# NOF fields not correct for record?
		# --> Reject file
		##############################################
		NOF_FLDS=`echo "${ExtRecord}" | awk -F, '{print NF}'  `  2>> ${LOGNAME}
		
		if [ ${NOF_FLDS} -ne 4 ];then
			echo "" >> ${LOGNAME}
			echo "Request file ${FF} has incorrectly formatted records. Incorrect number of fields ${NOF_FLDS} found instead of 4. " >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="Request file ${FF} has incorrectly formatted records. ($ENVNAME)"
			MSG="Request file ${FF} has incorrectly formatted records. Incorrect NOF fields ${NOF_FLDS} instead of 4. Request file has been rejected. Please correct and re-submit file."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${DSH_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" "${DSH_EMAIL_BCC}" "${DSH_EMAIL_REPLY_MSG}" >> ${LOGNAME} 2>&1

			# Set BAD_FILE_SW to True
			BAD_FILE_SW=Y

			# Exit loop to process records in file
			break
			
		fi

		##############################################
		# Extract DSH record fields; remove trailing spaces
		##############################################
		echo "Extract record fields" >> ${LOGNAME}
		
		PRVDR_ID_ON_REC=`echo "${ExtRecord}" | cut -d, -f1 | sed 's/ $//g'`  2>> ${LOGNAME}
		echo "PRVDR_ID_ON_REC: ${PRVDR_ID_ON_REC}" >> ${LOGNAME}

		# Remove any dashes in PRVDR_ID
		PRVDR_ID=`echo "${PRVDR_ID_ON_REC}" | sed 's/-//g'`  2>> ${LOGNAME}
		echo "PRVDR_ID: ${PRVDR_ID}" >> ${LOGNAME}
	
		FROM_FY_DT=`echo "${ExtRecord}" | cut -d, -f2 | sed 's/ $//g'`  2>> ${LOGNAME}
		echo "FROM_FY_DT: ${FROM_FY_DT}" >> ${LOGNAME}

		TO_FY_DT=`echo "${ExtRecord}" | cut -d, -f3 | sed 's/ $//g'`  2>> ${LOGNAME}
		echo "TO_FY_DT: ${TO_FY_DT}" >> ${LOGNAME}

		REQSTR_EMAIL=`echo "${ExtRecord}" | cut -d, -f4 | sed 's/ $//g'`  2>> ${LOGNAME}
		echo "REQSTR_EMAIL: ${REQSTR_EMAIL}" >> ${LOGNAME}

		
		##############################################
		# Validate EMAIL Address in record
		##############################################
		echo "Validate email address" >> ${LOGNAME}
				
		if [ "${REQSTR_EMAIL}" = "" ];then
			echo "" >> ${LOGNAME}
			echo "Request file ${FF} has blank/empty email address. " >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="Request file ${FF} has blank/empty email address. ($ENVNAME)"
			MSG="Request file ${FF} has blank/empty email address. File cannot be processed. Please correct and re-submit file."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${DSH_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" "${DSH_EMAIL_BCC}" "${DSH_EMAIL_REPLY_MSG}" >> ${LOGNAME} 2>&1

			# Set BAD_FILE_SW to True
			BAD_FILE_SW=Y

			# Exit loop to process records in file
			break	
		else
			# Make sure there are one or more valid characters before and after the at-sign
			VALID_EMAIL=`echo "${REQSTR_EMAIL}"  | egrep "^[a-zA-Z0-9_\.-]+@[a-zA-Z0-9\.-]+$" `  2>> ${LOGNAME}
			if [ "${VALID_EMAIL}" = ""  ];then
				echo "" >> ${LOGNAME}
				echo "Request file ${FF} has invalid email address: ${REQSTR_EMAIL} " >> ${LOGNAME}
				
				# Send Failure email	
				SUBJECT="Request file ${FF} has invalid email address. ($ENVNAME). "
				MSG="Request file ${FF} has invalid email address: ${REQSTR_EMAIL}. File cannot be processed. Please correct and re-submit file."
				${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${DSH_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" "${DSH_EMAIL_BCC}" "${DSH_EMAIL_REPLY_MSG}" >> ${LOGNAME} 2>&1	

				# Set BAD_FILE_SW to True
				BAD_FILE_SW=Y

				# Exit loop to process records in file
				break	
			
			else
				# Set the email recipients who will receive the request emails
				RQST_EMAIL_RECIPIENT="${DSH_EMAIL_SUCCESS_RECIPIENT},${REQSTR_EMAIL}"
				echo "RQST_EMAIL_RECIPIENT=${RQST_EMAIL_RECIPIENT}"  >> ${LOGNAME}			
			fi
		fi


		##############################################
		# Is FROM_FY_DT a valid date
		# NOTE: Invalid date will set RC to non-zero.
		##############################################
		echo "Validate From FY Date" >> ${LOGNAME}
				
		if [ "${FROM_FY_DT}" = "" ];then
			echo "" >> ${LOGNAME}
			echo "Incorrectly formatted record found. 'From FY Date' is blank/empty" >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="Request file ${FF} has incorrectly formatted records. ($ENVNAME)"
			MSG="Incorrectly formatted record found. 'From FY Date' is blank/empty. Request file cannot be processed. Please correct and re-submit file."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${RQST_EMAIL_RECIPIENT}" "${SUBJECT}" "${MSG}" "${DSH_EMAIL_BCC}" "${DSH_EMAIL_REPLY_MSG}" >> ${LOGNAME} 2>&1

			# Set BAD_FILE_SW to True
			BAD_FILE_SW=Y

			# Exit loop to process records in file
			break
			
		else

			date -d "${FROM_FY_DT}" >> ${LOGNAME}  2>&1

			if [ $? != 0 ]; then
				echo "" >> ${LOGNAME}
				echo "Incorrectly formatted record found. Invalid date for 'From FY Date': ${FROM_FY_DT}" >> ${LOGNAME}
				
				# Send Failure email	
				SUBJECT="Request file ${FF} has incorrectly formatted records. ($ENVNAME)"
				MSG="Incorrectly formatted record found. Invalid date for 'From FY Date': ${FROM_FY_DT}. Request file cannot be processed. Please correct and re-submit file."
				${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${RQST_EMAIL_RECIPIENT}" "${SUBJECT}" "${MSG}" "${DSH_EMAIL_BCC}" "${DSH_EMAIL_REPLY_MSG}" >> ${LOGNAME} 2>&1

				# Set BAD_FILE_SW to True
				BAD_FILE_SW=Y

				# Exit loop to process records in file
				break
			fi	
		fi

		##############################################
		# Is TO_FY_DT a valid date
		##############################################
		echo "Validate To FY Date" >> ${LOGNAME}

		if [ "${TO_FY_DT}" = "" ];then
			echo "" >> ${LOGNAME}
			echo "Incorrectly formatted record found. 'To FY Date' is blank/empty" >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="Request file ${FF} has incorrectly formatted records. ($ENVNAME)"
			MSG="Incorrectly formatted record found. 'To FY Date' is blank/empty. Request file cannot be processed. Please correct and re-submit file."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${RQST_EMAIL_RECIPIENT}" "${SUBJECT}" "${MSG}" "${DSH_EMAIL_BCC}" "${DSH_EMAIL_REPLY_MSG}" >> ${LOGNAME} 2>&1

			# Set BAD_FILE_SW to True
			BAD_FILE_SW=Y

			# Exit loop to process records in file
			break
			
		else
		
			date -d "${TO_FY_DT}"  >> ${LOGNAME}  2>&1

			if [ $? != 0 ]; then
				echo "" >> ${LOGNAME}
				echo "Incorrectly formatted record found. Invalid date for 'To FY Date': ${TO_FY_DT}" >> ${LOGNAME}
				
				# Send Failure email	
				SUBJECT="Request file ${FF} has incorrectly formatted records. ($ENVNAME)"
				MSG="Incorrectly formatted record found. Invalid date for 'To FY Date': ${TO_FY_DT}. Request file cannot be processed. Please correct and re-submit file."
				${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${RQST_EMAIL_RECIPIENT}" "${SUBJECT}" "${MSG}" "${DSH_EMAIL_BCC}" "${DSH_EMAIL_REPLY_MSG}" >> ${LOGNAME} 2>&1

				# Set BAD_FILE_SW to True
				BAD_FILE_SW=Y

				# Exit loop to process records in file
				break
				
			fi	
		fi


		##############################################
		# Convert FROM DT to DSH FY.
		##############################################
		echo "Convert FROM DT TO DSH FY YYYY" >> ${LOGNAME}

		MM=`date "+%m" -d "${FROM_FY_DT}"`
		if [ ${MM} -ge 10 ];then
			FROM_FY=`date "+%Y" -d "${FROM_FY_DT} +1 year" `		
		else
			FROM_FY=`date "+%Y" -d "${FROM_FY_DT}" `		
		fi

		echo "FROM_FY=${FROM_FY}" >> ${LOGNAME}	

		##############################################
		# Convert TO DT to DSH FY.
		##############################################
		echo "Convert TO DT TO DSH FY YYYY" >> ${LOGNAME}

		MM=`date "+%m" -d "${TO_FY_DT}"`
		if [ ${MM} -ge 10 ];then
			TO_FY=`date "+%Y" -d "${TO_FY_DT} +1 year" `		
		else
			TO_FY=`date "+%Y" -d "${TO_FY_DT}" `		
		fi

		echo "TO_FY=${TO_FY}" >> ${LOGNAME}	
		
		
		##############################################
		# Export fields for python extract code.
		##############################################		
		export PRVDR_ID
		export FROM_FY
		export TO_FY
		export FF_TMSTMP
		export FF_ID_NODE


		##############################################
		# Extract DSH records for Extract record.
		##############################################
		echo " " >> ${LOGNAME}
		echo "Extract DSH data for Provider ${PRVDR_ID} for Extract Dates ${FROM_FY} to ${TO_FY} " >> ${LOGNAME}
		${RUNDIR}DSH_Extracts.py  >> ${LOGNAME} 2>&1


		#############################################################
		# Check the status of extract script
		#############################################################
		RET_STATUS=$?

		if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Shell script DSH_Extracts.py failed." >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="Python script DSH_Extracts.py - Failed ($ENVNAME)"
			MSG="Python script DSH_Extracts.py  has failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
		fi

	done  <  ${DATADIR}${FF}


	#################################################
	# If not bad file --> continue success processing		
	#################################################
	if [ ${BAD_FILE_SW} = "N" ]; then


		#################################################
		# Save logfile end line num for current FF		
		#################################################
		echo " " >> ${LOGNAME}
		echo "select range of lines in log file to search for extract filenames and counts.  " >> ${LOGNAME}
				
		LOG_TO_LINE=`wc -l ${LOGNAME} | awk '{print $1}' ` 2>> ${LOGNAME}
		echo "LOG_TO_LINE=${LOG_TO_LINE}" >> ${LOGNAME}

		
		#############################################################
		# Extract log file entries for current request file.
		#############################################################
		sed -n "${LOG_FROM_LINE},${LOG_TO_LINE}p" ${LOGNAME} > ${DATADIR}${TMP_DSH_FF_LOGFILE}


		#############################################################
		# Get list of S3 files for success email.
		#############################################################
		echo "" >> ${LOGNAME}
		echo "Get S3 Extract file list and record counts" >> ${LOGNAME}

		#getExtractFilenamesAndCounts ${LOGDIR}${LOGNAME} >> ${LOGNAME}  2>&1
		getExtractFilenamesAndCounts ${DATADIR}${TMP_DSH_FF_LOGFILE} >> ${LOGNAME}  2>&1
		S3Files="${filenamesAndCounts}" 

		echo "" >> ${LOGNAME}
		S3Files=`echo "${S3Files}" ` >> ${LOGNAME}  2>&1


		#############################################################
		# Create Manifest file.
		#############################################################
		echo "" >> ${LOGNAME}
		echo "Create Manifest file for DSH Request Extract.  " >> ${LOGNAME}

		# Get Count of NOF Extract Files to include in manifest file
		getNOFFILES4ManifestFile

        if [ ${NOF_FILES_4_MANIFEST} -eq 0 ];then
		
			echo "No manifest file to create for DSH Request Extract.  " >> ${LOGNAME}

			#############################################################
			# Send success email of DSH Extract files
			#############################################################
			echo "" >> ${LOGNAME}
			echo "Send success email." >> ${LOGNAME}

			# Send Success email	
			SUBJECT="DSH Extract - completed ($ENVNAME)"
			MSG="DSH Extract completed for request file ${FF}. \n\nThe following extract files were processed:\n\n${S3Files}\n\nNo manifest file was created.\n\nPlease note that DSH data is calculated by the federal government fiscal year which goes from October 1 from the prior year, through September 30 of the current year. Example: Fiscal year 2021 is from 10/1/2020 through 9/30/2021."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${RQST_EMAIL_RECIPIENT}" "${SUBJECT}" "${MSG}" "${DSH_EMAIL_BCC}" "${DSH_EMAIL_REPLY_MSG}" >> ${LOGNAME} 2>&1

			if [[ $RET_STATUS != 0 ]]; then
				echo "" >> ${LOGNAME}
				echo "Error in calling sendEmail.py" >> ${LOGNAME}
				
				# Send Failure email	
				SUBJECT="Sending Success email in DSH_Extract.sh  - Failed (${ENVNAME})"
				MSG="Sending Success email in DSH_Extract.sh has failed."
				${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

				exit 12
			fi
			
		else	
			#####################################################
			# S3BUCKET --> points to location of extract file. 
			#          --> S3 folder is key token to config file to determine of manifest file is in HOLD status   
			# TMSTMP   --> uniquely identifies extract file(s) 
			# EMAIL_SUCCESS_RECIPIENT --> manifest file recipients
			# MANIFEST_HOLD_BUCKET --> overide destination for manifest file
			#
			# Ex. CreateManifestFile.sh s3://aws-hhs-cms-eadg-bia-ddom-extracts/xtr/DSH/ 20231211.125522 pbaranoski-con@index.com 
			#####################################################
			echo "Creating manifest file for DSH Request Extract.  " >> ${LOGNAME}

			if [ "${REQSTR_EMAIL}" = "" ];then
				BOX_RECIPIENT="${DSH_BOX_RECIPIENT}"
			else
				BOX_RECIPIENT="${REQSTR_EMAIL},${DSH_BOX_RECIPIENT}"
			fi 
			
			echo "BOX_RECIPIENT=${BOX_RECIPIENT}" >> ${LOGNAME}
			
			${RUNDIR}CreateManifestFile.sh ${S3BUCKET} ${FF_TMSTMP} "${BOX_RECIPIENT}" 

			#############################################################
			# Check the status of script
			#############################################################
			RET_STATUS=$?

			if [[ $RET_STATUS != 0 ]]; then
				echo "" >> ${LOGNAME}
				echo "Shell script CreateManifestFile.sh failed." >> ${LOGNAME}
				
				# Send Failure email	
				SUBJECT="Create Manifest file in DSH_Extracts.sh  - Failed ($ENVNAME)"
				MSG="Create Manifest file in DSH_Extracts.sh  has failed."
				${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

				exit 12
			fi	


			#############################################################
			# Send success email of DSH Extract files
			#############################################################
			echo "" >> ${LOGNAME}
			echo "Send success email." >> ${LOGNAME}

			# Send Success email	
			SUBJECT="DSH Extract - In-Process ($ENVNAME)"
			MSG="DSH Extract in process for request file ${FF}. \n\nThe following extract files were created:\n\n${S3Files}\n\nOnce the process is complete and the file(s) are available, you will receive an email from data.request@datainsights.cms.gov with a link to the file location in your Box account.\n\nThe manifest file is DSH_EXTRACT_Manifest_${FF_TMSTMP}.json\n\nPlease note that DSH data is calculated by the federal government fiscal year which goes from October 1 from the prior year, through September 30 of the current year. Example: Fiscal year 2021 is from 10/1/2020 through 9/30/2021."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${RQST_EMAIL_RECIPIENT}" "${SUBJECT}" "${MSG}" "${DSH_EMAIL_BCC}" "${DSH_EMAIL_REPLY_MSG}" >> ${LOGNAME} 2>&1

			if [[ $RET_STATUS != 0 ]]; then
				echo "" >> ${LOGNAME}
				echo "Error in calling sendEmail.py" >> ${LOGNAME}
				
				# Send Failure email	
				SUBJECT="Sending Success email in DSH_Extract.sh  - Failed (${ENVNAME})"
				MSG="Sending Success email in DSH_Extract.sh has failed."
				${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

				exit 12
			fi	


			#############################################################
			# Insert new Requestor Emails into DSH_EMAIL table.
			#############################################################
			echo "" >> ${LOGNAME}
			echo "Insert new DSH Requestor Email Address for Requestor UNIQ-ID" >> ${LOGNAME}
	
			${PYTHON_COMMAND} ${RUNDIR}DSH_AddReqEmails.py --ReqID "${FF_ID_NODE}" --Email "${REQSTR_EMAIL}"  >> ${LOGNAME} 2>&1

			if [[ $RET_STATUS != 0 ]]; then
				echo "" >> ${LOGNAME}
				echo "Error in calling sendEmail.py" >> ${LOGNAME}
				
				# Send Failure email	
				SUBJECT="Insert new DSH Requestor Email Address into DSH_EMail table in DSH_Extract.sh - Failed (${ENVNAME})"
				MSG="Insert new DSH Requestor Email Address into DSH_EMail table in DSH_Extract.sh has failed."
				${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

				exit 12
			fi	

		fi
	
	fi


	#############################################################
	# Move Finder File in S3 to archive folder
	#############################################################
	echo " " >> ${LOGNAME}
	echo "Moving S3 DSH Finder file ${FF} to S3 archive folder." >> ${LOGNAME}
	
	aws s3 mv s3://${FINDER_FILE_BUCKET}${FF} s3://${FINDER_FILE_BUCKET}archive/${FF}  1>> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Moving S3 DSH Finder file to S3 archive folder failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="DSH Extract - Failed ($ENVNAME)"
		MSG="Moving S3 Finder file to S3 archive folder failed.  ( ${FINDER_FILE_BUCKET}${S3Filename} to ${FINDER_FILE_BUCKET}archive/${S3Filename} )"
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi	

	
	#############################################################
	# Delete Finder File in Linux
	#############################################################
	echo "" >> ${LOGNAME}
	echo "Delete finder file ${DATADIR}${FF} from linux data directory." >> ${LOGNAME}
	rm ${DATADIR}${FF} 2>> ${LOGNAME}

	
done


#############################################################
# clean-up linux data directory
#############################################################
echo "" >> ${LOGNAME} 
echo "Remove temp files from data directory" >> ${LOGNAME} 

rm ${DATADIR}${TMP_DSH_FF_LOGFILE} 2>> ${LOGNAME}

 
#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "DSH_Extracts.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS
