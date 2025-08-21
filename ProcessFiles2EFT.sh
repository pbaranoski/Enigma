#!/usr/bin/sh
#
######################################################################################
# Name: ProcessFiles2EFT.sh
# Desc: Unzip extract files, rename them as P#EFT.ON.*, and place them in S3 EFT_Files
#       folder.
#
# Execute as ./ProcessFiles2EFT.sh $1 $2 (Optional) 
#            $1 = Extract S3 folder 
#                 (Note: Does not include the full path when run from RunDeck).
#            $2 = S3 Destination folder (optional) default: EFT_Files   
#
# Created: Paul Baranoski  04/11/2023
#
# Modified: 
#
# Paul Baranoski 2023-04-11 Created script.
# Paul Baranoski 2023-04-18 Add '1' to end of time in EFT filename
#                           Modify script to accept S3 Extract folder as optional parameter
#                           to script. Script will use that parameter if it is included, 
#                           and parameter file if it is not included.
#                           Add code to clean config Xref file of any CR (\r) characters.
# Paul Baranoski 2023-05-09 Added code to be able to find the actual substitution token
#                           offset when the token contained hard-coded leading characters. 
#                           Ex. Finding token "PR{YY}" was able to extract {YY} as actual 
#                               substitution token.
# Paul Baranoski 2023-05-10 Added ability to count XREF file matches. Re-worked if statement
#                           to handle more conditions.
# Paul Baranoski 2023-05-12 Added code to check return_status after call to CombineS3Files.sh.
# Paul Baranoski 2023-05-15 Change code to remove '\r' to use sed -i command.
# Paul Baranoski 2023-05-16 Modify grep -bo '{' code to only look at first occurrence.
# Paul Baranoski 2023-06-02 Add code to verify that MF_FILENAME is a valid length.
# Paul Baranoski 2023-06-06 Add code to handle SF suffix files Ex. filename.txt-0, filename.txt-1
# Paul Baranoski 2023-06-07 Make some modifications to the suffix files logic after testing in prod.
# Paul Baranoski 2023-06-08 Correct syntax "if [ ${sfx_num} -gt 9]" to if [ ${sfx_num} -gt 9 ]" which was causing 
#                           error ->  [: missing `]
#                           Remove duplicate edit for filename length. (How did that happen?)
# Paul Baranoski 2023-06-16 Modify TMSTMP variable to use current value if it exists. This will help to group
#                           log files from same run together since they will all have the same timestamp.
# Paul Baranoski 2023-06-21 Revamped script to remove call to python code to unzip compressed extract file.
#                           Instead, the script downloads the file to linux, unzips there, and moves/renames file
#                           from linux to S3.
# Paul Baranoski 2023-06-23 When getting S3 ls of files, added grep -v to exclude "parts" files.
# Paul Baranoski 2023-06-27 Add sed command to convert 2-byte encoded characters to space. These 2-byte encoded characters
#                           were causing EFT issues, and are from "bad" binary data contained in Teradata and SF databases.
#                           Add edit to force script to end if NOF characters and NOF bytes are not equal.
# Paul Baranoski 2023-07-21 Modified sed command to convert 2-byte encoded characters to space to handle all non-UTF-8/ASCII
#                           characters 2-byte encoded characters instead of one particular instance. (After additional 2-byte
#                           characters were found).
# Paul Baranoski 2023-09-21 Modified code to move EFT file to S3 EFT_Files folder. Only performed move if EFT file HLQ was P#EFT  
#                           or T#EFT.
# Paul Baranoski 2024-01-09 Modified script to no longer use config file to get S3ExtractFolder param. RunDeck will instead pass the parameter. 
#                           Added ability to accept an over-ride S3 EFT Destination folder. 
#                           ADD SSA-RDATE Key with special processing.
# Paul Baranoski 2024-01-26 Modified script to convert double back-slashes to single backslash. If there is a back-slash in the data
#                           snowflake convert to double back-slash because the back-slash is an escape character. The reason
#                           for the change is that the extrac back-slash incorrectly increases the LRECL, causing failure of the file
#                           to EFT. 
# Paul Baranoski 2024-02-01 Add ENVNAME to SUBJECT for all emails.  
# Paul Baranoski 2024-02-27 Comment out code to convert double back-slashes to single backslash to resolve SAF ENC OPT issue '\\T' in data
#                           which causes mis-aligned data.         
# Paul Baranoski 2024-03-25 Add echo "FINAL MF_FILENAME=${MF_FILENAME}" to make it easier to find EFT filenames for SFTP processes to display in emails. 
# Paul Baranoski 2024-05-06 Add logic to determine if file is a SAS/binary file, and bypass specific text file data verfication logic. Add logic to
#                           remove SAS file extension from filename for EFT filename conversion. 
# Paul Baranoski 2024-06-13 Add code to remove SAS file extension from SF_FILENAME variable. 
# Paul Baranoski 2025-07-18 Add -f flag to gzip command to force replacement of unzipped file if still on server. 
######################################################################################

######################################################################################
set +x


#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP:=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/ProcessFiles2EFT_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

#EFT_SF_2_MF_XREF_FILE=EFT_SF_2_MF_XREF.txt

#EFT_DRIVER_FILE=EFT_DRIVER_FILE_${TMSTMP}.txt

S3_EFT_DESTINATION=
S3_EFT_FILES=EFT_Files/


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "" >> ${LOGNAME}
echo "################################### " >> ${LOGNAME}
echo "ProcessFiles2EFT.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}


##################################################################
# Extract can run stand-alone or as a called script.
##################################################################
if ! [[ $# -eq 1 || $# -eq 2  ]]
then
	echo "Incorrect # of parameters sent to script. NOF parameters: $#" >> ${LOGNAME}
	exit 12
fi


#############################################################
# Display parameters passed to script 
#############################################################
S3ParmExtractFolder=$1
S3ParmEFTDestFolder=$2

echo " " >> ${LOGNAME}
echo "Parameters to script: " >> ${LOGNAME}
echo "   S3ParmExtractFolder=${S3ParmExtractFolder} " >> ${LOGNAME}
echo "   S3ParmEFTDestFolder=${S3ParmEFTDestFolder} " >> ${LOGNAME}


#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh >> ${LOGNAME}

EFT_SF_2_MF_XREF_FILE=EFT_SF_2_MF_XREF_${ENVNAME}.txt

#############################################################
# Examples:
# S3 Bucket=aws-hhs-cms-eadg-bia-ddom-extracts 
# S3 High-level folder=xtr/ 
#############################################################
S3BucketAndHLFolder=${bucket}

S3Bucket=`echo ${S3BucketAndHLFolder} | cut -d/ -f1 `  2>> ${LOGNAME}
S3HLFolder=`echo ${S3BucketAndHLFolder} | cut -d/ -f2- `  2>> ${LOGNAME}

echo "" >> ${LOGNAME}
echo "S3 Bucket=${S3Bucket} " >> ${LOGNAME}
echo "S3 High-level folder=${S3HLFolder} " >> ${LOGNAME}

#############################################################
# Set S3 EFT DESTINATION Folder 
#############################################################
echo "" >> ${LOGNAME}

S3_EFT_DESTINATION=${S3ParmEFTDestFolder:-${S3_EFT_FILES}}
echo "S3_EFT_DESTINATION=${S3_EFT_DESTINATION} " >> ${LOGNAME}


###########################################################################
# Set S3ExtractFolder to search for files to process for EFT
# 
# NOTE: When run stand-alone from RunDeck, parm does not include the full S3 Folder path. 
#       Need to remove full path. 
###########################################################################
echo "" >> ${LOGNAME}
echo "Determine if param Extract folder contains full S3 path" >> ${LOGNAME}

FullS3PathIncluded=`echo "${S3ParmExtractFolder}" | grep "${S3BucketAndHLFolder}" `  2>> ${LOGNAME}
echo "FullS3PathIncluded=${FullS3PathIncluded} " >> ${LOGNAME}

# Executed by child script
if [ "${FullS3PathIncluded}" != "" ];then

	NonProdFldr=`echo "${S3ParmExtractFolder}" | awk '/DEV|TST|IMPL/{print}' | wc -l` 2>> ${LOGNAME}
	echo "NonProdFldr flag=${NonProdFldr}" >> ${LOGNAME}

	if [ ${NonProdFldr} -eq 1 ]; then
		S3ExtractFolder=`echo $1 | cut -d/ -f4 ` 2>> ${LOGNAME}
	else
		S3ExtractFolder=`echo $1 | cut -d/ -f3 ` 2>> ${LOGNAME}
	fi
	
	# Add the ending "/"
	S3ExtractFolder=${S3ExtractFolder}/

else
	# Executed stand-alone by RunDeck
	S3ExtractFolder=${S3ParmExtractFolder}	

fi	

echo "S3ExtractFolder=${S3ExtractFolder} " >> ${LOGNAME}


#############################################################
# Download configuration file EFT_SF_2_MF_XREF.txt
#############################################################
echo "" >> ${LOGNAME}
echo "Copy EFT_SF_2_MF_XREF configuration file from S3 to Linux data directory" >> ${LOGNAME}

aws s3 cp s3://${CONFIG_BUCKET}${EFT_SF_2_MF_XREF_FILE} ${DATADIR}${EFT_SF_2_MF_XREF_FILE}  1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Copying S3 ${EFT_SF_2_MF_XREF_FILE} Parameter file to Linux failed." >> ${LOGNAME}
	
	# Send Failure email
	SUBJECT="ProcessFiles2EFT.sh - Failed (${ENVNAME})"
	MSG="Copying S3 ${EFT_SF_2_MF_XREF_FILE} from ${CONFIG_BUCKET} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${EFT_EMAIL_SENDER}" "${EFT_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	


#############################################################
# Remove CR (\r) from records in config xref file
#############################################################
sed -i 's/\r//g' ${DATADIR}${EFT_SF_2_MF_XREF_FILE}


#############################################################
# Set S3 extract bucket/folder
#############################################################
echo "" >> ${LOGNAME}
echo "S3 Extract Folder to process: ${S3ExtractFolder} " >> ${LOGNAME}


#############################################################
# Get a list of all files in S3 bucket/folder
# NOTE: 1) grep -v "PRE " --> exclude s3 sub-folders (will appear as blank lines in EXTRACT FILES)
#       2) awk print $4   --> get the S3 name only
#       3) egrep -v       --> exclude parts files for EFT processing
#############################################################
echo "" >> ${LOGNAME}
echo "Get S3 filenames for folder ${S3BucketAndHLFolder}${S3ExtractFolder} " >> ${LOGNAME}

EXTRACT_FILES=`aws s3 ls s3://${S3BucketAndHLFolder}${S3ExtractFolder} | grep -v "PRE " | awk '{print $4}' | egrep -v "_[0-9]{1,2}_[0-9]{1,2}_[0-9]{1,2}\."  `    2>> ${LOGNAME} 
echo "EXTRACT_FILES=${EXTRACT_FILES}" >> ${LOGNAME}

if [ -z "${EXTRACT_FILES}" ];then
	echo "No files to process in ${S3BucketAndHLFolder}${S3ExtractFolder} " >> ${LOGNAME}
	continue
fi


#############################################################
# Loop thru each file in EXTRACT_FILES
#
# NOTE!!! We assume that we will not be processing "parts" files
#############################################################
for gz_filename in ${EXTRACT_FILES} 
do

	echo "" >> ${LOGNAME}
	echo "*****************************************************************" >> ${LOGNAME}
	echo "gz_filename=${gz_filename}" >> ${LOGNAME}

	txt_filename=`echo ${gz_filename} | sed -e 's/.gz//' `  2>> ${LOGNAME} 
	echo "txt_filename=${txt_filename}" >> ${LOGNAME}


	#############################################################
	# Download S3 compressed file to linux
	#############################################################
	echo "" >> ${LOGNAME}
	echo "Download S3 compressed file ${gz_filename} to linux"  >> ${LOGNAME}
	
	aws s3 cp s3://${S3BucketAndHLFolder}${S3ExtractFolder}${gz_filename} ${DATADIR}${gz_filename}  1>> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Copying S3 file ${S3BucketAndHLFolder}${S3ExtractFolder}${gz_filename} to Linux failed." >> ${LOGNAME}
		
		# Send Failure email
		SUBJECT="ProcessFiles2EFT.sh - Failed (${ENVNAME})"
		MSG="Copying S3 file ${S3BucketAndHLFolder}${S3ExtractFolder}${gz_filename} to Linux failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${EFT_EMAIL_SENDER}" "${EFT_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi


	#############################################################
	# unzip gz file on linux
	#############################################################
	echo "" >> ${LOGNAME}
	echo "Unzip ${gz_filename} file on linux"  >> ${LOGNAME}

	gzip -d -f ${DATADIR}${gz_filename} 2>> ${LOGNAME}
	

	#############################################################
	# Is file binary (SAS) based on file extension? 
	#     If yes 1) --> bypass .txt logic 
	#############################################################
	bBinaryFile=`echo ${gz_filename} | grep -Eic '*.sas7bdat$' ` 

	# File is a binary file
	if [ ${bBinaryFile} -eq 1 ];then
		echo "" >> ${LOGNAME}
		echo "${gz_filename} is a binary file. Skipping text validation logic. " >> ${LOGNAME}
		
	else	
		#############################################################
		# Convert bad binary data x'c28d' to spaces.
		# Other two-byte characters: x'c39b', x'c386', x'c384'
		# UTF-8/ASCII characters are x'00' thru x'7f'
		#############################################################
		echo "" >> ${LOGNAME}
		echo "Clean-up bad binary data on text file --> 2-byte encoded characters"  >> ${LOGNAME}

		#sed -i 's/\xc2\x8d/ /g' ${DATADIR}${txt_filename} 2>> ${LOGNAME}
		LC_ALL=C sed -i 's/[\x80-\xff][\x80-\xff]/ /g' ${DATADIR}${txt_filename} 2>> ${LOGNAME} 

		#############################################################
		# Convert double back-slashes to single back-slash.
		# SF converts single back-slash in results set (bad data)
		# and convert to double back-slash which incorrectly increases the LRECL
		#############################################################
		#echo "" >> ${LOGNAME}
		#echo "Convert double back-slashes to single back-slash. "  >> ${LOGNAME}
		#sed -i 's_\\\\_\\_'g ${DATADIR}${txt_filename} 2>> ${LOGNAME} 
		
		#############################################################
		# Verify that NOF bytes == NOF characters
		#############################################################
		echo "" >> ${LOGNAME}
		echo "Verify that file NOF Bytes = NOF Characters" >> ${LOGNAME}

		CNTS=`wc -cm ${DATADIR}${txt_filename}`  2>> ${LOGNAME}
		CNTS1=`echo $CNTS | awk '{print $1}' `
		CNTS2=`echo $CNTS | awk '{print $2}'`
		
		if [ ${CNTS1} -ne ${CNTS2} ]; then
			echo "" >> ${LOGNAME}
			echo "ProcessFiles2EFT.sh failed. Could not convert all multi-byte characters for file ${txt_filename}. CNTS1=${CNTS1} CNTS2=${CNTS2} " >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="ProcessFiles2EFT.sh - Failed (${ENVNAME})"
			MSG="ProcessFiles2EFT.sh has failed. Could not convert all multi-byte characters for file ${txt_filename}. CNTS1=${CNTS1} CNTS2=${CNTS2} "
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${EFT_EMAIL_SENDER}" "${EFT_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12			
		fi
	fi
	

	###################################################################
	# Find Key/Value file mapping record
	###################################################################
	echo "" >> ${LOGNAME}
	echo "Build MF EFT file name for SF file " >> ${LOGNAME}

	# Extract appropriate key/value record from config file
	SEARCH_1NODE=`echo ${txt_filename} | cut -d_ -f1 `  2>> ${LOGNAME}	
	SEARCH_2NODE=`echo ${txt_filename} | cut -d_ -f1-2 `  2>> ${LOGNAME}	
	
	# Count number of matching rows
	NOF_SF2MF_KEY_VALUE_MATCHES=`grep -c "^${SEARCH_1NODE}" ${DATADIR}${EFT_SF_2_MF_XREF_FILE} `  2>> ${LOGNAME}
	echo "NOF_SF2MF_KEY_VALUE_MATCHES=${NOF_SF2MF_KEY_VALUE_MATCHES}"  >> ${LOGNAME}
	
	if [ ${NOF_SF2MF_KEY_VALUE_MATCHES} -eq 0 ];then
		echo "" >> ${LOGNAME}
		echo "ProcessFiles2EFT.sh failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="ProcessFiles2EFT.sh - Failed (${ENVNAME})"
		MSG="ProcessFiles2EFT.sh has failed. Could not find matching SF2MF Key/value record for key=${SEARCH_1NODE} "
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${EFT_EMAIL_SENDER}" "${EFT_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12	
		
	elif [ ${NOF_SF2MF_KEY_VALUE_MATCHES} -eq 1 ];then	
		SF2MF_KEY_VALUE_PAIR=`grep "^${SEARCH_1NODE}" ${DATADIR}${EFT_SF_2_MF_XREF_FILE} `  2>> ${LOGNAME}
		echo "SF2MF_KEY_VALUE_PAIR--> ${SF2MF_KEY_VALUE_PAIR}" >> ${LOGNAME}

	elif [ ${NOF_SF2MF_KEY_VALUE_MATCHES} -ge 2 ];then
		NOF_SF2MF_KEY_VALUE_MATCHES=`grep -c "^${SEARCH_2NODE}" ${DATADIR}${EFT_SF_2_MF_XREF_FILE} `  2>> ${LOGNAME}
		
		if [ ${NOF_SF2MF_KEY_VALUE_MATCHES} -eq 1 ];then	
			# Try to find a match looking at two nodes
			SF2MF_KEY_VALUE_PAIR=`grep "^${SEARCH_2NODE}" ${DATADIR}${EFT_SF_2_MF_XREF_FILE} `  2>> ${LOGNAME}
			echo "SF2MF_KEY_VALUE_PAIR--> ${SF2MF_KEY_VALUE_PAIR}" >> ${LOGNAME}

		else
			echo "" >> ${LOGNAME}
			echo "Found ${NOF_SF2MF_KEY_VALUE_MATCHES} matching (too many) SF2MF Key/value records for key=${SEARCH_2NODE}" >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="ProcessFiles2EFT.sh - Failed (${ENVNAME})"
			MSG="ProcessFiles2EFT.sh has failed. Found ${NOF_SF2MF_KEY_VALUE_MATCHES} matching (too many) SF2MF Key/value records for key=${SEARCH_2NODE} "
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${EFT_EMAIL_SENDER}" "${EFT_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12	
		fi	
		
	fi

	
	############################################################
	# Extract SF and MF file masks from config record; 
	#   1) remove file extension 
	#   2) change '_' to ' ' to make it easier to create array
	#
	# NOTE: "=" separates key and value parts
	############################################################
	SF_FILE_MASK=`echo ${SF2MF_KEY_VALUE_PAIR} | cut -d= -f1 | sed -e "s/.txt//" -e "s/.csv//" -e "s/.sas7bdat//" | tr '_' ' ' `    2>> ${LOGNAME}  
	SF_FILENAME=`echo ${txt_filename} | sed -e "s/.txt//" -e "s/.csv//" -e "s/.sas7bdat//" | tr '_' ' ' `   2>> ${LOGNAME}

	MF_FILE_MASK=`echo ${SF2MF_KEY_VALUE_PAIR} | cut -d= -f2 `  2>> ${LOGNAME}
	MF_FILENAME=${MF_FILE_MASK}
	
	echo "MF_FILE_MASK=${MF_FILE_MASK}" >> ${LOGNAME}
	echo "SF_FILE_MASK Array=${SF_FILE_MASK}" >> ${LOGNAME}
	echo "SF_FILENAME Array=${SF_FILENAME}" >> ${LOGNAME}
	
	# Create array of tokens from filenames
	SF_FILEMASK_ARRAY=(${SF_FILE_MASK})
	SF_FILENAME_ARRAY=(${SF_FILENAME})

	echo "" >> ${LOGNAME}
	echo "Parse SF filename mask nodes" >> ${LOGNAME}

	for (( i=0 ; i < ${#SF_FILEMASK_ARRAY[@]}; i++ )); do
		echo "" >> ${LOGNAME}
		echo "$i= ${SF_FILEMASK_ARRAY[$i]}" >> ${LOGNAME}
		
		sub_token=`echo ${SF_FILEMASK_ARRAY[$i]} | grep "{" `
		
		# replacement token exists; string is not null
		if [ -n "${sub_token}" ];then
			key=${SF_FILEMASK_ARRAY[$i]} 
			value=${SF_FILENAME_ARRAY[$i]}
			
			if [ "${sub_token}" = "{TIMESTAMP}" ]; then
				YYMMDD=`echo ${value} | cut -c3-8 `
				HHMMSS=`echo ${value} | cut -c10-15 `
				# EFT transer process needs Time node to have 7 digits -- add "1" after time
				value="D${YYMMDD}.T${HHMMSS}1"
				echo "valueTM=${value}"  >> ${LOGNAME}
				
			elif [ "${sub_token}" = "{SSA-RDATE}" ]; then
				YYMMDD=`echo ${value} | cut -c3-8 `
				HHMMSS=`echo ${value} | cut -c10-15 `
				# SSA needs RDATE with no time component
				value="R${YYMMDD}.T${HHMMSS}"
				echo "valueRDT=${value}"  >> ${LOGNAME}

			else
				# calculate offset if there are leading characters before substitution token
				offset=`echo ${key} | grep -bo "{" | head -n 1 | cut -d: -f1`  2>> ${LOGNAME}
				echo "key token offset=${offset} + 1"       >> ${LOGNAME}
				offset=`expr ${offset} + 1 `               

				key=`echo ${key} | cut -c${offset}- `      2>> ${LOGNAME}
				value=`echo ${value} | cut -c${offset}- `  2>> ${LOGNAME}
			fi
		
			echo "${key} replaced by ${value} "   >> ${LOGNAME}
			MF_FILENAME=`echo "${MF_FILENAME}" | sed -e "s/${key}/${value}/" `
			echo "MF_FILENAME=${MF_FILENAME}"   >> ${LOGNAME}
		fi
	done	

	echo "FINAL MF_FILENAME=${MF_FILENAME}"   >> ${LOGNAME}
			
	###################################################################
	# Verify that EFT filename is a valid length 
	###################################################################
	echo "" >> ${LOGNAME}
	echo "Verify EFT filename length. " >> ${LOGNAME}
	
	if [ ${#MF_FILENAME} -gt 44 ];then
		echo "" >> ${LOGNAME}
		echo "${MF_FILENAME} filename is ${#MF_FILENAME} bytes which is too long. " >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="ProcessFiles2EFT - Failed (${ENVNAME})"
		MSG="${MF_FILENAME} filename is ${#MF_FILENAME} bytes which is too long."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${EFT_EMAIL_SENDER}" "${EFT_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 

		exit 12
	fi


	#############################################################
	# Upload uncompressed linux file to S3/EFT_Files folder
	# NOTE: Only send/trigger files that can actually be EFT'd.
	#############################################################
	HLQ=`echo ${MF_FILENAME} | cut -d. -f1 `
	
	if ! [ "${HLQ}" = "P#EFT" -o "${HLQ}" = "T#EFT" -o "${HLQ}" = "MNUP" ];then
		echo "" >> ${LOGNAME}
		echo "HLQ=${HLQ}; File NOT loaded to S3 EFT_FILES folder."  >> ${LOGNAME}

		# Remove file from linux since its not being moved	
		rm ${DATADIR}${txt_filename} 1>> ${LOGNAME}  2>&1
		
	else
		echo "" >> ${LOGNAME}
		echo "Upload linux decompressed file ${txt_filename} to s3://${S3BucketAndHLFolder}${S3_EFT_DESTINATION}${MF_FILENAME}"  >> ${LOGNAME}
		
		aws s3 mv ${DATADIR}${txt_filename} s3://${S3BucketAndHLFolder}${S3_EFT_DESTINATION}${MF_FILENAME}  1>> ${LOGNAME} 2>&1

		RET_STATUS=$?

		if [[ $RET_STATUS != 0 ]];then
			echo "" >> ${LOGNAME}
			echo "Moving file ${txt_filename} to s3://${S3BucketAndHLFolder}${S3_EFT_DESTINATION}${MF_FILENAME} failed." >> ${LOGNAME}
			
			# Send Failure email
			SUBJECT="ProcessFiles2EFT.sh - Failed (${ENVNAME})"
			MSG="Moving file ${txt_filename} to s3://${S3BucketAndHLFolder}${S3_EFT_DESTINATION}${MF_FILENAME} failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${EFT_EMAIL_SENDER}" "${EFT_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
		fi
	fi


	###################################################################
	# Move processed Extract file to Extract archive folder.
	###################################################################
	echo "" >> ${LOGNAME}
	echo "Move processed ${gz_filename} to S3 Extract archive folder " >> ${LOGNAME}

	aws s3 mv s3://${S3BucketAndHLFolder}${S3ExtractFolder}${gz_filename} s3://${S3BucketAndHLFolder}${S3ExtractFolder}archive/${gz_filename}  1>> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Move processed ${gz_filename} to S3 Extract archive folder failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="ProcessFiles2EFT - Failed (${ENVNAME})"
		MSG="Move processed ${gz_filename} to S3 Extract archive folder failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${EFT_EMAIL_SENDER}" "${EFT_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 

		exit 12
	fi	
		
done


#############################################################
# clean-up linux data directory
#############################################################


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "ProcessFiles2EFT.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS
