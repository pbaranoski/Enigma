#!/usr/bin/sh
#
######################################################################################
# Name: ProcessFiles2EFT.sh
# Desc: Unzip extract files, rename them as P#EFT.ON.*, and place them in S3 EFT_Files
#       folder.
#
# Execute as ./ProcessFiles2EFT.sh $S3BucketFldr (parameter optional when run stand-alone.
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

EFT_SF_2_MF_XREF_FILE=EFT_SF_2_MF_XREF.txt
EFT_S3_DRIVER_FILE=EFT_DRIVER_FILE.txt
EFT_DRIVER_FILE=EFT_DRIVER_FILE_${TMSTMP}.txt

touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "" >> ${LOGNAME}
echo "################################### " >> ${LOGNAME}
echo "ProcessFiles2EFT.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################

source ${RUNDIR}SET_XTR_ENV.sh >> ${LOGNAME}

S3BucketAndHLFolder=${bucket}
S3Bucket=`echo ${S3BucketAndHLFolder} | cut -d/ -f1 `  2>> ${LOGNAME}
S3HLFolder=`echo ${S3BucketAndHLFolder} | cut -d/ -f2- `  2>> ${LOGNAME}

S3Files2EFTFolder=Files2EFT/

S3EFTFolder=${S3HLFolder}${S3Files2EFTFolder}

echo "" >> ${LOGNAME}
echo "S3 Bucket=${S3Bucket} " >> ${LOGNAME}
echo "S3 High-level folder=${S3HLFolder} " >> ${LOGNAME}
echo "S3 Files2EFT folder=${S3EFTFolder} " >> ${LOGNAME}


###########################################################################
# If script has one parameter  (S3 Bucket/fldr)
#    Execute as child-script (called from a parent script)
#    Write parameter value to EFT_DRIVER_FILE (which drives processing)
# Else 
#    Download EFT_DRIVER_FILE file from S3 to linux for stand-alone run
###########################################################################
if [ $# -eq 1 ]; then

	echo "1 parameter sent to script: ${1} " >> ${LOGNAME}

	NonProdFldr=`echo "$1" | awk '/DEV|TST|IMPL/{print}' | wc -l` 2>> ${LOGNAME}
	echo "NonProdFldr flag=${NonProdFldr}" >> ${LOGNAME}

	if [ ${NonProdFldr} -eq 1 ]; then
		S3ExtractFolder=`echo $1 | cut -d/ -f4 ` 2>> ${LOGNAME}
	else
		S3ExtractFolder=`echo $1 | cut -d/ -f3 ` 2>> ${LOGNAME}
	fi
	
	# Add the ending "/"
	S3ExtractFolder=${S3ExtractFolder}/
	# write parameter to EFT_DRIVER_FILE 
	echo ${S3ExtractFolder} > ${DATADIR}${EFT_DRIVER_FILE} 
	
else

	#############################################################
	# Download configuration file EFT_S3_DRIVER_FILE from S3
	#############################################################
	echo "" >> ${LOGNAME}
	echo "Copy EFT_DRIVER_FILE configuration file from S3 to Linux data directory" >> ${LOGNAME}

	aws s3 cp s3://${CONFIG_BUCKET}${EFT_S3_DRIVER_FILE} ${DATADIR}${EFT_DRIVER_FILE}  1>> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Copying S3 ${EFT_S3_DRIVER_FILE} Parameter file to Linux failed." >> ${LOGNAME}
		
		# Send Failure email
		SUBJECT="ProcessFiles2EFT.sh  - Failed"
		MSG="Copying S3 ${EFT_S3_DRIVER_FILE} from ${CONFIG_BUCKET} failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${EFT_EMAIL_SENDER}" "${EFT_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi
fi	


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
	SUBJECT="ProcessFiles2EFT.sh  - Failed"
	MSG="Copying S3 ${EFT_SF_2_MF_XREF_FILE} from ${CONFIG_BUCKET} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${EFT_EMAIL_SENDER}" "${EFT_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	


#############################################################
# Remove CR (\r) from records in config xref file
#############################################################
sed -i 's/\r//g' ${DATADIR}${EFT_SF_2_MF_XREF_FILE}


#############################################################
# Loop thru EFT Driver file records 
# NOTE: Contains S3 Extract folders to process (e.g., NYSPAP)
#############################################################
while read FF_EFT_DRIVER_REC
do

	#############################################################
	# Set S3 extract bucket/folder
	#############################################################
	echo "" >> ${LOGNAME}
	
	S3ExtractFolder=`echo "${FF_EFT_DRIVER_REC}" | tr -d '\r' `  2>> ${LOGNAME}
	# skip blank lines
	if [ -z "${S3ExtractFolder}" ];then
		continue
	fi
	
	echo "S3 Extract Folder to process: ${S3ExtractFolder} " >> ${LOGNAME}


	#############################################################
	# Get a list of all files in S3 bucket/folder
	#############################################################
	echo "" >> ${LOGNAME}
	echo "Get S3 filenames for folder ${S3BucketAndHLFolder}${S3ExtractFolder} " >> ${LOGNAME}

	EXTRACT_FILES=`aws s3 ls s3://${S3BucketAndHLFolder}${S3ExtractFolder} | awk '{print $4}' `    2>> ${LOGNAME} 
	echo "EXTRACT_FILES=${EXTRACT_FILES}" >> ${LOGNAME}

	if [ -z "${EXTRACT_FILES}" ];then
		echo "No files to process in ${S3BucketAndHLFolder}${S3ExtractFolder} " >> ${LOGNAME}
		continue
	fi


	#############################################################
	# Loop thru each file in EXTRACT_FILES
	#############################################################
	for gz_filename in ${EXTRACT_FILES} 
	do

		echo "" >> ${LOGNAME}
		echo "*****************************************************************" >> ${LOGNAME}
		echo "gz_filename=${gz_filename}" >> ${LOGNAME}

		txt_filename=`echo ${gz_filename} | sed -e 's/.gz//' `  2>> ${LOGNAME} 
		echo "txt_filename=${txt_filename}" >> ${LOGNAME}


		###################################################################
		# Call python program to unzip Extract file from Extract folder,
		# and place parts in Files2EFT folder.
		###################################################################
		# Ex. SOURCE_BUCKET=aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod
		# Ex. ZIP_FILE=xtr/DEV/Blbtn/filename.gz 
		# Ex. UNZIP_FILE=xtr/DEV/Files2EFT/filename.txt
		###################################################################
		SOURCE_BUCKET=${S3Bucket}
		ZIP_FILE=${S3HLFolder}${S3ExtractFolder}${gz_filename}
		UNZIP_FILE=${S3EFTFolder}${txt_filename} 
		
		echo "SOURCE_BUCKET=${SOURCE_BUCKET}" >> ${LOGNAME}
		echo "ZIP_FILE=${ZIP_FILE}" >> ${LOGNAME}
		echo "UNZIP_FILE=${UNZIP_FILE}" >> ${LOGNAME}

		#############################################################
		# Execute Python code to extract data.
		#############################################################
		echo "" >> ${LOGNAME}
		echo "Start execution of UnzipS3File.py program"  >> ${LOGNAME}
		${PYTHON_COMMAND} ${RUNDIR}UnzipS3File.py --bucket ${SOURCE_BUCKET} --zipFile ${ZIP_FILE} --unzipFile ${UNZIP_FILE}   >> ${LOGNAME} 2>&1


		#############################################################
		# Check the status of python script  
		#############################################################
		RET_STATUS=$?

		if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Python script UnzipS3File.py failed" >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="ProcessFiles2EFT.sh  - Failed"
			MSG="Python script UnzipS3File.py failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${EFT_EMAIL_SENDER}" "${EFT_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
		fi

		echo "Python script UnzipS3File.py completed successfully. " >> ${LOGNAME}	


		###################################################################
		# Find s3 files in Files2EFT/ folder matching txt_filename
		# NOTE: If only one file (no parts) --> skip combineFiles script.
		###################################################################
		echo "" >> ${LOGNAME}
		echo "Find S3 file parts for file ${txt_filename} " >> ${LOGNAME}

		NOF_FILE_PARTS=`aws s3 ls s3://${S3BucketAndHLFolder}${S3Files2EFTFolder}${txt_filename} | awk '{print $4}' | wc -l`    1>> ${LOGNAME} 2>&1
		echo "${NOF_FILE_PARTS} file parts found matching s3://${S3BucketAndHLFolder}${S3Files2EFTFolder}${txt_filename} " >> ${LOGNAME}

		RET_STATUS=$?

		if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Find S3 file parts for file ${txt_filename} failed." >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="ProcessFiles2EFT - Failed"
			MSG="Find S3 file parts for file ${txt_filename} failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${EFT_EMAIL_SENDER}" "${EFT_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
		fi

		
		###################################################################
		# Call CombineFiles script to combine part files into single file.
		# Use aws s3 ls command to get number of parts files. 
		# If one, skip calling CombineFiles script.
		###################################################################
		if [ ${NOF_FILE_PARTS} -eq  1 ]; then
			echo "S3 File ${txt_filename} is single file. " >> ${LOGNAME}
		else
			echo "S3 File ${txt_filename} has multiple parts. " >> ${LOGNAME}
			
			echo "" >> ${LOGNAME}
			echo "Concatenate S3 files using CombineS3Files.sh   " >> ${LOGNAME}

			echo "S3BUCKET=${S3BucketAndHLFolder}${S3Files2EFTFolder} " >> ${LOGNAME} 

			concatFilename=${txt_filename}
			echo "concatFilename=${txt_filename}" >> ${LOGNAME} 

			${RUNDIR}CombineS3Files.sh ${S3BucketAndHLFolder}${S3Files2EFTFolder} ${txt_filename} 

			RET_STATUS=$?

			if [[ $RET_STATUS != 0 ]]; then
				echo "" >> ${LOGNAME}
				echo "Call CombineS3Files.sh failed." >> ${LOGNAME}
				
				# Send Failure email	
				SUBJECT="ProcessFiles2EFT - Failed"
				MSG="Call CombineS3Files.sh failed."
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
			SUBJECT="ProcessFiles2EFT.sh  - Failed"
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
				SUBJECT="ProcessFiles2EFT.sh  - Failed"
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
		SF_FILE_MASK=`echo ${SF2MF_KEY_VALUE_PAIR} | cut -d= -f1 | sed -e "s/.txt//" -e "s/.csv//" | tr '_' ' ' `    2>> ${LOGNAME}  
		SF_FILENAME=`echo ${txt_filename} | sed -e "s/.txt//" -e "s/.csv//" | tr '_' ' ' `   2>> ${LOGNAME}

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


		###################################################################
		# Verify that EFT filename is a valid length 
		###################################################################
		echo "" >> ${LOGNAME}
		echo "Verify EFT filename length. " >> ${LOGNAME}
		
		if [ ${#MF_FILENAME} -gt 44 ];then
			echo "" >> ${LOGNAME}
			echo "${MF_FILENAME} filename is ${#MF_FILENAME} bytes which is too long. " >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="ProcessFiles2EFT - Failed"
			MSG="${MF_FILENAME} filename is ${#MF_FILENAME} bytes which is too long."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${EFT_EMAIL_SENDER}" "${EFT_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 

			exit 12
		fi


		###################################################################
		# Get S3 filename(s) in Files2EFT folder since there may now be two 
        # files instead of one after unzip/combine s3 file process has run.		
		#  FYI: In case one file has turned into two or more "suffix" files 
		#       Ex. filename.txt-0, filename.txt-1
		###################################################################
		echo "" >> ${LOGNAME}
		echo "Get S3 filenames in folder ${S3BucketAndHLFolder}${S3Files2EFTFolder} that match ${txt_filename} " >> ${LOGNAME}

		S3Files_IN_FILES2EFT=`aws s3 ls s3://${S3BucketAndHLFolder}${S3Files2EFTFolder}${txt_filename} | awk '{print $4}' `    2>> ${LOGNAME} 
		echo "S3Files_IN_FILES2EFT=${S3Files_IN_FILES2EFT}" >> ${LOGNAME}

		if [ -z "${S3Files_IN_FILES2EFT}" ];then
			echo "" >> ${LOGNAME}
			echo "No files to process that matched ${S3BucketAndHLFolder}${S3Files2EFTFolder}${txt_filename} were found. " >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="ProcessFiles2EFT - Failed"
			MSG="No files to process that matched ${S3BucketAndHLFolder}${S3Files2EFTFolder}${txt_filename} were found."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${EFT_EMAIL_SENDER}" "${EFT_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 

			exit 12
		fi


		for file2eft in ${S3Files_IN_FILES2EFT}
		do 

			echo "----------------------------------" >> ${LOGNAME}
			echo "file2eft=${file2eft} " >> ${LOGNAME}
		
			###################################################################
			# Is file a suffix file? 
			# Yes --> overlay last-extra byte of time component
			###################################################################
			SFX_FILE=` echo ${file2eft} | grep "txt-" `
			echo "SFX_FILE=|${SFX_FILE}|"  >> ${LOGNAME}

			# file contains a suffix
			if [ -n "${SFX_FILE}" ]; then
				echo "File2EFT filename ${file2eft} has suffix"  >> ${LOGNAME}

				#replace txt_filename with new suffix filename
				txt_filename=${file2eft}
				
				sfx_num=`echo ${txt_filename} | cut -d- -f2 ` 
				echo "sfx_num=${sfx_num}"  >> ${LOGNAME}
				
				if [ ${sfx_num} -gt 9 ]; then
					echo "" >> ${LOGNAME}
					echo "${txt_filename} file suffix value of ${sfx_num} greater than the max value of 9. " >> ${LOGNAME}
					
					# Send Failure email	
					SUBJECT="ProcessFiles2EFT - Failed"
					MSG="${txt_filename} file suffix value of ${sfx_num} greater than the max value of 9."
					${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${EFT_EMAIL_SENDER}" "${EFT_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 

					exit 12
				fi
				
				MF_filename_len=${#MF_FILENAME}
				echo "MF_filename_len=${MF_filename_len} "  >> ${LOGNAME}
				bef_suffix_pos=`expr ${MF_filename_len} - 1 `
				echo "bef_suffix_pos=${bef_suffix_pos}"  >> ${LOGNAME}

				MF_FILENAME_PT1=`echo ${MF_FILENAME} | cut -c1-${bef_suffix_pos}`
				MF_FILENAME=${MF_FILENAME_PT1}${sfx_num}  
				echo "MF_FILENAME with overlayed suffix=${MF_FILENAME}"  >> ${LOGNAME} 
			else
				echo "File2EFT filename ${file2eft} does not contain a file suffix"	>> ${LOGNAME}
			fi


			###################################################################
			# Move (and rename) txt-filename from Files2EFT folder to EFT_FILES folder. 
			###################################################################
			echo "" >> ${LOGNAME}
			echo "Move ${txt_filename} to EFT_Files folder and rename as ${MF_FILENAME} " >> ${LOGNAME}

			echo "aws s3 mv s3://${S3BucketAndHLFolder}${S3Files2EFTFolder}${txt_filename} s3://${S3BucketAndHLFolder}EFT_Files/${MF_FILENAME}" >> ${LOGNAME}
			
			aws s3 mv s3://${S3BucketAndHLFolder}${S3Files2EFTFolder}${txt_filename} s3://${S3BucketAndHLFolder}EFT_Files/${MF_FILENAME}   1>> ${LOGNAME} 2>&1

			RET_STATUS=$?

			if [[ $RET_STATUS != 0 ]]; then
				echo "" >> ${LOGNAME}
				echo "Moving S3 filename ${txt_filename} from Files2EFT to EFT_Files and renaming as ${MF_FILENAME} failed." >> ${LOGNAME}
				
				# Send Failure email	
				SUBJECT="ProcessFiles2EFT - Failed"
				MSG="Moving ${txt_filename} to EFT_Files folder and renaming as ${MF_FILENAME} failed."
				${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${EFT_EMAIL_SENDER}" "${EFT_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 

				exit 12
			fi
		
		done


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
			SUBJECT="ProcessFiles2EFT - Failed"
			MSG="Move processed ${gz_filename} to S3 Extract archive folder failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${EFT_EMAIL_SENDER}" "${EFT_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 

			exit 12
		fi	
			
	done


done <  ${DATADIR}${EFT_DRIVER_FILE} 


#############################################################
# clean-up linux data directory
#############################################################
echo "" >> ${LOGNAME} 
echo "Remove ${EFT_DRIVER_FILE} from data directory" >> ${LOGNAME} 

rm ${DATADIR}${EFT_DRIVER_FILE} 2>> ${LOGNAME} 


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "ProcessFiles2EFT.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS
