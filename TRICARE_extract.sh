#!/usr/bin/bash
############################################################################################################
# Name:  TRICARE_extract.sh
#
# Desc: TRICARE Extract
#
# Author     : Paul Baranoski	
# Created    : 09/13/2023
#
# Modified:
#
# Paul Baranoski 2023-09-13 Created script.
#
# Paul Baranoski   2023-11-28  Add ENVNAME to email Subject line.
# Paul Baranoski   2023-12-18  Modify script to gracefully end with RC=4 when no Finder Files are found.
#                              Update emails to add ENVNAME to subject line.
# Paul Baranoski   2025-01-29  Update CMS_EMAIL_SENDER to CMS_EMAIL_SENDER.
#                              Update ENIGMA_EMAIL_FAILURE_RECIPIENT to ENIGMA_EMAIL_FAILURE_RECIPIENT.
# Paul Baranoski   2025-02-04  Modify Email constants to use CMS_EMAIL_SENDER and ENIGMA_EMAIL_FAILURE_RECIPIENT.
# Paul Baranoski   2025-08-14  Add EFT filename mask info to success email.
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/TRICARE_extract_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "TRICARE_extract.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh

source ${RUNDIR}FilenameCounts.bash

S3BUCKET=${TRICARE_BUCKET} 

echo "TRICARE bucket=${S3BUCKET}" >> ${LOGNAME}
echo "Finder files bucket=${FINDER_FILE_BUCKET}" >> ${LOGNAME}


#################################################################################
# Remove any residual TRICARE Finder files in data directory.
#################################################################################
echo " " >> ${LOGNAME}
echo "Remove any residual Finder Files in data directory." >> ${LOGNAME}

rm ${DATADIR}tempTRICARE.txt >> ${LOGNAME} 2>&1


#############################################################
# Execute Script to load TRICARE Finder File table in SF
#############################################################
echo "" >> ${LOGNAME}
echo "Execute script LOAD_TRICARE_FNDR_FILE.sh"  >> ${LOGNAME}
${RUNDIR}LOAD_TRICARE_FNDR_FILE.sh   2>> ${LOGNAME}


#############################################################
# Check the status of script 
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then

	if [[ $RET_STATUS -eq 4 ]]; then
		echo "" >> ${LOGNAME}
		echo "LOAD_TRICARE_FNDR_FILE.sh ended. No Finder Files found." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="LOAD_TRICARE_FNDR_FILE.sh ended. No Finder Files found. (${ENVNAME})"
		MSG="LOAD_TRICARE_FNDR_FILE.sh ended. No Finder Files found."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 4	
	else
		echo "" >> ${LOGNAME}
		echo "LOAD_TRICARE_FNDR_FILE.sh failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="LOAD_TRICARE_FNDR_FILE.sh  - Failed (${ENVNAME})"
		MSG="LOAD_TRICARE_FNDR_FILE.sh has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi	
fi

echo "" >> ${LOGNAME}
echo "LOAD_TRICARE_FNDR_FILE.sh completed successfully. " >> ${LOGNAME}


#############################################################
# Execute Python code to extract TRICARE data.
#############################################################
echo "" >> ${LOGNAME}
echo "Start execution of TRICARE_extract.py program"  >> ${LOGNAME}

# Export environment variables for Python code
export TMSTMP

${PYTHON_COMMAND} ${RUNDIR}TRICARE_extract.py >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script  
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script TRICARE_extract.py failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="TRICARE_extract.sh  - Failed (${ENVNAME})"
		MSG="TRICARE Extract has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script TRICARE_extract.py completed successfully. " >> ${LOGNAME}


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

SUBJECT="Weekly TRICARE extract (${ENVNAME})" 
MSG="The Extract for the creation of the weekly TRICARE file from Snowflake has completed.\n\nThe following file(s) were created:\n\n${S3Files}\n\nAn EFT version of the file was created using the following file mask P#EFT.ON.V0067.RSP{SPLTNO}.{TIMESTAMP}."

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${TRICARE_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Error in calling sendEmail.py" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Sending Success email in TRICARE_extract.sh  - Failed (${ENVNAME})"
		MSG="Sending Success email in TRICARE_extract.sh  has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
fi	


#############################################################
# Move Finder File(s) processed to archive directory
#
# NOTE: The tempTRICARE.txt file was created in LOAD_TRICARE_FNDR_FILE.sh
#       and contains the list of S3 TRICARE Finder Files
#############################################################
echo "" >> ${LOGNAME}
echo "Display list of TRICARE Finder Files in S3." >> ${LOGNAME}

# Extract just the filenames from the S3 filename information
S3FinderFilenames=`awk '{print $4}' ${DATADIR}tempTRICARE.txt` 

echo "TRICARE S3 Finder files found: ${S3FinderFilenames}" >> ${LOGNAME}


#################################################################################
# Move TRICARE Finder Files in S3 to archive directory.
#################################################################################
echo "" >> ${LOGNAME}
echo "Move processed S3 TRICARE Finder Files to archive directory in S3." >> ${LOGNAME}

for S3FinderFilename in ${S3FinderFilenames}
do
	aws s3 mv s3://${FINDER_FILE_BUCKET}${S3FinderFilename} s3://${FINDER_FILE_BUCKET}archive/${S3FinderFilename}   1>> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Moving S3 TRICARE Finder file to S3 archive folder failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="TRICARE_extract.sh  - Failed (${ENVNAME})"
		MSG="Moving S3 finder file ${S3FinderFilename} from ${FINDER_FILE_BUCKET} to S3 archive folder failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi	

done


#############################################################
# Download S3 extract file to linux (for EFT processing)
#############################################################
gz_filename=${COPY_INTO_FILENAMES}

echo "" >> ${LOGNAME}
echo "gz_filename=${gz_filename}"  >> ${LOGNAME} 
echo "Download S3 extract file ${gz_filename} to linux"  >> ${LOGNAME}

aws s3 cp s3://${S3BUCKET}${gz_filename} ${DATADIR}${gz_filename}  1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Copying S3 file ${S3BUCKET}${gz_filename} to Linux failed." >> ${LOGNAME}
	
	# Send Failure email
	SUBJECT="ProcessFiles2EFT.sh  - Failed (${ENVNAME})"
	MSG="Copying S3 file ${S3BUCKET}${gz_filename} to Linux failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


#############################################################
# unzip gz file on linux; get new filename
#############################################################
echo "" >> ${LOGNAME}
echo "Unzip ${gz_filename} file on linux"  >> ${LOGNAME}

gzip -d   ${DATADIR}${gz_filename} 2>> ${LOGNAME}
		
txt_filename=`echo ${gz_filename} | sed s/.gz// ` 2>> ${LOGNAME}
echo "Unzipped text filename ${txt_filename} file on linux"  >> ${LOGNAME}


#############################################################
# Split extract file into 4 files for EFT process
#############################################################
num_split_files=4

echo "" >> ${LOGNAME}
echo "Split file ${txt_filename} into ${num_split_files} files "  >> ${LOGNAME}

total_lines=`cat ${DATADIR}${txt_filename} | wc -l ` 2>> ${LOGNAME}
echo "total_lines: ${total_lines}"          >> ${LOGNAME}

((lines_per_file = ($total_lines + $num_split_files - 1) / $num_split_files))
echo "lines_per_file: ${lines_per_file} " >> ${LOGNAME}

# Split the actual file into 4 separate files for EFT processing. 
split --numeric-suffixes=1  --lines=${lines_per_file} -a 1 ${DATADIR}/${txt_filename} ${DATADIR}/${txt_filename}_  2>> ${LOGNAME}

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script TRICARE_extract.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT=" TRICARE split extract file  - Failed (${ENVNAME})"
	MSG=" TRICARE split extract file failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


#################################################################################
# Get List of TRICARE Split files on linux
#################################################################################
echo "" >> ${LOGNAME}
echo "Get list of TRICARE Split Files on linux" >> ${LOGNAME}

pathAndTxtSplitFilenames=`ls ${DATADIR}${txt_filename}_* ` 2>> ${LOGNAME}
echo "pathAndTxtSplitFilenames=${pathAndTxtSplitFilenames}" >> ${LOGNAME}


#################################################################################
# Move TRICARE Split Extract files from linux to S3 Extract folder.
#################################################################################
echo "" >> ${LOGNAME}
echo "Move TRICARE Split Files on linux to S3 Extract folder" >> ${LOGNAME}

for pathAndFilename in ${pathAndTxtSplitFilenames}
do
	# zip split file
	gzip ${pathAndFilename} >> ${LOGNAME}
	
	gzSplitFilename=`basename ${pathAndFilename}.gz`
	echo "gzSplitFilename: ${gzSplitFilename}" >> ${LOGNAME}

	aws s3 mv ${DATADIR}${gzSplitFilename} s3://${S3BUCKET}${gzSplitFilename}   1>> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Moving TRICARE Split file ${txtSplitFilename} from linux to S3 extract folder failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="TRICARE_extract.sh  - Failed (${ENVNAME})"
		MSG="Moving TRICARE Split file ${txtSplitFilename} from linux to S3 extract folder failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi	

done

#################################################################################
# Move TRICARE Extract file to archive folder.
#################################################################################
aws s3 mv s3://${S3BUCKET}${gz_filename} s3://${S3BUCKET}archive/${gz_filename}   1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Moving S3 TRICARE Extract file to S3 archive folder failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="TRICARE_extract.sh  - Failed (${ENVNAME})"
	MSG="Moving S3 TRICARE Extract file to S3 archive folder failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	


#############################################################
# EFT Extract files
#############################################################
echo " " >> ${LOGNAME}
echo "EFT TRICARE Extract file " >> ${LOGNAME}
${RUNDIR}ProcessFiles2EFT.sh ${S3BUCKET}  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of extract script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script ProcessFiles2EFT.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT=" TRICARE EFT process  - Failed (${ENVNAME})"
	MSG=" TRICARE EFT process has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME} 
echo "Remove tempTRICARE.txt from data directory" >> ${LOGNAME} 

rm ${DATADIR}tempTRICARE.txt >> ${LOGNAME} 2>&1
rm ${DATADIR}TRICARE_EXTRACT_* >> ${LOGNAME} 2>&1


#############################################################
# end script
#############################################################
echo "" >> ${LOGNAME}
echo "TRICARE_extract.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS