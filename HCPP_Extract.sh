#!/usr/bin/bash
#
######################################################################################
# Name:  HCPP_Extract.sh
#
# Desc: Performs Extract of HCPP data. 
#
# Input: Finder file that includes a Contract Number, Extract Year, and Contractor
#        Ex. Entry in Finder file: "H3503 2018 Bland" 	
#
# Author     : Paul Baranoski	
# Created    : 02/06/2023
#
# Modified:
#
# Paul Baranoski 2023-02-06 Created script.
# Paul Baranoski 2023-04-25 Add logic to skip blank finder file records
# Paul Baranoski 2023-05-08 Add EFT logic.
# Sean Whitelock 2025-01-14 Updated Send Success Email Failure message subject and msg to reflect the correct extract (had OFM_PDE instead of HCPP)
# Sean Whitelock 2025-01-15 Added Environment name to email success and failure message 
#
######################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/HCPP_Extract_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

FF_PREFIX=HCPP_Finder_File


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "HCPP_Extract.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}


#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh >> ${LOGNAME}

S3BUCKET=${HCPP_BUCKET} 

echo "HCPP bucket=${S3BUCKET}" >> ${LOGNAME}
echo "Finder File bucket=${FINDER_FILE_BUCKET}" >> ${LOGNAME}

source ${RUNDIR}FilenameCounts.bash


#################################################################################
# Get list of HCPP Finder files in S3
#################################################################################
echo "" >> ${LOGNAME}
echo "List HCPP Finder Files in S3 " >> ${LOGNAME}

aws s3 ls s3://${FINDER_FILE_BUCKET}${FF_PREFIX} > ${DATADIR}tempHCPP.txt  2>> ${LOGNAME}

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Listing S3 Finder Files failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="HCPP Extract - Failed ($ENVNAME)"
	MSG="Listing S3 finder files from ${FINDER_FILE_BUCKET} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${HCPP_EMAIL_SENDER}" "${HCPP_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	

FINDER_FILES=`awk '{print $4}' ${DATADIR}tempHCPP.txt`  2>> ${LOGNAME}

echo "S3 HCPP Finder Files found: ${FINDER_FILES} "  >> ${LOGNAME} 


#################################################################################
# Loop thru HCPP Finder Files
#################################################################################
for finderFile in ${FINDER_FILES} 
do

		#############################################################
		# Start extract for next Finder file record
		#############################################################
		echo " " >> ${LOGNAME}
		echo "-----------------------------------" >> ${LOGNAME}

		#############################################################
		# Copy Finder file from S3 to linux 
		#############################################################
		echo "Copy Finder File ${finderFile} from S3 to linux data directory" >> ${LOGNAME}

		aws s3 cp s3://${FINDER_FILE_BUCKET}${finderFile} ${DATADIR}${finderFile}  1>> ${LOGNAME} 2>&1

		RET_STATUS=$?

		if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Copying S3 HCPP Finder file ${finderFile} to Linux failed." >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="HCPP_Extract.sh  - Failed ($ENVNAME)"
			MSG="Copying S3 file from ${FINDER_FILE_BUCKET} failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${HCPP_EMAIL_SENDER}" "${HCPP_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
		fi	
		
		
		#################################################################################
		# Process records in Finder File
		#################################################################################
		echo "" >> ${LOGNAME}
		echo "Process Finder File records" >> ${LOGNAME}

		while read FF_RECORD
		do

			echo "" >> ${LOGNAME}
			echo "Read next Finder File record" >> ${LOGNAME}

			# Remove CR from input record.
			FF_RECORD=`echo ${FF_RECORD} | sed 's/\r//' `
			
			# skip blank lines
			if [ -z "${FF_RECORD}" ]; then
				continue
			fi
 
			# Extract parameter values from record	
			CONTRACT_NUM=`echo ${FF_RECORD} | cut -d, -f1 `  2>> ${LOGNAME}
			EXT_YR=`echo ${FF_RECORD} | cut -d, -f2 `  2>> ${LOGNAME}
			CONTRACTOR=`echo ${FF_RECORD} | cut -d, -f3 `  2>> ${LOGNAME}
			
			echo "CONTRACT_NUM=${CONTRACT_NUM}" >> ${LOGNAME}
			echo "EXT_YR=${EXT_YR}" >> ${LOGNAME}
			echo "CONTRACTOR=${CONTRACTOR}" >> ${LOGNAME}

			# Export environment variables for Python code
			export TMSTMP
			export CONTRACT_NUM
			export EXT_YR
			export CONTRACTOR

			#############################################################
			# Execute Python code to extract data.
			#############################################################
			echo "Start execution of HCPP_Extract.py program"  >> ${LOGNAME}
			${PYTHON_COMMAND} ${RUNDIR}HCPP_Extract.py >> ${LOGNAME} 2>&1


			#############################################################
			# Check the status of python script  
			#############################################################
			RET_STATUS=$?

			if [[ $RET_STATUS != 0 ]]; then
					echo "" >> ${LOGNAME}
					echo "Python script HCPP_Extract.py failed" >> ${LOGNAME}
					
					# Send Failure email	
					SUBJECT="HCPP_Extract.sh  - Failed ($ENVNAME)"
					MSG="HCPP Extract has failed."
					${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${HCPP_EMAIL_SENDER}" "${HCPP_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

					exit 12
			fi

			echo "Python script HCPP_Extract.py completed successfully. " >> ${LOGNAME}

		done <  ${DATADIR}${finderFile} 


		#############################################################
		# Move S3 Finder file to archive directory    
		#############################################################	
		echo "" >> ${LOGNAME}
		echo "Move processed S3 Finder File ${finderFile} to S3 archive folder" >> ${LOGNAME}

		aws s3 mv s3://${FINDER_FILE_BUCKET}${finderFile} s3://${FINDER_FILE_BUCKET}archive/   1>> ${LOGNAME} 2>&1

		RET_STATUS=$?

		if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Moving S3 HCPP Finder file ${finderFile} to S3 archive folder failed." >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="HCPP_Extract.sh  - Failed ($ENVNAME)"
			MSG="Moving S3 file to archive ${FINDER_FILE_BUCKET} failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${HCPP_EMAIL_SENDER}" "${HCPP_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
		fi	
		
		
		#############################################################
		# Remove Finder file from linux data directory   
		#############################################################	
		echo "Remove processed finder file ${finderFile} from linux data directory"  >> ${LOGNAME}		
		rm ${DATADIR}${finderFile} 2>> ${LOGNAME}

done


#############################################################
# Get list of S3 files for success email.
#############################################################
echo "" >> ${LOGNAME}
echo "Get S3 Extract file list and record counts" >> ${LOGNAME}

getExtractFilenamesAndCounts ${LOGNAME}  >> ${LOGNAME}  2>&1
S3Files="${filenamesAndCounts}" 


#############################################################
# Send Success email.
#############################################################
echo "" >> ${LOGNAME}
echo "Send success email with S3 Extract filename." >> ${LOGNAME}
echo "S3Files=${S3Files} "   >> ${LOGNAME}

SUBJECT="HCPP Extract - (${ENVNAME})" 
MSG="The HCPP Extract from Snowflake has completed.\n\nThe following file(s) were created:\n\n${S3Files}"

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${HCPP_EMAIL_SENDER}" "${HCPP_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Error in calling sendEmail.py" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Sending Success email in HCPP_Extract.sh  - Failed (${ENVNAME})"
	MSG="Sending Success email in HCPP_Extract.sh  has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${HCPP_EMAIL_SENDER}" "${HCPP_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	


#############################################################
# EFT Extract files
#############################################################
echo " " >> ${LOGNAME}
echo "EFT HCPP Extract Files " >> ${LOGNAME}
${RUNDIR}ProcessFiles2EFT.sh ${S3BUCKET}  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of extract script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script ProcessFiles2EFT.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="HCPP_Extract.sh  - Failed ($ENVNAME)"
	MSG="EFT HCPP Extract Files process has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${HCPP_EMAIL_SENDER}" "${HCPP_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "HCPP_Extract.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS
