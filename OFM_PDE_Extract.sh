#!/usr/bin/bash
############################################################################################################
# Name:  OFM_PDE_Extract.sh
#
# Desc: OFM PDE Extract using finder files for various contractors
#
# Execute as ./OFM_PDE_Extract.sh 
#
# NOTE: Script must be /usr/bin/bash instead of /usr/bin/sh because the included FilenameCounts.bash file requires some bash specific syntax for 
#       a paste command which uses "bash Process Substitution". The paste command expects files and not variables as input. However, 
#       "bash Process Substitution" makes variables appear to the command as files.  The purpose of doing it this way instead of using temp files 
#       is because we have multiple scripts using the same data directory that would use the same temp file, possibly interferring with each other. 
#
#       With the EFT functionality changing the AWS name to Mainframe Name, ensure that the CONTRACTOR NAME in the finder
#       files follow the proper MF dataset name constraints. The script will abort if the name is too long. 
#
# Author     : Paul Baranoski	
# Created    : 03/24/2023
#
# Modified:
#
# Paul Baranoski 2023-03-24 Created script.
# Paul Baranoski 2023-04-04 Had wrong log file descriptions for S3 config and Finder_files folders. Corrected.
# Paul Baranoski 2023-04-26 Added code to skip blank lines in Finder files.
# Josh Turner    2023-05-11 Added EFT functionality
# Paul Baranoski 2024-07-31 Add ENV to Subject line for emails.
# Paul Baranoski 2024-08-02 Comment out EFT processing so I can process Conrad Finder File and manually create Manifest file.
#                           Remove configuration file logic.
# Paul Baranoski 2024-08-06 Add createManifestFileFunc.sh include-script to handle the create of manifest files to
#                           limit the NOF extract files to a constant value set in parent script.
# Paul Baranoski 2024-08-28 Correct syntax for TMPSTMP from '=' to ':='. 
# Paul Baranoski 2025-01-29 Remove createManifestFileFunc.sh and use of its function to control NOF files to include in a manifest file.
#                           That logic is now contained in the CreateManifestFile.sh. 
# Paul Baranosi  2025-02-06 Update script to accept parameter year override so we can run extract as if run during a prior year.
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP:=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/OFM_PDE_Extract_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

FF_PREFIX=OFM_PDE_Finder_File

#############################################################
# Variables for extracting logfile entries for success emails
#############################################################
LOG_FROM_LINE=1
LOG_TO_LINE=1
TMP_OFM_PDE_FF_LOGFILE=tmpOFM_PDE_FFLOG.txt
TMP_OFM_PDE_FF_LIST=tempOFMPDE.txt


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "OFM_PDE_Extract.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}


#############################################################
# Verify that required NOF parameters have been sent 
#############################################################
if ! [[ $# -eq 0 || $# -eq 1  ]]
then
	echo "Incorrect # of parameters sent to script. NOF parameters: $#" >> ${LOGNAME}
	exit 12
fi

#############################################################
# Get override extract dates if passed 
#############################################################
P_CURR_YYYY=$1

echo "Parameters to script: " >> ${LOGNAME}
echo "   P_CURR_YYYY=${P_CURR_YYYY} " >> ${LOGNAME}


#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh

S3BUCKET=${OFM_PDE_BUCKET} 
echo "OFM PDE Extract bucket=${S3BUCKET}" >> ${LOGNAME}
echo "configuration file bucket=${CONFIG_BUCKET}" >> ${LOGNAME}
echo "Finder files bucket=${FINDER_FILE_BUCKET}" >> ${LOGNAME}

#############################################################
# Include common script modules 
#############################################################
source ${RUNDIR}FilenameCounts.bash


#################################################################################
# Calculate extract run dates
#################################################################################
echo "" >> ${LOGNAME}

CUR_YYYY=${P_CURR_YYYY:=`date +%Y`} 

CLM_EFCTV_DT=${CUR_YYYY}-06-30  2>> ${LOGNAME}
echo "CLM_EFCTV_DT=${CLM_EFCTV_DT}" >> ${LOGNAME}

CLM_PRIOR_YYYY=`expr ${CUR_YYYY} - 1 `  2>> ${LOGNAME}
echo "CLM_PRIOR_YYYY=${CLM_PRIOR_YYYY}" >> ${LOGNAME}


#################################################################################
# Get list of OFM PDE Finder Files in S3.
#################################################################################
echo "" >> ${LOGNAME}
echo "List OFM PDE Finder Files in S3 " >> ${LOGNAME}

aws s3 ls s3://${FINDER_FILE_BUCKET}${FF_PREFIX} > ${DATADIR}${TMP_OFM_PDE_FF_LIST}  2>> ${LOGNAME}

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Listing S3 Finder Files failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="OFM PDE Extract - Failed (${ENVNAME})"
	MSG="Listing S3 finder files from ${FINDER_FILE_BUCKET} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${OFM_PDE_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	

FINDER_FILES=`awk '{print $4}' ${DATADIR}${TMP_OFM_PDE_FF_LIST}`  2>> ${LOGNAME}

echo "S3 OFM PDE Finder Files found: ${FINDER_FILES} "  >> ${LOGNAME} 


#################################################################################
# Loop thru OFM PDE Finder Files
#################################################################################
for finderFile in ${FINDER_FILES} 
do

		#############################################################
		# Start extract for next Finder file record
		#############################################################
		echo " " >> ${LOGNAME}
		echo "-----------------------------------" >> ${LOGNAME}
	    echo "Processing ${finderFile}" >> ${LOGNAME}		

	
		#################################################
		# Save logfile start line num for current FF
		# NOTE: This is used to extract filenames and 
		#       record counts for current respective success emails.	
		#################################################
		echo "" >> ${LOGNAME}
		
		LOG_FROM_LINE=`wc -l ${LOGNAME} | awk '{print $1}' `  2>> ${LOGNAME}
		echo "LOG_FROM_LINE=${LOG_FROM_LINE}" >> ${LOGNAME}

		
		#############################################################
		# Copy Finder file from S3 to linux 
		#############################################################
		echo "Copy Finder File ${finderFile} from S3 to linux data directory" >> ${LOGNAME}

		aws s3 cp s3://${FINDER_FILE_BUCKET}${finderFile} ${DATADIR}${finderFile}  1>> ${LOGNAME} 2>&1

		RET_STATUS=$?

		if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Copying S3 OFM PDE Finder file ${finderFile} to Linux failed." >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="OFM_PDE_Extract.sh  - Failed (${ENVNAME})"
			MSG="Copying S3 file from ${FINDER_FILE_BUCKET} failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${OFM_PDE_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
		fi	

	
		#################################################################################
		# Extract CONTRACTOR/Mailbox from Finder File filename
		# Ex. OFM_PDE_Finder_File_Bland_20230324.145911 --> "Bland"
        #
        # If EFT'ing file: be sure to verify the CONTRACTOR/Mailbox is not too long for the Mainframe
		#################################################################################
		echo " " >> ${LOGNAME}
		echo "Extract CONTRACTOR Name from Finder File filename" >> ${LOGNAME}
		
		CONTRACTOR=`echo ${finderFile} | cut -d_ -f5`  2>> ${LOGNAME}
		echo "CONTRACTOR=${CONTRACTOR}" >> ${LOGNAME}

        CONTRACTOR_LEN=`expr length "$CONTRACTOR"`
		if [[ $CONTRACTOR_LEN -gt 8 ]]; then
			echo "" >> ${LOGNAME}
			echo "Finder file CONTRACTOR length check failed." >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="OFM_PDE_Extract.sh  - Failed (${ENVNAME})"
			MSG="The contractor name length for finder file ${finderFile} is too long with length: ${CONTRACTOR_LEN}. Please check all finder file names."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${OFM_PDE_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
		fi


		#################################################################################
        # Set Box email recipients based on Contractor	
		#################################################################################
		echo " " >> ${LOGNAME}
		echo "Set appropriate Contractor/Mailbox recipient  " >> ${LOGNAME}
		
		# Set Box recipient for Contractor/Mailbox
		case "${CONTRACTOR}" in 
		
			 "BLAND")
				BOX_RECIPIENT=${OFM_PDE_BLAND_BOX_RECIPIENT} 
				;;
			 "CGI")
				BOX_RECIPIENT=${OFM_PDE_CGI_BOX_RECIPIENT} 
				;;
			 "MHM")
				BOX_RECIPIENT=${OFM_PDE_MHM_BOX_RECIPIENT} 
				;;
			 "DJLLC")
				BOX_RECIPIENT=${OFM_PDE_DJLLC_BOX_RECIPIENT} 
				;;
			 "CONRAD")
				BOX_RECIPIENT=${OFM_PDE_CONRAD_BOX_RECIPIENT} 
				;;
			 *)

				echo "" >> ${LOGNAME}
				echo "CONTRACTOR ${CONTRACTOR} box email recipients are not set-up. Skip processing of contractor finder file. Make appropriate coding changes." >> ${LOGNAME}
				
				# Send Failure email	
				SUBJECT="OFM_PDE_Extract.sh  - Warning (${ENVNAME})"
				MSG="CONTRACTOR ${CONTRACTOR} box email recipients are not set-up. Skip processing of contractor finder file. Make appropriate coding changes."
				${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${OFM_PDE_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
			
				# Process next Finder File
				continue
				;;
		esac
		
		echo "BOX_RECIPIENT=${BOX_RECIPIENT}" >> ${LOGNAME}


		#################################################################################
		# Create Timestamp for Extract files for this BOX Recipient
		#################################################################################
		EXTRACT_FILE_TMSTMP=`date +%Y%m%d.%H%M%S`
		echo "EXTRACT_FILE_TMSTMP=${EXTRACT_FILE_TMSTMP}" >> ${LOGNAME}

		
		#################################################################################
		# Process records in Finder File
		#################################################################################
		echo "" >> ${LOGNAME}
		echo "Process records in ${finderFile}" >> ${LOGNAME}
	
		#################################################
		# Process Finder File Request records
		#################################################
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
			PBP_NUM=`echo ${FF_RECORD} | cut -d, -f2 `  2>> ${LOGNAME}

			echo "CONTRACT_NUM=${CONTRACT_NUM}" >> ${LOGNAME}
			echo "PBP_NUM=${PBP_NUM}" >> ${LOGNAME}

			# Export environment variables for Python code
			export EXTRACT_FILE_TMSTMP
			export CLM_PRIOR_YYYY
			export CLM_EFCTV_DT
			export CONTRACTOR
			export CONTRACT_NUM
			export PBP_NUM

			#############################################################
			# Execute Python code to extract data.
			#############################################################
			echo "Start execution of OFM_PDE_Extract.py program"  >> ${LOGNAME}
			${PYTHON_COMMAND} ${RUNDIR}OFM_PDE_Extract.py >> ${LOGNAME} 2>&1


			#############################################################
			# Check the status of python script  
			#############################################################
			RET_STATUS=$?

			if [[ $RET_STATUS != 0 ]]; then
					echo "" >> ${LOGNAME}
					echo "Python script OFM_PDE_Extract.py failed" >> ${LOGNAME}
					
					# Send Failure email	
					SUBJECT="OFM_PDE_Extract.sh  - Failed (${ENVNAME})"
					MSG="OFM PDE Extract has failed."
					${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${OFM_PDE_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

					exit 12
			fi

			echo "Python script OFM_PDE_Extract.py completed successfully. " >> ${LOGNAME}
		

		done <  ${DATADIR}${finderFile} 


		#############################################################
		# Create Manifest file.
		#############################################################		
		echo "" >> ${LOGNAME}
		echo "Creating manifest file for OFM PDE Request Extract.  " >> ${LOGNAME}
		
		${RUNDIR}CreateManifestFile.sh ${S3BUCKET} ${EXTRACT_FILE_TMSTMP} ${BOX_RECIPIENT} 


		#############################################################
		# Check the status of script
		#############################################################
		RET_STATUS=$?

		if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Shell script CreateManifestFile.sh failed." >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="Create Manifest file in OFM_PDE_Extract.sh - Failed ($ENVNAME)"
			MSG="Create Manifest file in OFM_PDE_Extract.sh has failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
		fi			

			
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
		sed -n "${LOG_FROM_LINE},${LOG_TO_LINE}p" ${LOGNAME} > ${DATADIR}${TMP_OFM_PDE_FF_LOGFILE}


		#############################################################
		# Get list of S3 files for success email.
		#############################################################
		echo "" >> ${LOGNAME}
		echo "Get S3 Extract file list and record counts" >> ${LOGNAME}

		#getExtractFilenamesAndCounts ${LOGDIR}${LOGNAME} >> ${LOGNAME}  2>&1
		getExtractFilenamesAndCounts ${DATADIR}${TMP_OFM_PDE_FF_LOGFILE} >> ${LOGNAME}  2>&1
		S3Files="${filenamesAndCounts}" 

		echo "" >> ${LOGNAME}
		S3Files=`echo "${S3Files}" ` >> ${LOGNAME}  2>&1


		#############################################################
		# Check the status of script
		#############################################################
		RET_STATUS=$?

		if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Shell script CreateManifestFile.sh failed." >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="Create Manifest file in OFM_PDE_Extracts.sh  - Failed ($ENVNAME)"
			MSG="Create Manifest file in OFM_PDE_Extracts.sh  has failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
		fi


		#############################################################
		# Send Success email.
		#############################################################
		echo "" >> ${LOGNAME}
		echo "Send success email." >> ${LOGNAME}

		# Add Box recipients to Success email recipients	
		SUCCESS_EMAIL_RECIPIENT="${OFM_PDE_EMAIL_SUCCESS_RECIPIENT},${BOX_RECIPIENT}"
			
		SUBJECT="OFM_PDE Extract - completed (${ENVNAME})" 
		MSG="OFM_PDE Extract completed for request file ${finderFile}.\n\n\nThe following extract file(s) were created:\n\n${S3Files}"

		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${SUCCESS_EMAIL_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Error in calling sendEmail.py" >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="Sending Success email in OFM_PDE_Extract.sh  - Failed (${ENVNAME})"
			MSG="Sending Success email in OFM_PDE_Extract.sh  has failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${OFM_PDE_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
		fi	
		
		
		#############################################################
		# Move S3 Finder file to archive directory    
		#############################################################	
		echo "" >> ${LOGNAME}
		echo "Move processed S3 Finder File ${finderFile} to S3 archive folder" >> ${LOGNAME}

		aws s3 mv s3://${FINDER_FILE_BUCKET}${finderFile} s3://${FINDER_FILE_BUCKET}archive/   1>> ${LOGNAME} 2>&1

		RET_STATUS=$?

		if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Moving S3 OFM PDE Finder file ${finderFile} to S3 archive folder failed." >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="OFM_PDE_Extract.sh  - Failed (${ENVNAME})"
			MSG="Moving S3 file to archive ${FINDER_FILE_BUCKET} failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${OFM_PDE_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
		fi	
		
		
		#############################################################
		# Remove Finder file from linux data directory   
		#############################################################	
		echo "Remove processed finder file ${finderFile} from linux data directory"  >> ${LOGNAME}		
		rm ${DATADIR}${finderFile} 2>> ${LOGNAME}

		# allow snowflake to clear up connections
		sleep 30s
		
done


#############################################################
# clean-up linux data directory
#############################################################
echo "" >> ${LOGNAME} 
echo "Remove temp files from data directory" >> ${LOGNAME} 

rm ${DATADIR}${TMP_OFM_PDE_FF_LIST} 2>> ${LOGNAME}
rm ${DATADIR}${TMP_OFM_PDE_FF_LOGFILE} 2>> ${LOGNAME}


#############################################################
# End script
#############################################################
echo "" >> ${LOGNAME}
echo "OFM_PDE_Extract.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

exit $RET_STATUS