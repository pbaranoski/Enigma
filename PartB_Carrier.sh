#!/usr/bin/bash
######################################################################################
# Name:  PartB_Carrier.sh
#
# Desc: Extract Part B Carrier Claims data. 
#
# Created: Sumathi Gayam  
# Modified: 06/13/2022
# 
# Paul Baranoski 2022-09-27 Added code to call CombineS3Files.sh to concatenate/combine 
#                           S3 "parts" files 
# Paul Baranoski 2023-07-26 Modify logic to get filenames and record counts for email.  
# Paul Baranoski 2024-02-01 Add ENVNAME to SUBJECT line of emails.
# Paul Baranoski 2024-02-01 Remove call to box. Add EFT functionality.
#                           Add logic to remove temp file at end of script.
# Paul Baranoski 2025-02-04  Modify Email constants to use CMS_EMAIL_SENDER and ENIGMA_EMAIL_FAILURE_RECIPIENT.
######################################################################################
set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
LOGNAME=/app/IDRC/XTR/CMS/logs/PartB_Carrier_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "PartB_Carrier.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh

source ${RUNDIR}FilenameCounts.bash

S3BUCKET=${PTB_CARR_BUCKET} 


#############################################################
# Get Previous Year 
#############################################################
last_year=`date +'%Y' -d 'last year'`

echo "Last Year =${last_year}" >> ${LOGNAME}
echo "TMSTMP=${TMSTMP}" >> ${LOGNAME}

#############################################################
# Make variables available for substitution in Python code
#############################################################
export TMSTMP
export last_year

#############################################################
# Execute Python code to load tmp table for Part B Carrier data
#############################################################
echo "" >> ${LOGNAME}
echo "Start execution of LOAD_ST_TMP_LEO_PTB_TAB.py program" >> ${LOGNAME}

${PYTHON_COMMAND} ${RUNDIR}LOAD_ST_TMP_LEO_PTB_TAB.py  1>> ${LOGNAME} 2>&1

#############################################################
# Check the status of python script - Load to PTB table
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo Python script LOAD_ST_TMP_LEO_PTB_TAB.py failed >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="PartB Carrier Load to temp table - Failed (${ENVNAME})"
		MSG="PartB Carrier load to temp table has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script LOAD_ST_TMP_LEO_PTB_TAB.py completed successfully. " >> ${LOGNAME}


#############################################################
# Execute Python code - Part B Carrier Quarterly files
#############################################################
echo "" >> ${LOGNAME}
echo "Start execution of PartB_Carrier.py program" >> ${LOGNAME}

${PYTHON_COMMAND} ${RUNDIR}PartB_Carrier.py >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo Python script PartB_Carrier.py failed >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="PartB Carrier Extract - Failed (${ENVNAME})"
		MSG="PartB Carrier extract has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script PartB_Carrier.py completed successfully. " >> ${LOGNAME}


####################################################################
# Concatenate S3 files
# NOTE: Multiple files with suffix "n_n_n.txt.gz" are created. 
#       Will concatenate them into single file.
#
# Example --> PartB_Carrier_FINAL_2021_QTR1_20220922.084321.txt.gz_0_0_0.txt.gz 
#         --> PartB_Carrier_FINAL_2021_QTR1_20220922.084321.txt.gz
####################################################################
echo "" >> ${LOGNAME}
echo "Concatenate S3 files " >> ${LOGNAME}

echo "S3BUCKET=${S3BUCKET} " >> ${LOGNAME} 

# Get list of S3 files created during this run (includes suffix)
aws s3 ls s3://${S3BUCKET} | grep ${TMSTMP} | awk '{print $4}' > ${DATADIR}PTBCarrTemp.txt 

# Remove filename suffix from "parts" filenames, and get unique list of "concatenation" filenames
# Add space after first ".gz" file extension. Then get first part of filename (delimiter is space) 
# Ex. "*.txt.gz_0_0_0.txt.gz" --> "*.txt.gz 0_0_0.txt.gz"
S3Files2Process=`cat ${DATADIR}PTBCarrTemp.txt | sed s'/\.gz_/\.gz /g' | awk '{print $1}' | uniq `

echo "S3Files2Process=${S3Files2Process}" >> ${LOGNAME}

echo "" >> ${LOGNAME}
	
# Loop over S3 Concatenation Filenames (one for each Qtr) 
for concatFilename in ${S3Files2Process}
do

	echo "Execute CombineS3Files.sh for ${concatFilename}" >> ${LOGNAME}
	${RUNDIR}CombineS3Files.sh ${S3BUCKET} ${concatFilename} 

	#############################################################
	# Check the status of script
	#############################################################
	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Shell script CombineS3Files.sh failed." >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="Combining S3 files in PartB_Carrier.sh - Failed (${ENVNAME})"
			MSG="Combining S3 files in PartB_Carrier.sh has failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
	fi	
	

done


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

SUBJECT="PartB Carrier extract (${ENVNAME})" 
MSG="The Extract for the creation of the PartB Carrier file from Snowflake has completed.\n\nThe following file(s) were created:\n\n${S3Files}"

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PECOS_EMAIL_SENDER}" "${PARTB_CARRIER_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Error in calling sendEmail.py" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Sending Success email in PartB_Carrier.sh - Failed (${ENVNAME})"
		MSG="Sending Success email in PartB_Carrier.sh  has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
fi	


##\/ Keep BOX/manifest file logic for the future
#############################################################
# Create Manifest file
#############################################################
#echo "" >> ${LOGNAME}
#echo "Create Manifest file for PartB Carrier Extract.  " >> ${LOGNAME}
#
#${RUNDIR}CreateManifestFile.sh ${S3BUCKET} ${TMSTMP} ${PARTB_CARRIER_EMAIL_SUCCESS_RECIPIENT} 
#
#
#############################################################
# Check the status of script
#############################################################
#RET_STATUS=$?
#
#if [[ $RET_STATUS != 0 ]]; then
#		echo "" >> ${LOGNAME}
#		echo "Shell script CreateManifestFile.sh failed." >> ${LOGNAME}
#		
#		# Send Failure email	
#		SUBJECT="Create Manifest file in PartB_Carrier.sh - Failed (${ENVNAME})"
#		MSG="Create Manifest file in PartB_Carrier.sh  has failed."
#		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
#
#		exit 12
#fi	
##/\ Keep BOX/manifest file logic for the future


#############################################################
# EFT Extract files
#############################################################
echo " " >> ${LOGNAME}
echo "EFT PartB Carrier Extract Files " >> ${LOGNAME}
${RUNDIR}ProcessFiles2EFT.sh ${S3BUCKET}  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of extract script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script ProcessFiles2EFT.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="PartB Carrier Extract EFT process  - Failed (${ENVNAME})"
	MSG="PartB Carrier Claim Extract EFT process has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


#############################################################
# clean-up linux data directory
#############################################################
echo "" >> ${LOGNAME} 
echo "Remove ${EXT_DT_CONFIG_FILE} from data directory" >> ${LOGNAME} 

rm ${DATADIR}PTBCarrTemp.txt 2>> ${LOGNAME} 
 

#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "PartB_Carrier.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS
