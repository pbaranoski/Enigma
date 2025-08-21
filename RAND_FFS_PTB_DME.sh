#!/usr/bin/bash
############################################################################################################
# Name: RAND_FFS_PTB_DME.sh
# Desc: RAND FFS Part B Extract for DME
#
# Author     : Joshua Turner	
# Created    : 3/3/2023
#
# Modified:
# Joshua Turner 	2023-03-03	New script.
# Paul Baranoski    2024-03-12 Modify logic for extract year to be 2 years prior to current year.
#                              Add call to create manifest file.
#                              Add ENVNAME to SUBJECT of all emails.
#                              Add ${TMSTMP} to temp_RAND_PARTA_Files_${TMSTMP}.txt. When Part A or B jobs
#                              are run concurrently, the later job over-writes the temp file. The presence 
#                              of the timestamp will all for jobs to be run concurrently.
############################################################################################################
set +x

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/
#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
LOGNAME=/app/IDRC/XTR/CMS/logs/RAND_FFS_PTB_DME_${TMSTMP}.log
touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "RAND_FFS_PTB_DME.sh started at ${TMSTMP} " >> ${LOGNAME}
echo "" >> ${LOGNAME}

source ${RUNDIR}SET_XTR_ENV.sh

source ${RUNDIR}FilenameCounts.bash

S3BUCKET=${RAND_FFSPTAB_BUCKET} 

echo "RAND FFS_PTAB bucket=${S3BUCKET}" >> ${LOGNAME}


###########################################################################################
# Establish parameters and call RAND_FFS_PartB_Extract.py with the proper CTYP and 
# CLM_TYPE_CD range
###########################################################################################
CURR_YEAR=`date +%Y`
EXT_YEAR=`expr ${CURR_YEAR} - 2`
CTYP="DME"
CLM_TYPE_CD1="81"
CLM_TYPE_CD2="82"

echo "-----------------------------------------------------------------" >> ${LOGNAME}
echo "CLM_TYPE_LIT=${CTYP}" >> ${LOGNAME}
echo "EXT_CLM_TYPES=${CLM_TYPE_CD1}, ${CLM_TYPE_CD2}" >> ${LOGNAME}
echo "EXT_YEAR=${EXT_YEAR}" >> ${LOGNAME}
echo "-----------------------------------------------------------------" >> ${LOGNAME}

# Export environment variables for Python code
export TMSTMP
export CLM_TYPE_CD1
export CLM_TYPE_CD2
export EXT_YEAR
export CTYP
export DATADIR

#############################################################
# Execute Python code to extract data.
#############################################################
echo "" >> ${LOGNAME}
echo "Start execution of RAND_FFS_PartB_Extract.py program"  >> ${LOGNAME}
${PYTHON_COMMAND} ${RUNDIR}RAND_FFS_PartB_Extract.py >> ${LOGNAME} 2>&1

#############################################################
# Check the status of python script  
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Python script RAND_FFS_PartB_Extract.py failed" >> ${LOGNAME}
fi

echo "" >> ${LOGNAME}
echo "Python script RAND_FFS_PartB_Extract.py completed successfully for DME. " >> ${LOGNAME}
		
###########################################################################################
# Loop through the extract file list and call combineFiles.sh for each 
# Note: this step will likely take the bulk of processing time for the script
###########################################################################################
echo "##########################################" >> ${LOGNAME}
echo "Reading extract file list and calling CombineS3Files.sh" >> ${LOGNAME}

RAND_FILE_LIST=temp_RAND_PARTB_Files_${TMSTMP}.txt	
FILE_LIST=`ls ${DATADIR}${RAND_FILE_LIST}` 1>> ${LOGNAME}  2>&1
FILE_PREFIX=`cat ${FILE_LIST} | tr -d '\r' `

for EXT in $(echo $FILE_PREFIX ) 
do
	${RUNDIR}CombineS3Files.sh ${RAND_FFSPTAB_BUCKET} ${EXT} >> ${LOGNAME} 2>&1

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "RAND_FFS_PTB.DME.sh failed during the combine step" >> ${LOGNAME}

		#Send Failure email	
		SUBJECT="RAND FFS Part B DME Extract - Failed (${ENVNAME})"
		MSG="RAND_FFS_PTB_DME.sh failed while combining S3 files."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${RAND_FFS_EMAIL_SENDER}" "${RAND_FFS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
		exit 12
	fi
	
done


###########################################################################################
# Delete any temp RAND PART B file list files from DATADIR
###########################################################################################
echo "" >> ${LOGNAME}
echo "Delete temp file that contains Combine file names" >> ${LOGNAME}

rm ${DATADIR}temp_RAND_PARTB_Files_${TMSTMP}.txt >> ${LOGNAME} 2>&1


###########################################################################################
# Call getExtractFilenamesAndCounts() from FilenameCounts.bash to get all files created
# and the counts for each file to send in the success email
###########################################################################################
echo "" >> ${LOGNAME}
echo "Get S3 Extract file list and record counts." >> ${LOGNAME}

getExtractFilenamesAndCounts ${LOGNAME} >> ${LOGNAME} 2>&1
FILE_LIST="${filenamesAndCounts}"

###########################################################################################
# Send Success Email
###########################################################################################
echo "" >> ${LOGNAME}
echo "Sending success email with filenames." >> ${LOGNAME}

#Send Failure email	
SUBJECT="RAND FFS Part B DME Extract - COMPLETE (${ENVNAME})"
MSG="RAND_FFS_PTB_DME.sh completed successfully.\n\nThe following file(s) were created:\n\n${FILE_LIST}"
${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${RAND_FFS_EMAIL_SENDER}" "${RAND_FFS_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "RAND_FFS_PTB_DME.sh failed - Error in calling sendEmail.py" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="RAND_FFS_PTB_DME.sh failed - Error in calling sendEmail.py (${ENVNAME})"
	MSG="Sending Success email in RAND_FFS_PTB_DME.sh  has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${RAND_FFS_EMAIL_SENDER}" "${RAND_FFS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


#############################################################
# Create Manifest file
#############################################################
echo "" >> ${LOGNAME}
echo "Create Manifest file for RAND FFS Extract.  " >> ${LOGNAME}

${RUNDIR}CreateManifestFile.sh ${S3BUCKET} ${TMSTMP} ${RAND_FFS_BOX_RECIPIENTS} ${MANIFEST_HOLD_BUCKET} 


#############################################################
# Check the status of script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Shell script CreateManifestFile.sh failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Create Manifest file in RAND_FFS_PTA_DME.sh - Failed (${ENVNAME})"
		MSG="Create Manifest file in RAND_FFS_PTA_DME.sh has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${RAND_FFS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
fi


###########################################################################################
# End script
###########################################################################################
echo "" >> ${LOGNAME}
echo "RAND_FFS_PTB_DME.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

exit $RET_STATUS