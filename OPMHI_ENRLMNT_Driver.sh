#!/usr/bin/bash
############################################################################################################
# Script Name: OPMHI_ENRLMNT_Driver.sh
# Description: This script executes the SQL python script for OPM-HI ENRLMNT 
#
# Created: Joshua Turner
# Modified: 06/13/2023
#
# 10/03/2023   Paul Baranoski       Modified extract to use BOX delivery. Added logic to build
#                                   client-specific EXT_FILENAME.
#                                   Change call to CreateManifestFile.sh to use CUR_DT instead of TMPSTMP because
#                                   client doesn't appear to want timestamp appended to end of extract files.
# 10/10/2023   Paul Baranoski       Add code to pass additional parameter (flag) to CreateManifestFile.sh to 
#                                   indicate that pgm wants Manifest file placed into an S3 "staging" folder
# 10/12/2023   Paul Baranoski       Remove code to pass additional parameter. This logic will be handled 
#                                   within CreateManifestFile.sh using a Manifest Configuration file.
# 08/08/2024   Paul Baranoski       Add ENV to Subject line for emails.
############################################################################################################
set +x
#################################################################################
# Establish log file  
#################################################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
CUR_DT=`date +%Y%m%d`
LOGNAME=/app/IDRC/XTR/CMS/logs/OPMHI_ENRLMNT_Driver_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

##############################################################################
# This flag determines if Manifest file will be written to S3/manifest_files 
#  or S3/manifest_files_hold
##############################################################################
touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

#################################################################################
# Included to produce filenames and counts from the extracts
#################################################################################
source ${RUNDIR}FilenameCounts.bash

echo "################################### " >> ${LOGNAME}
echo "OPMHI_ENRLMNT_Driver.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#################################################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#################################################################################
source ${RUNDIR}SET_XTR_ENV.sh
S3BUCKET=${OPMHI_ENRLMNT_BUCKET}

#################################################################################
# Clean linux data directory of any residual files
#################################################################################
echo "" >> ${LOGNAME}
echo "Remove any residual FEHB_CMS_ENR_* files on linux data directory" >> ${LOGNAME}
rm ${DATADIR}FEHB_CMS_ENR_*   2>> ${LOGNAME}


#################################################################################
# Set date parameters for previous quarter
#################################################################################
echo "" >> ${LOGNAME}
echo "Calculate parameters based on current calendar quarter" >> ${LOGNAME}

CURR_YR=`date +%Y`
CAL_QTR=$(( ($(date +%-m)-1)/3+1 ))
echo "Current calculated CAL_QTR=${CAL_QTR}"  >> ${LOGNAME}

if [ $CAL_QTR = "1" ]; then
	EXT_YEAR=`expr ${CURR_YR} - 1`
	START_DATE="${EXT_YEAR}-10-01"
	END_DATE="${EXT_YEAR}-12-31"
	EXT_QTR="4"
elif [ $CAL_QTR = "2" ]; then
	EXT_YEAR=${CURR_YR}
	START_DATE="${CURR_YR}-01-01"
	END_DATE="${CURR_YR}-03-31"
	EXT_QTR="1"
elif [ $CAL_QTR = "3" ]; then
	EXT_YEAR=${CURR_YR}
	START_DATE="${CURR_YR}-04-01"
	END_DATE="${CURR_YR}-06-30"
	EXT_QTR="2"
elif [ $CAL_QTR = "4" ]; then
	EXT_YEAR=${CURR_YR}
	START_DATE="${CURR_YR}-07-01"
	END_DATE="${CURR_YR}-09-30"
	EXT_QTR="3"
fi


#################################################################################
# Set and export other parameters for the SQL
#################################################################################
CTYP="ENR"
STAGE_NAME="OPMHIENRLMNT"

# Build Extract Filename. Remove dashes in date.
echo "" >> ${LOGNAME}
echo "Build Extract filename" >> ${LOGNAME}
EXT_FILENAME=`echo "FEHB_CMS_${CTYP}_${START_DATE}_${END_DATE}_${CUR_DT}.txt.gz" | sed 's/-//g' `  2>> ${LOGNAME}


export EXT_YEAR
export EXT_QTR
export START_DATE
export END_DATE
export STAGE_NAME
export TMSTMP
export CTYP
export EXT_FILENAME

echo "" >> ${LOGNAME}
echo "OPM-HI ENRLMNT Extract is starting with the following parameters:" >> ${LOGNAME}
echo "CLAIM TYPE: ${CTYP}" >> ${LOGNAME}
echo "START DATE: ${START_DATE}" >> ${LOGNAME}
echo "END DATE: ${END_DATE}" >> ${LOGNAME}
echo "S3 Bucket: ${S3BUCKET}" >> ${LOGNAME}
echo "EXT_FILENAME: ${EXT_FILENAME}" >> ${LOGNAME}


#################################################################################
# Execute Python code to produce ENRLMNT file
#################################################################################
echo "" >> ${LOGNAME}
echo "Start execution of OPMHI_ENRLMNT_Extract.py program"  >> ${LOGNAME}

${PYTHON_COMMAND} ${RUNDIR}OPMHI_ENRLMNT_Extract.py >> ${LOGNAME} 2>&1

#################################################################################
# Check the status of python script 
#################################################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script OPMHI_ENRLMNT_Extract.py failed" >> ${LOGNAME}
		
        # Send Failure email	
        SUBJECT="OPMHI_ENRLMNT_Driver.sh - Failed (${ENVNAME})"
        MSG="OPM-HI ENRLMNT extract has failed."
        ${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${OPMHI_EMAIL_SENDER}" "${OPMHI_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script OPMHI_ENRLMNT_Extract.py completed successfully. " >> ${LOGNAME}

###########################################################################################
# Call combineS3Files.sh to combine all file parts
###########################################################################################
echo "" >> ${LOGNAME}
echo "Calling combine files script" >> ${LOGNAME}

${RUNDIR}CombineS3Files.sh ${S3BUCKET} ${EXT_FILENAME}

RET_STATUS=$?

if [ $RET_STATUS != 0 ]; then
         echo "" >> ${LOGNAME}
         echo "Shell script CombineS3Files.sh failed" >> ${LOGNAME}

         # Send Failure email	
         SUBJECT="OPMHI_ENRLMNT_Driver.sh - Failed (${ENVNAME})"
         MSG="CombineS3Files.sh for OPM-HI PTB ENRLMNT has failed."
         ${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${OPMHI_EMAIL_SENDER}" "${OPMHI_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

         exit 12
fi

###########################################################################################
# Get a list of all S3 files for success email
###########################################################################################
echo "" >> ${LOGNAME}
echo "Get S3 Extract file list" >> ${LOGNAME}

getExtractFilenamesAndCounts ${LOGNAME}  >> ${LOGNAME}  2>&1
S3Files="${filenamesAndCounts}" 


#################################################################################
# Send success email
#################################################################################
echo "" >> ${LOGNAME}
echo "Send success email with S3 Extract filename." >> ${LOGNAME}
echo "S3Files=${S3Files} "   >> ${LOGNAME}

SUBJECT="OPMHI_ENRLMNT_Driver.sh  - Completed (${ENVNAME})"
MSG="OPM-HI ENROLLMENT has completed successfully.\n\nThe following file(s) were created:\n\n${S3Files}"

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${OPMHI_EMAIL_SENDER}" "${OPMHI_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Error in calling sendEmail.py" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Sending Success email in OPMHI_ENRLMNT_Driver.sh - Failed (${ENVNAME})"
		MSG="Sending Success email in OPMHI_ENRLMNT_Driver.sh  has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${OPMHI_EMAIL_SENDER}" "${OPMHI_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
fi


#############################################################
# Create Manifest file
#############################################################
echo "" >> ${LOGNAME}
echo "Create Manifest file for OPMHI Enrollment Extract.  " >> ${LOGNAME}

${RUNDIR}CreateManifestFile.sh ${S3BUCKET} ${CUR_DT} ${OPMHI_EMAIL_SUCCESS_RECIPIENT} 


#############################################################
# Check the status of script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script CreateManifestFile.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Create Manifest file in OPMHI_ENRLMNT_Driver.sh - Failed (${ENVNAME})"
	MSG="Create Manifest file in OPMHI_ENRLMNT_Driver.sh has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${OPMHI_EMAIL_SENDER}" "${OPMHI_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	

#################################################################################
# script clean-up and send success email
#################################################################################
echo "" >> ${LOGNAME}
echo "OPMHI_ENRLMNT_Driver.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS