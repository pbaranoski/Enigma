#!/usr/bin/bash
############################################################################################################
# Script Name: HOS_EXTRACT.sh
# Description: This script executes the python that creates the two HOS extracts for H and M contract types.
#              General script flow:
#              (1)  Extract for H contractor using HFILE finder file
#                (1.1) Combine all H file segments
#                (1.2) Create Box manifest file for HFILE extract
#
#              (2)  Extract for M contractor using MFILE finder file
#                (2.1)  Combine all M file segments
#                (2.2)  Create Box manifest file for MFILE extract
#
# Author     : Joshua Turner	
# Created    : 03/27/2023
#
# Modified:             Date:        Description:
# --------------------  -----------  ----------------------------------------------------------------------
# Joshua Turner 	2023-03-28   New script.
# Joshua Turner         2023-10-26   Updated for BOX delivery. Added call to create manifest file for each
#                                    extract file.
# Paul Baranoski        2023-04-10   Add blank line after call to getExtractFilenamesAndCounts for 
#                                    Dashboard_MS.sh processsing.
# Nat.Tinovsky		2025-02-06   Add ENVNAME to SUBJECT were was missing.
############################################################################################################
set +x

#####################################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#####################################################################
source /app/IDRC/XTR/CMS/scripts/run/SET_XTR_ENV.sh
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

source ${RUNDIR}FilenameCounts.bash
#####################################################################
# Establish log file  
#####################################################################
export TMSTMP=`date +%Y%m%d.%H%M%S`
LOGNAME=/app/IDRC/XTR/CMS/logs/HOS_EXTRACT_${TMSTMP}.log
touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME}

echo "#############################################################" >> ${LOGNAME}
echo "HOS_EXTRACT.sh started at: ${TMSTMP}" >> ${LOGNAME}
echo "#############################################################" >> ${LOGNAME}

#####################################################################
# Call HOS_EXTRACT.py with the variables for 'H' type 
#####################################################################
export YEAR=`date +%Y`
MONTH=`date +%m`
export SDATE="${YEAR}-${MONTH}-01"
export HOS_FF_TABLE=HOSHFF
export FILETYPE=HFILE

echo ""
echo "Calling HOS_EXTRACT.py for H type using the following variables:" >> ${LOGNAME}
echo "DATE: ${SDATE}" >> ${LOGNAME}
echo "FF Table: ${HOS_FF_TABLE}" >> ${LOGNAME}
echo "FILETYPE: ${FILETYPE}" >> ${LOGNAME}

${PYTHON_COMMAND} ${RUNDIR}HOS_EXTRACT.py >> ${LOGNAME} 2>&1

RET_STATUS=$?
if [[ ${RET_STATUS} != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "HOS_EXTRACT.sh failed while extracting HFILE" >> ${LOGNAME}
	#SEND FAILURE EMAIL IF EITHER COPY FAILED
	SUBJECT="HOS_EXTRACT.sh - Failed in ${ENVNAME}"
	MSG="HOS_EXTRACT.sh/py failed while extracting HFILE"
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${HOS_EMAIL_SENDER}" "${HOS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
	exit 12
fi

echo "HOS_EXTRACT complete for HFILE." >> ${LOGNAME}

#####################################################################
# Combine 'H' File Type
#####################################################################
echo "" >> ${LOGNAME}
FILE_PREFIX=HOS_XTR_Y${YEAR}_${FILETYPE}_${TMSTMP}.csv.gz
echo "Combing files with prefix type: ${FILE_PREFIX}" >> ${LOGNAME}

${RUNDIR}CombineS3Files.sh ${HOS_BUCKET} ${FILE_PREFIX} 

RET_STATUS=$?
if [[ ${RET_STATUS} != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "HOS_EXTRACT.sh failed while combining file: ${FILE_PREFIX}" >> ${LOGNAME}
	#SEND FAILURE EMAIL IF EITHER COPY FAILED
	SUBJECT="HOS_EXTRACT.sh - Failed in ${ENVNAME}"
	MSG="HOS_EXTRACT.sh failed while combining file: ${FILE_PREFIX}"
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${HOS_EMAIL_SENDER}" "${HOS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
	exit 12
fi

#####################################################################
# Create manifest file for 'H' file and check return status
#####################################################################
echo "" >> ${LOGNAME}
echo "Creating Manifest file for: ${FILE_PREFIX}" >> ${LOGNAME}

${RUNDIR}CreateManifestFile.sh ${HOS_BUCKET} ${TMSTMP} ${HOS_EMAIL_BOX_RECIPIENT}

RET_STATUS=$?
if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script CreateManifestFile.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Create Manifest file in HOS_EXTRACT.sh - Failed ${ENVNAME}"
	MSG="Create Manifest file for HOS_EXTRACT.sh has failed in the HFILE step."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${HOS_EMAIL_SENDER}" "${HOS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi

#####################################################################
# Call HOS_EXTRACT.py with the variables for 'M' type 
#####################################################################
export TMSTMP=`date +%Y%m%d.%H%M%S`
export HOS_FF_TABLE=HOSMFF
export FILETYPE=MFILE
echo ""
echo "Calling HOS_EXTRACT.py for M type using the following variables:" >> ${LOGNAME}
echo "DATE: ${SDATE}" >> ${LOGNAME}
echo "FF Table: ${HOS_FF_TABLE}" >> ${LOGNAME}
echo "FILETYPE: ${FILETYPE}" >> ${LOGNAME}

${PYTHON_COMMAND} ${RUNDIR}HOS_EXTRACT.py >> ${LOGNAME} 2>&1

RET_STATUS=$?
if [[ ${RET_STATUS} != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "HOS_EXTRACT.sh failed while extracting MFILE" >> ${LOGNAME}
	#SEND FAILURE EMAIL IF EITHER COPY FAILED
	SUBJECT="HOS_EXTRACT.sh - Failed in ${ENVNAME}"
	MSG="HOS_EXTRACT.sh/py failed while extracting MFILE"
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${HOS_EMAIL_SENDER}" "${HOS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
	exit 12
fi

echo "HOS_EXTRACT complete for MFILE." >> ${LOGNAME}

#####################################################################
# Combine 'M' Files Type
#####################################################################
echo "" >> ${LOGNAME}
FILE_PREFIX=HOS_XTR_Y${YEAR}_${FILETYPE}_${TMSTMP}.csv.gz
echo "Combing files with prefix type: ${FILE_PREFIX}" >> ${LOGNAME}

${RUNDIR}CombineS3Files.sh ${HOS_BUCKET} ${FILE_PREFIX} 

RET_STATUS=$?
if [[ ${RET_STATUS} != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "HOS_EXTRACT.sh failed while combining file: ${FILE_PREFIX}" >> ${LOGNAME}
	#SEND FAILURE EMAIL IF EITHER COPY FAILED
	SUBJECT="HOS_EXTRACT.sh - Failed in ${ENVNAME}"
	MSG="HOS_EXTRACT.sh failed while combining file: ${FILE_PREFIX}"
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${HOS_EMAIL_SENDER}" "${HOS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
	exit 12
fi

#####################################################################
# Create manifest file for 'M' file and check return status
#####################################################################
echo "" >> ${LOGNAME}
echo "Creating Manifest file for: ${FILE_PREFIX}" >> ${LOGNAME}

${RUNDIR}CreateManifestFile.sh ${HOS_BUCKET} ${TMSTMP} ${HOS_EMAIL_BOX_RECIPIENT}

RET_STATUS=$?
if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script CreateManifestFile.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Create Manifest file in HOS_EXTRACT.sh - Failed ${ENVNAME}"
	MSG="Create Manifest file for HOS_EXTRACT.sh has failed in the MFILE step."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${HOS_EMAIL_SENDER}" "${HOS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi

#####################################################################
# Send Success email with the filenames and counts
#####################################################################
echo "" >> ${LOGNAME}
echo "Getting S3 filename list and record counts." >> ${LOGNAME}

getExtractFilenamesAndCounts ${LOGNAME} >> ${LOGNAME} 2>&1
FILE_LIST="${filenamesAndCounts}"

#####################################################################
# Send success email
#####################################################################
echo "" >> ${LOGNAME}

SUBJECT="Health Outcome Survey (HOS) for ${SDATE} ${ENVNAME}" 
MSG="The extract for Health Outcome Survey has been completed.\n\nFILE NAME                                            NO. OF RECORDS\n======================================================\n${FILE_LIST}"
${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${HOS_EMAIL_SENDER}" "${HOS_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Error in calling sendEmail.py" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Sending Success email in HOS_EXTRACT.sh  - Failed ${ENVNAME}"
	MSG="Sending Success email in HOS_EXTRACT.sh  has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${HOS_EMAIL_SENDER}" "${HOS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	

echo "#############################################################" >> ${LOGNAME}
echo "HOS_EXTRACT.sh completed successfully." >> ${LOGNAME}
echo "Ended at `date` " >> ${LOGNAME}
echo "#############################################################" >> ${LOGNAME}
echo "" >> ${LOGNAME}

exit $RET_STATUS
