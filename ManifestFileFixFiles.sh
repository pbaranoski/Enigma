#!/usr/bin/bash
############################################################################################################
# Script Name: ManifestFileFixFiles.sh
# Description: This script will extract dashboard info from extract scripts.
#
# Author     : Paul Baranoski	
# Created    : 12/01/2023
#
# Paul Baranoski 2023-12-01 Created script.
# Paul Baranoski 2023-12-22 Add manifest file migration from s3://manifest_files to manifest_files_archive.
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/ManifestFileFixFiles_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

MANIFEST_TMP_DIR=tmpManifestFiles/
MANIFEST_TMP_FILE=tmpManifestFiles.txt

touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "ManifestFileFixFiles.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}


#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh 

# isolate the S3 bucket only
S3BUCKET=`echo ${bucket} | cut -d/ -f1 `
S3BUCKET=${S3BUCKET}/ 

##\/##
S3BUCKET=aws-hhs-cms-eadg-bia-ddom-extracts/
MANIFEST_ARCHIVE_BUCKET=aws-hhs-cms-eadg-bia-ddom-extracts/xtr/manifest_files_hold/
##/\##

echo "" >> ${LOGNAME}
echo "S3BUCKET=${S3BUCKET}" >> ${LOGNAME}
echo "MANIFEST_ARCHIVE_BUCKET=${MANIFEST_ARCHIVE_BUCKET}" >> ${LOGNAME}


if ! [ -d "${DATADIR}${MANIFEST_TMP_DIR}" ]; then
	mkdir -m775 ${DATADIR}tmpManifestFiles
fi

#################################################################################
# Get list of manifest files and copy to data directory for processing.
#################################################################################
echo "" >> ${LOGNAME}
echo "Find Manifest files: " >> ${LOGNAME}

aws s3 ls s3://${MANIFEST_ARCHIVE_BUCKET}  >> ${LOGNAME}  2>&1

RET_STATUS=$?

if [[ $RET_STATUS = 0 ]]; then

	echo "" >> ${LOGNAME}
	echo "Copy manifest files to temp data directory"  >> ${LOGNAME}

	# if manifest files exist --> copy to data directory
	aws s3 cp --recursive s3://${MANIFEST_ARCHIVE_BUCKET} ${DATADIR}${MANIFEST_TMP_DIR}  >> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Shell script ManifestFileFixFiles.sh failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Copying manifest files from ${MANIFEST_ARCHIVE_BUCKET} from S3 to data directory - Failed (${ENVNAME})"
		MSG="Copying manifest files from ${MANIFEST_ARCHIVE_BUCKET} from S3 to data directory has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi
else

	echo "" >> ${LOGNAME}
	echo "Shell script ManifestFileFixFiles.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Listing manifest files from ${MANIFEST_ARCHIVE_BUCKET} from S3  - Failed (${ENVNAME})"
	MSG="Listing manifest files from ${MANIFEST_ARCHIVE_BUCKET} from S3 has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12

fi


#################################################################################
# Display list of manifest files we will process
#################################################################################
echo "" >> ${LOGNAME}
echo "Display list of manifest files to process" >> ${LOGNAME}
ls ${DATADIR}${MANIFEST_TMP_DIR}*.json > ${DATADIR}${MANIFEST_TMP_DIR}${MANIFEST_TMP_FILE}  2>> ${LOGNAME}
cat ${DATADIR}${MANIFEST_TMP_DIR}${MANIFEST_TMP_FILE}   >> ${LOGNAME}


#################################################################################
# Loop thru list.
#################################################################################
while read ManifestfileNPath
do

	echo "" >> ${LOGNAME}
	echo "*********************" >> ${LOGNAME}
	
	echo "ManifestfileNPath=${ManifestfileNPath}"  >> ${LOGNAME}
	
	#############################################################################################
	# Is this manifest file for ANTI-TRUST?
	#############################################################################################
	DOJ_filename=`grep "DOJ_ANTI_TRUST" ${ManifestfileNPath} ` 
	echo "DOJ_filename=${DOJ_filename}" >> ${LOGNAME}
	
	# not ANTI-TRUST manifest file
	if [ -z "${DOJ_filename}" ]; then
		
		continue
	fi
	
	#############################################################################################
	# Modify JIRA-ticket #
	#############################################################################################
	sed -i 's/IDRBI-73703/IDRBI-71678/g' ${ManifestfileNPath}

	#############################################################################################
	# Upload manifest file to S3 hold
	#############################################################################################	
	manifestFilename=`basename ${ManifestfileNPath} `  2>> ${LOGNAME}

	aws s3 cp ${ManifestfileNPath} s3://${MANIFEST_ARCHIVE_BUCKET}${manifestFilename}  >> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Shell script ManifestFileFixFiles.sh failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Copying manifest files from ${MANIFEST_ARCHIVE_BUCKET} from S3 to data directory - Failed (${ENVNAME})"
		MSG="Copying manifest files from ${MANIFEST_ARCHIVE_BUCKET} from S3 to data directory has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi
	
	
done  <  ${DATADIR}${MANIFEST_TMP_DIR}${MANIFEST_TMP_FILE}


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME} 
echo "Remove temporary manifest files and directory from data directory" >> ${LOGNAME} 

rm ${DATADIR}${MANIFEST_TMP_DIR}*.json >> ${LOGNAME} 2>&1
rm -r ${DATADIR}${MANIFEST_TMP_DIR} >> ${LOGNAME} 2>&1


#############################################################
# end script
#############################################################
echo "" >> ${LOGNAME}
echo "ManifestFileFixFiles.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS