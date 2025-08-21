#!/usr/bin/bash
############################################################################################################
# Script Name: ManifestFileOnHoldReport.sh
# Description: TThis extract will report on the manifest files/extract files on hold.
#
# Author     : Paul Baranoski	
# Created    : 12/01/2023
#
# Paul Baranoski 2023-12-01 Created script.
# Paul Baranoski 2023-12-22 Add manifest file migration from s3://manifest_files to manifest_files_archive.
# Paul Baranoski 2024-01-17 Modify temporay directory name to be specific for OnHold processing so there 
#                           is no conflict with ManifestFileReport.sh processing.
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/ManifestFileOnHoldReport_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

MANIFEST_TMP_DIR=tmpManifestFilesOnHold/
MANIFEST_TMP_FILE=tmpManifestFilesOnHold.txt

touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "ManifestFileOnHoldReport.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}


#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh 

# isolate the S3 bucket only
S3BUCKET=`echo ${bucket} | cut -d/ -f1 `
S3BUCKET=${S3BUCKET}/ 


echo "" >> ${LOGNAME}
echo "S3BUCKET=${S3BUCKET}" >> ${LOGNAME}
echo "MANIFEST_HOLD_BUCKET=${MANIFEST_HOLD_BUCKET}" >> ${LOGNAME}


# if directory exists, remove it.
if [ -d "${DATADIR}${MANIFEST_TMP_DIR}" ]; then
	rm -rf ${DATADIR}${MANIFEST_TMP_DIR}
fi

# create temporary directory
mkdir -m775 ${DATADIR}${MANIFEST_TMP_DIR}


#################################################################################
# Get list of manifest files and copy to data directory for processing.
#################################################################################
echo "" >> ${LOGNAME}
echo "Find Manifest files on hold: " >> ${LOGNAME}
aws s3 ls s3://${MANIFEST_HOLD_BUCKET}  >> ${LOGNAME}
		
# Ex. Total Objects: 14 --> " 14" --> "14"
NOF_FILES=`aws s3 ls s3://${MANIFEST_HOLD_BUCKET} --summarize | grep 'Total Objects' | cut -d: -f2 | sed 's/ //g' ` 2>> ${LOGNAME}

RET_STATUS=$?

if [[ $RET_STATUS = 0 ]]; then

	# We have files to process
	if [[ ${NOF_FILES} -gt 0 ]]; then

		echo "" >> ${LOGNAME}
		echo "Copy manifest files to temp data directory"  >> ${LOGNAME}
		
		# Get list of manifest files in S3 hold status (folder) 
		Filenames2Process=`aws s3 ls s3://${MANIFEST_HOLD_BUCKET} | grep -v 'PRE' | awk '{print $4}' ` 2>> ${LOGNAME}  

		#################################################################################
		# Loop thru list.
		#################################################################################
		for Filename2Process in ${Filenames2Process}
		do

			echo "" >> ${LOGNAME}
			echo "*********************" >> ${LOGNAME}

			#############################################################################################
			# Copy manifest file from S3 to temp data directory
			#############################################################################################
			aws s3 cp s3://${MANIFEST_HOLD_BUCKET}${Filename2Process} ${DATADIR}${MANIFEST_TMP_DIR}${Filename2Process}  >> ${LOGNAME} 2>&1

			RET_STATUS=$?

			if [[ $RET_STATUS != 0 ]]; then
				echo "" >> ${LOGNAME}
				echo "Shell script ManifestFileOnHoldReport.sh failed." >> ${LOGNAME}
				
				# Send Failure email	
				SUBJECT="ManifestFileOnHoldReport.sh - Failed (${ENVNAME})"
				MSG="Copying manifest files from ${MANIFEST_HOLD_BUCKET} from S3 to data directory has failed."
				${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

				exit 12
			fi

		done  

	else
		# No files to process
		echo "" >> ${LOGNAME}
		echo "No manifest files on-hold in ${MANIFEST_HOLD_BUCKET} " >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="ManifestFileOnHoldReport.sh - No manifest files on-hold (${ENVNAME})"
		MSG="No manifest files are on-hold in ${MANIFEST_HOLD_BUCKET} ."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 0	
			
	fi	
else

	echo "" >> ${LOGNAME}
	echo "Shell script ManifestFileOnHoldReport.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="ManifestFileOnHoldReport.sh - Failed (${ENVNAME})"
	MSG="Listing manifest files from ${MANIFEST_HOLD_BUCKET} from S3 has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12

fi


#################################################################################
# Display list of manifest files on-hold we will process
#################################################################################
echo "" >> ${LOGNAME}
echo "Display list of manifest files to process" >> ${LOGNAME}
ls ${DATADIR}${MANIFEST_TMP_DIR}*.json > ${DATADIR}${MANIFEST_TMP_DIR}${MANIFEST_TMP_FILE}  2>> ${LOGNAME}
cat ${DATADIR}${MANIFEST_TMP_DIR}${MANIFEST_TMP_FILE}   >> ${LOGNAME}


#################################################################################
# Write out HTML header.
#################################################################################
echo "<html><body><table cellspacing='1px' border='1' > " >> ${DATADIR}${MANIFEST_TMP_DIR}tmpManifestFileReport.txt	
echo "<tr bgcolor='#00B0F0'><th>Data Request ID</th><th>Manifest Filename</th> <th>Extract filename</th><th>Extract file size</th></tr>" >> ${DATADIR}${MANIFEST_TMP_DIR}tmpManifestFileReport.txt	

#################################################################################
# Loop thru list.
#################################################################################
while read ManifestfileNPath
do

	echo "" >> ${LOGNAME}
	echo "*********************" >> ${LOGNAME}
	
	echo "ManifestfileNPath=${ManifestfileNPath}"  >> ${LOGNAME}
	
	#############################################################################################
	# Isolate dataRequestID
	# Ex. "dataRequestID": "RAND_CMS_CAR_20210815_20210818",  --> RAND_CMS_CAR_20210815_20210818
	#############################################################################################
	manifestFilename=`basename ${ManifestfileNPath} `  2>> ${LOGNAME}

	DataRequestID=`grep 'dataRequestID' ${ManifestfileNPath} | cut -d: -f2 | tr -d ' ",' `    2>> ${LOGNAME}
	echo "DataRequestID=${DataRequestID}" >> ${LOGNAME}
	
	S3ExtractFileFolder=`grep 'fileLocation' ${ManifestfileNPath} | head -n 1 | cut -d: -f2 | tr -d ' ",'  `   2>> ${LOGNAME}
	echo "S3ExtractFileFolder=${S3ExtractFileFolder}" >> ${LOGNAME}
	
	# loop thru extract filenames in case there are more than one.
	ExtractFilenames=`grep 'fileName' ${ManifestfileNPath} | cut -d: -f2 | tr -d ' ",' `   2>> ${LOGNAME}
	
	for ExtractFilename in ${ExtractFilenames}
	do
		echo "ExtractFilename=${ExtractFilename}"  >> ${LOGNAME}
		
		S3ExtractBucketFolder=${S3BUCKET}${S3ExtractFileFolder}
		extractFileSize=`aws s3 ls ${S3ExtractBucketFolder}${ExtractFilename} --human | awk '{print $3,$4}'`
		echo "extractFileSize=${extractFileSize} " >> ${LOGNAME}
		
		echo "<tr><td>${DataRequestID}</td><td>${manifestFilename}</td><td>${ExtractFilename}</td><td></td>${extractFileSize}</tr> " >> ${DATADIR}${MANIFEST_TMP_DIR}tmpManifestFileReport.txt	
	
	done
	
done  <  ${DATADIR}${MANIFEST_TMP_DIR}${MANIFEST_TMP_FILE}


#################################################################################
# Write out HTML trailer.
#################################################################################
echo "</table></body></html>" >> ${DATADIR}${MANIFEST_TMP_DIR}tmpManifestFileReport.txt	


#############################################################
# Send success email
#############################################################
MANIFEST_INFO=`cat ${DATADIR}${MANIFEST_TMP_DIR}tmpManifestFileReport.txt `

echo "" >> ${LOGNAME}
echo "Send success email" >> ${LOGNAME}
echo "MANIFEST_INFO=${MANIFEST_INFO} "   >> ${LOGNAME}

SUBJECT="Manifest Files still on hold (not processed) Report (${ENVNAME})"
MSG="Manifest Files still on hold (not processed) Report has completed.<br><br>The manifest files still on hold . . .<br><br>${MANIFEST_INFO}"
${PYTHON_COMMAND} ${RUNDIR}sendEmailHTML.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME} 
echo "Remove temporary manifest files and directory from data directory" >> ${LOGNAME} 

rm ${DATADIR}${MANIFEST_TMP_DIR}*.json >> ${LOGNAME} 2>&1
rm -rf ${DATADIR}${MANIFEST_TMP_DIR} >> ${LOGNAME} 2>&1


#############################################################
# end script
#############################################################
echo "" >> ${LOGNAME}
echo "ManifestFileOnHoldReport.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS