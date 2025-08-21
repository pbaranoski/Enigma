#!/usr/bin/bash
############################################################################################################
# Script Name: ManifestFileReport.sh
#
# Description: This script can be run stand-alone and will report on the manifest files/extract files recently processed. 
#              This script can also be run a a called script to report on DOJ manifest files that are in hold status. 
#              The DOJ manifest files are used for reporting purposes only, and are not used to trigger a extract file 
#              delivery process.
#
#              The default ManifestSourceBucket = manifest_files.
#              For DOJ processing, and override manifest_file folder can be supplied (e.g., aws-hhs-cms-eadg-bia-ddom-extracts/xtr/manifest_files_hold ),
#              and also a manifest file HLQ to process a specific subset of the manifest files in the on-hold folder.
#
# Execute as ./ManifestFileReport.sh (processing stand-alone for already-processed manifest files).
# Execute as ./ManifestFileReport.sh $1 $2 (processing for DOJ manifest files) 
# 			$1 = Manifest Files Source bucket/folder   Ex: aws-hhs-cms-eadg-bia-ddom-extracts/xtr/manifest_files_hold 
# 			$2 = ${ManifestFileHLQ}
#
# Author     : Paul Baranoski	
# Created    : 12/01/2023
#
# Paul Baranoski 2023-12-01 Created script.
# Paul Baranoski 2023-12-22 Add manifest file migration from s3://manifest_files to manifest_files_archive.
# Paul Baranoski 2024-01-09 Add functionality to allow this script to be called by other scripts and: 
#                        	1) pass in an override S3 manifest folder to process
#                           2) pass in a manifest file HLQ to process a subset of files
# Paul Baranoski 2024-01-16 Add code to remove temporary directory before test to create it if it isn't there.
#                           Remove aws cp --recursive since it is copying from sub-directories which is not what is intended.
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/ManifestFileReport_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

MANIFEST_TMP_DIR=tmpManifestFiles/
MANIFEST_TMP_FILE=tmpManifestFiles.txt
MANIFEST_RPT_FILE=tmpManifestFileReport.txt

touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "ManifestFileReport.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}


##################################################################
# Extract can run stand-alone or as a called script.
##################################################################
if ! [[ $# -eq 0 || $# -eq 2  ]]
then
	echo "Incorrect # of parameters sent to script. NOF parameters: $#" >> ${LOGNAME}
	exit 12
fi

#############################################################
# Display parameters passed to script 
#############################################################
ManifestSourceBucketOverride=$1
ManifestFileHLQOverride=$2

echo " " >> ${LOGNAME}
echo "Parameters to script: " >> ${LOGNAME}
echo "NOF parameters for script: " $# >> ${LOGNAME}
echo "   ManifestSourceBucketOverride=${ManifestSourceBucketOverride} " >> ${LOGNAME}
echo "   ManifestFileHLQOverride=${ManifestFileHLQOverride} " >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh 

# isolate the S3 bucket only - bucket is defined in SET_XTR_ENV.sh
S3BUCKET=`echo ${bucket} | cut -d/ -f1 `
S3BUCKET=${S3BUCKET}/ 


# Set Manifest File Source bucket--> Use override if exists, otherwise use default manifest_files bucket/folder
ManifestSourceBucket=${ManifestSourceBucketOverride:-${MANIFEST_BUCKET}}
ManifestFileHLQ=${ManifestFileHLQOverride:-""}

echo "" >> ${LOGNAME}
echo "S3BUCKET=${S3BUCKET}" >> ${LOGNAME}
echo "MANIFEST_ARCHIVE_BUCKET=${MANIFEST_ARCHIVE_BUCKET}" >> ${LOGNAME}

echo "ManifestSourceBucket=${ManifestSourceBucket}" >> ${LOGNAME}
echo "ManifestFileHLQ=${ManifestFileHLQ}" >> ${LOGNAME}

# if directory exists, remove it.
if [ -d "${DATADIR}${MANIFEST_TMP_DIR}" ]; then
	rm -rf ${DATADIR}${MANIFEST_TMP_DIR}
fi

# create temporary directory
mkdir -m775 ${DATADIR}${MANIFEST_TMP_DIR}


#################################################################################
# Get list of manifest files and copy to data directory for processing/reporting.
#################################################################################
echo "" >> ${LOGNAME}
echo "Display available manifest files in S3: " >> ${LOGNAME}
aws s3 ls s3://${ManifestSourceBucket}${ManifestFileHLQ}  >> ${LOGNAME}

# Ex. Total Objects: 14 --> " 14" --> "14"
NOF_FILES=`aws s3 ls s3://${ManifestSourceBucket}${ManifestFileHLQ} --summarize | grep 'Total Objects' | cut -d: -f2 | sed 's/ //g' ` 2>> ${LOGNAME}

RET_STATUS=$?

if [[ $RET_STATUS = 0 ]]; then

	# We have files to process/report on
	if [[ ${NOF_FILES} -gt 0 ]]; then

		echo "" >> ${LOGNAME}
		echo "Copy manifest files to temp data directory"  >> ${LOGNAME}
		
		# Get list of available manifest files in S3(Extract just the filename) and put list into temp directory
		Filenames2Process=`aws s3 ls s3://${ManifestSourceBucket}${ManifestFileHLQ} | grep -v 'PRE' | awk '{print $4}' ` 2>> ${LOGNAME}  

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
			aws s3 cp s3://${ManifestSourceBucket}${Filename2Process} ${DATADIR}${MANIFEST_TMP_DIR}${Filename2Process}  >> ${LOGNAME} 2>&1

			RET_STATUS=$?

			if [[ $RET_STATUS != 0 ]]; then
				echo "" >> ${LOGNAME}
				echo "Shell script ManifestFileReport.sh failed." >> ${LOGNAME}
				
				# Send Failure email	
				SUBJECT="ManifestFileReport.sh - Failed (${ENVNAME})"
				MSG="Copying manifest file ${ManifestSourceBucket}${Filename2Process} from S3 to data directory has failed."
				${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

				exit 12
			fi

		done  

	else
		# No files to process/report on
			echo "" >> ${LOGNAME}
			echo "No manifest files to report on in ${ManifestSourceBucket}${ManifestFileHLQ} " >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="ManifestFileReport.sh (${ENVNAME})"
			MSG="No manifest files to report on in ${ManifestSourceBucket}${ManifestFileHLQ} ."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 0	
			
	fi			
else

	echo "" >> ${LOGNAME}
	echo "Shell script ManifestFileReport.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="ManifestFileReport.sh  - Failed (${ENVNAME})"
	MSG="Listing manifest files from ${ManifestSourceBucket}${ManifestFileHLQ} from S3 has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12

fi


#################################################################################
# Display list of manifest files we will process/report
#################################################################################
echo "" >> ${LOGNAME}
echo "Display list of manifest files to process for reporting on" >> ${LOGNAME}
ls ${DATADIR}${MANIFEST_TMP_DIR}*.json > ${DATADIR}${MANIFEST_TMP_DIR}${MANIFEST_TMP_FILE}  
cat ${DATADIR}${MANIFEST_TMP_DIR}${MANIFEST_TMP_FILE}   >> ${LOGNAME}


#################################################################################
# Write out HTML header.
#################################################################################
echo "<html><body><table cellspacing='1px' border='1' > " >> ${DATADIR}${MANIFEST_TMP_DIR}${MANIFEST_RPT_FILE}	
echo "<tr bgcolor='#00B0F0'><th>Data Request ID</th><th>Manifest Filename</th> <th>Extract filename</th><th>Extract file size</th></tr>" >> ${DATADIR}${MANIFEST_TMP_DIR}${MANIFEST_RPT_FILE}	

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
		
		echo "<tr><td>${DataRequestID}</td><td>${manifestFilename}</td><td>${ExtractFilename}</td><td>${extractFileSize}</td></tr> " >> ${DATADIR}${MANIFEST_TMP_DIR}${MANIFEST_RPT_FILE}	
	
	done
	
done  <  ${DATADIR}${MANIFEST_TMP_DIR}${MANIFEST_TMP_FILE}


#################################################################################
# Write out HTML trailer.
#################################################################################
echo "</table></body></html>" >> ${DATADIR}${MANIFEST_TMP_DIR}${MANIFEST_RPT_FILE}	


#############################################################
# Send success email
#############################################################
MANIFEST_INFO=`cat ${DATADIR}${MANIFEST_TMP_DIR}${MANIFEST_RPT_FILE} `

echo "" >> ${LOGNAME}
echo "Send success email" >> ${LOGNAME}
echo "MANIFEST_INFO=${MANIFEST_INFO} "   >> ${LOGNAME}

SUBJECT="Manifest Files Processed Report (${ENVNAME})"
MSG="Manifest Files Processed Report has completed.<br><br>The manifest files processed . . .<br><br>${MANIFEST_INFO}"
${PYTHON_COMMAND} ${RUNDIR}sendEmailHTML.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1


#############################################################
# Move processed manifest files to archive folder.
#############################################################
echo "" >> ${LOGNAME}
echo "Move processed manifest files from s3://manifest_files folder to s3://manifest_files_archive folder"  >> ${LOGNAME}

while read ManifestfileNPath
do

	echo "" >> ${LOGNAME}

	ManifestJSONFilename=`basename ${ManifestfileNPath}` 2>> ${LOGNAME} 
	echo "ManifestJSONFilename=${ManifestJSONFilename}"	>> ${LOGNAME}
	
	# move manifest file from manifest_files bucket/folder to manifest_files_archive bucket/folder
	#echo "s3://${ManifestSourceBucket}${ManifestJSONFilename} ${MANIFEST_ARCHIVE_BUCKET}${ManifestJSONFilename}"  >> ${LOGNAME}
	aws s3 mv s3://${ManifestSourceBucket}${ManifestJSONFilename} s3://${MANIFEST_ARCHIVE_BUCKET}${ManifestJSONFilename} >> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Shell script ManifestFileReport.sh failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="ManifestFileReport.sh - Failed (${ENVNAME})"
		MSG="Moving manifest file ${ManifestJSONFilename} from ${ManifestSourceBucket} to ${MANIFEST_ARCHIVE_BUCKET} failed."
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
rm -rf ${DATADIR}${MANIFEST_TMP_DIR} >> ${LOGNAME} 2>&1


#############################################################
# end script
#############################################################
echo "" >> ${LOGNAME}
echo "ManifestFileReport.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS