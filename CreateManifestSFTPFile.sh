#!/usr/bin/bash
############################################################################################################
# Name:  CreateManifestSFTPFile.sh
#
# Desc: Create Manifest file required for transfers of Extract files to Outside Consumers using BOX 
#
# Execute as ./CreateManifestSFTPFile.sh $1 $2 $3 $4 $5
#
# $1 = S3 bucket/folder_name  Ex1: bucket=aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod/xtr/DEV/MNUPAnnual/  
#                             Ex2: bucket=aws-hhs-cms-eadg-bia-ddom-extracts/xtr/MNUPAnnual/
# $2 = S3 filename timestamp  Ex:  R240325.T131201   
# $3 = SFTP recipient email addresses (comma delimited)  
# $4 = S3 Destination manifest_files folder (SFTP - manifest_files_ssa_sftp/) or manifest_miles/SSA
# $5 = Manifest Filename HLQ (Beginning Nodes) --> Ex. MNUP_ANNUAL_
# $6 = SFTP Destination folder --> Ex. SSAMNUP
# 
# $1 = Where extract file lives in S3
# $2 = How to indentify all extract files to be included in the manifest file (e.g. all files with same "timestamp")
# $3 = The SFTP account email address
# $4 = The S3 folder where to place the manifest file
# $5 = The HLQ for the name of the manifest file itself
# $6 = The dataRequestID value in the manifest file. The folder name at the destination.
# 
#
# 2024-030-25 Paul Baranoski   Created script.	
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
#TMSTMP = If TMSTMP value set by caller via export --> use that value. 
#         Else use the timestamp created in this script
TMSTMP=${TMSTMP:=`date +%Y%m%d.%H%M%S`}

LOGNAME=/app/IDRC/XTR/CMS/logs/CreateManifestSFTPFile_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "CreateManifestSFTPFile.sh started at `date` " >> ${LOGNAME}
echo " " >> ${LOGNAME}
echo "TMSTMP=${TMSTMP} " >> ${LOGNAME}


#############################################################
# Display parameters passed to script 
# Ex. CreateManifestSFTPFile.sh s3://aws-hhs-cms-eadg-bia-ddom-extracts/xtr/DOJ/ 20231211.125522 pbaranoski-con@index.com 
#############################################################
S3BucketAndFldr=$1
S3FilenameTmstmp=$2
RecipientEmails=$3
S3MANIFEST_FOLDER_TO_USE=$4
MANIFEST_FILE_HLQ=$5
SFTP_DEST_FLDR=$6

echo " " >> ${LOGNAME}
echo "Parameters to script: " >> ${LOGNAME}
echo "   S3BucketAndFldr=${S3BucketAndFldr} " >> ${LOGNAME}
echo "   S3FilenameTmstmp=${S3FilenameTmstmp} " >> ${LOGNAME}
echo "   RecipientEmails=${RecipientEmails} " >> ${LOGNAME}
echo "   S3MANIFEST_FOLDER_TO_USE=${S3MANIFEST_FOLDER_TO_USE} " >> ${LOGNAME}
echo "   MANIFEST_FILE_HLQ=${MANIFEST_FILE_HLQ} " >> ${LOGNAME}
echo "   SFTP_DEST_FLDR=${SFTP_DEST_FLDR} " >> ${LOGNAME}

#############################################################
# Verify that required NOF parameters have been sent.
#############################################################
if ! [[ $# -eq 6 ]]
then
	echo "Incorrect # of parameters sent to script. NOF parameters: $#" >> ${LOGNAME}
	exit 12
fi

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh


#############################################################
# Extract the S3Bucket from S3BucketAndFldr
# Ex. aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod/xtr/DEV/Blbtn/ --> aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod
#############################################################
S3Bucket=`echo ${S3BucketAndFldr} | cut -d/ -f1`  2>> ${LOGNAME}

echo "" >> ${LOGNAME}
echo "S3Bucket=${S3Bucket}" >> ${LOGNAME}


#############################################################
# Extract the S3 FolderName from the S3BucketAndFldr
# Ex. aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod/xtr/DEV/Blbtn/ -->  xtr/DEV/Blbtn/
# Ex. aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod/xtr/Blbtn/ -->  xtr/Blbtn/
#############################################################
S3Folder=`echo ${S3BucketAndFldr} | cut -d/ -f2-` 2>> ${LOGNAME}
echo "S3Folder=${S3Folder}" >> ${LOGNAME}


#############################################################
# Get list of S3 files to include in manifest.
#############################################################
echo "" >> ${LOGNAME}
echo "Get list of S3 files to include in manifest file " >> ${LOGNAME}

aws s3 ls s3://${S3BucketAndFldr} | grep ${S3FilenameTmstmp} > ${DATADIR}manTemp_${TMSTMP}.txt  2>> ${LOGNAME}

RET_STATUS=$?

if [ $RET_STATUS != 0 ]; then
	echo "" >> ${LOGNAME}
	echo "Error in getting files from S3 Bucket ${S3BucketAndFldr} OR no S3 Files found to include in manifest file." >> ${LOGNAME}

	exit 12
fi


#############################################################
# if no S3 files found --> return to caller
#############################################################
#nofFiles=`wc -l ${DATADIR}manTemp_${TMSTMP}.txt | cut -d' ' -f1`   2>> ${LOGNAME}
nofFiles=`wc -l ${DATADIR}manTemp_${TMSTMP}.txt | awk '{print $1}' `  2>> ${LOGNAME}

if [ $nofFiles -eq 0 ]; then
	echo "" >> ${LOGNAME}
	echo "No S3 Files found to include in manifest file" >> ${LOGNAME}

	exit 12
fi


#############################################################
# Get S3 Files; Convert list to comma-delimited string
#############################################################
S3Files=`cat ${DATADIR}manTemp_${TMSTMP}.txt | awk '{print $4}' | tr '\n' ','` 2>> ${LOGNAME}
echo "S3Files=${S3Files}" >> ${LOGNAME}


#############################################################
# Get total S3 Files size
#############################################################
S3FileSizes=`cat ${DATADIR}manTemp_${TMSTMP}.txt | awk '{print $3}' ` 2>> ${LOGNAME}
echo "S3FileSizes=${S3FileSizes}" >> ${LOGNAME}

totS3FileSize=0

for S3FileSize in ${S3FileSizes}
do
	totS3FileSize=`expr ${totS3FileSize} + ${S3FileSize}`

done 

echo "" >> ${LOGNAME}
echo "totS3FileSize=${totS3FileSize}" >> ${LOGNAME}


#############################################################
# Get manifest filename
#############################################################
# Ex. NYSPAP_Manifest_20221006.093854.json
ManifestFilename=${MANIFEST_FILE_HLQ}_Manifest_${S3FilenameTmstmp}.json
echo "ManifestFilename=${ManifestFilename}" >> ${LOGNAME}

 
#############################################################
# Execute Python code to create manifest file.
#############################################################
echo "" >> ${LOGNAME}
echo "Start execution of CreateManifestSFTPFile.py program"  >> ${LOGNAME}

echo "S3Bucket=${S3Bucket}" >> ${LOGNAME}
echo "S3Folder=${S3Folder}" >> ${LOGNAME}
echo "S3Files=${S3Files}" >> ${LOGNAME}
echo "RecipientEmails=${RecipientEmails}" >> ${LOGNAME}
echo "S3MANIFEST_FOLDER_TO_USE=${S3MANIFEST_FOLDER_TO_USE}" >> ${LOGNAME}
echo "SFTP_DEST_FLDR=${SFTP_DEST_FLDR}" >> ${LOGNAME}


ManifestPathAndFilename=${DATADIR}${ManifestFilename}
echo "ManifestPathAndFilename=${ManifestPathAndFilename}" >> ${LOGNAME}


#############################################################
# Check the status of python script  
#############################################################
echo "" >> ${LOGNAME}
${PYTHON_COMMAND} ${RUNDIR}CreateManifestSFTPFile.py --bucket ${S3Bucket} --folder ${S3Folder} --files ${S3Files} --REmails ${RecipientEmails} --outfile ${ManifestPathAndFilename} --SFTPDestFldr ${SFTP_DEST_FLDR}  >> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script CreateManifestSFTPFile.py failed" >> ${LOGNAME}

        exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script CreateManifestSFTPFile.py completed successfully. " >> ${LOGNAME}


#################################################################################
# Move manifest file to S3 manifest folder. 
#################################################################################
echo "" >> ${LOGNAME}

## Copy manifest file to s3 manifest folder
aws s3 cp ${DATADIR}${ManifestFilename} s3://${S3MANIFEST_FOLDER_TO_USE}${ManifestFilename}  1>> ${LOGNAME} 2>&1


RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Copying Manifest file to S3 manifest folder to s3://${S3MANIFEST_FOLDER_TO_USE}${ManifestFilename} failed." >> ${LOGNAME}

	exit 12
fi	
	
#################################################################################
# Remove manifest file from Linux data directory 
#################################################################################
echo "" >> ${LOGNAME}
echo "Remove Manifest file from Linux data directory." >> ${LOGNAME}

rm ${DATADIR}${ManifestFilename} 2>> ${LOGNAME} 	


#############################################################
# remove temporary file from data directory
#############################################################
echo " " >> ${LOGNAME}
echo "Delete temp file manTemp_${TMSTMP}.txt from linux data directory " >> ${LOGNAME}

rm ${DATADIR}manTemp_${TMSTMP}.txt  2>> ${LOGNAME}


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "CreateManifestSFTPFile.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS