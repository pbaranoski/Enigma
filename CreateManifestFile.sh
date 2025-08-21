#!/usr/bin/bash
############################################################################################################
# Name:  CreateManifestFile.sh
#
# Desc: Create Manifest file required for transfers of Extract files to Outside Consumers using BOX 
#
# Execute as ./CreateManifestFile.sh $1 $2 $3 $4 $5
#
# $1 = S3 bucket/folder_name  Ex1: bucket=aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod/xtr/DEV/Blbtn/  
#                             Ex2: bucket=aws-hhs-cms-eadg-bia-ddom-extracts/xtr/Blbtn/
# $2 = S3 filename timestamp  Ex:  20220922.084321   
# $3 = Extract recipient email addresses (comma delimited)  
# $4 = (optional) S3 destination manifest_files folder (DEFAULT, SSA_BOX, VA_BOX) 
# $5 = (optional) $4 becomes required if using $5. For files in SFTP_files folder --> Original S3 Extract folder (e.g. MNUPAnnual)
#                 for lookup against JIRA_Extract_Mappings.txt to find JIRA ticket # for manifest file 
# 
# $1 = Where extract file lives in S3
# $2 = How to indentify all extract files to be included in the manifest file (e.g. all files with same "timestamp")
# $3 = The box account email address
# $4 = The S3 Destination manifest_files folder (DEFAULT, SSA_BOX, VA_BOX)
# $5 = Key to use against JIRA_Extract_Mappings.txt to find JIRA ticket # for manifest file
# 
#
# 10/12/2022 Paul Baranoski   Created script.	
# 10/27/2022 Paul Baranoski   Added code to use "ExtractType" to get the JIRA ticket # from JIRA_Extract_Mappings file. 
# 03/08/2023 Paul Baranoski   Modify S3 location for manifest configuration file. 
# 04/07/2023 Paul Baranoski   Included SET_XTR_ENV.sh to get CONFIG_BUCKET value.  
#                             Added sed command to remove \r when getting "jiraEntry"
#                             Corrected log messages when getting S3 file list with grep. When no files are found using grep,   
#                             RC is set to 1 and not 0 (as I expected). Nothing wrong with command, but message mis-leading.
#                             So, no issue in getting list of files from S3, but grep filter resulting in no files found, set RC = 1.
# 05/08/2023 Paul Baranoski   Modified syntax of TMSTMP=${TMSTMP=`date +%Y%m%d.%H%M%S`} to TMSTMP=${TMSTMP:=`date +%Y%m%d.%H%M%S`}
# 08/22/2023 Paul Baranoski   Add timestamp to temp file so each caller has their own temp file copy.
# 10/10/2023 Paul Baranoski   Add capability to move manifest file to a "staging folder" if caller requests it. 
#                             Add code to handle 4th optional parameter to indicate if "staging folder" is to be used. 
# 10/12/2023 Paul Baranoski   Add code to read a Manifest configuration file to determine if the manifest file should be
#                             written to the S3 Staging/Hold folder instead of using a passed parameter. Remove 
#                             optional parameter code added on 10/10/2023.   
# 11/27/2023 Paul Baranoski   Add support for multiple S3 manifest_files folders (DEFAULT, SSA_BOX, VA_BOX)
# 12/26/2023 Paul Baranoski   Create alternate logic for getting JIRA ticket numbers for DOJ requests. Match
#                             each DOJ JIRA mapping entry to Extract filename for a match.
# 12/28/2023 Paul Baranoski   Modify Manifest filename to use "Matching Key" for DOJ request so we can differentiate DOJ_ANTI_TRUST from other DOJ requests.
# 01/17/2023 Paul Baranoski   Add additional comments, and override functionality.
# 05/22/2024 Paul Baranoski   Script seemed to hang when there wasn't a DOJ mapping for an extract file.
#                             Added double quotes around MatchingKey (in case it was blank) when code was extracting jiraEntry. When the value was blank,
#                             the code appeared to hang. 
#                             Also, added code after call to findDOJJiraTicketMatch to check to see if MatchingKey was blank, and error at that point and exit script. 
# 05/30/2024 Paul Baranoski   Dupliate DOJ logic for FOIA requests.  
# 10/02/2024 Paul Baranoski   Add "=" to command --> grep "${MatchingKey}=" ${DATADIR}${JIRA_MAPPING_FILE} | sed 's/\r//'. This was because for STS_MED_INS, 
#                             both STS_MED_INS and STS_MED_INS_MN were returned where only one entry is supposed to be returned. This caused script errors.
# 12/31/2024 Paul Baranoski   Add carat in grep statement to get jiraEntry. Even though I should only get one match, the code appears to be getting two matches.
# 01/06/2025 Paul Baranoski   Remove obsolete code: 1) FileSize logic, and 2) modify code to call new CreateManifestFilev2.py module.
# 01/29/2025 Paul Baranoski   Add echos for passed parm values when incorrect # of parms passed. 
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
#TMSTMP = If TMSTMP value set by caller via export --> use that value. 
#         Else use the timestamp created in this script
TMSTMP=${TMSTMP:=`date +%Y%m%d.%H%M%S`}

LOGNAME=/app/IDRC/XTR/CMS/logs/CreateManifestFile_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

S3MANIFEST_FOLDER_TO_USE=""
JIRA_MAPPING_FILE=JIRA_Extract_Mappings.txt
MANIFEST_CONFIG_FILE=MANIFEST_FILE_PROCESS_CONFIG.txt

HoldManifestFile=N

touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "CreateManifestFile.sh started at `date` " >> ${LOGNAME}
echo " " >> ${LOGNAME}
echo "TMSTMP=${TMSTMP} " >> ${LOGNAME}

#############################################################
# Verify that required NOF parameters have been sent.
#############################################################
if ! [[ $# -eq 3 || $# -eq 4  ||  $# -eq 5 ]]
then
	echo "Incorrect # of parameters sent to script. NOF parameters: $#" >> ${LOGNAME}
	echo "parm1=$1" >> ${LOGNAME}
	echo "parm2=$2" >> ${LOGNAME}
	echo "parm3=$3" >> ${LOGNAME}	
	exit 12
fi

#############################################################
# Display parameters passed to script 
# Ex. CreateManifestFile.sh s3://aws-hhs-cms-eadg-bia-ddom-extracts/xtr/DOJ/ 20231211.125522 pbaranoski-con@index.com 
#############################################################
S3BucketAndFldr=$1
S3FilenameTmstmp=$2
RecipientEmails=$3
MANIFEST_BUCKET_OVERRIDE=$4
ExtractTypeOverride=$5

echo " " >> ${LOGNAME}
echo "Parameters to script: " >> ${LOGNAME}
echo "   S3BucketAndFldr=${S3BucketAndFldr} " >> ${LOGNAME}
echo "   S3FilenameTmstmp=${S3FilenameTmstmp} " >> ${LOGNAME}
echo "   RecipientEmails=${RecipientEmails} " >> ${LOGNAME}
echo "   MANIFEST_BUCKET_OVERRIDE=${MANIFEST_BUCKET_OVERRIDE} " >> ${LOGNAME}
echo "   ExtractTypeOverride=${ExtractTypeOverride} " >> ${LOGNAME}


#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh

echo " " >> ${LOGNAME}

if [ ! "${MANIFEST_BUCKET_OVERRIDE}" = "" ]; then
	echo "Using override manifest bucket value" >> ${LOGNAME}
	S3MANIFEST_FOLDER_TO_USE=${MANIFEST_BUCKET_OVERRIDE}
else
	echo "Using default manifest bucket value" >> ${LOGNAME}
	S3MANIFEST_FOLDER_TO_USE=${MANIFEST_BUCKET}
fi

echo "MANIFEST_BUCKET=${MANIFEST_BUCKET} " >> ${LOGNAME}
echo "MANIFEST_HOLD_BUCKET=${MANIFEST_HOLD_BUCKET} " >> ${LOGNAME}
echo "S3MANIFEST_FOLDER_TO_USE=${S3MANIFEST_FOLDER_TO_USE} " >> ${LOGNAME}

echo "MANIFEST_CONFIG_FILE=${MANIFEST_CONFIG_FILE}" >> ${LOGNAME}

MANIFEST_EMAIL_SENDER=${CMS_EMAIL_SENDER}
MANIFEST_EMAIL_SUCCESS_RECIPIENT=${ENIGMA_EMAIL_SUCCESS_RECIPIENT}
MANIFEST_EMAIL_FAILURE_RECIPIENT=${ENIGMA_EMAIL_FAILURE_RECIPIENT}


#############################################################
# Extract the S3Bucket from S3BucketAndFldr
# Ex. aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod/xtr/DEV/Blbtn/ --> aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod
#############################################################
S3Bucket=`echo ${S3BucketAndFldr} | cut -d/ -f1`  2>> ${LOGNAME}

echo "" >> ${LOGNAME}
echo "S3Bucket=${S3Bucket}" >> ${LOGNAME}
echo "CONFIG_BUCKET=${CONFIG_BUCKET}" >> ${LOGNAME}


#############################################################
# Extract the S3 FolderName from the S3BucketAndFldr
# Ex. aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod/xtr/DEV/Blbtn/ -->  xtr/DEV/Blbtn/
# Ex. aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod/xtr/Blbtn/ -->  xtr/Blbtn/
#############################################################
S3Folder=`echo ${S3BucketAndFldr} | cut -d/ -f2-` 2>> ${LOGNAME}
echo "S3Folder=${S3Folder}" >> ${LOGNAME}

#############################################################
# Get ExtractType
#############################################################
if [ "${ExtractTypeOverride}" = "" ];then
	ExtractType=`basename ${S3BucketAndFldr} ` 2>> ${LOGNAME}
else
	echo "Using ExtractTypeOverride parameter: ${ExtractTypeOverride}" >> ${LOGNAME}
	ExtractType=${ExtractTypeOverride}
fi

echo "ExtractType=${ExtractType}" >> ${LOGNAME}


#############################################################
# functions
#############################################################
function findDOJFOIAJiraTicketMatch {

	echo "In function findDOJFOIAJiraTicketMatch " >> ${LOGNAME}
	
	p_ExtractType=$1
	S3ExtractFilename=$2
	MatchingKey=""
	
	echo "p_ExtractType=${p_ExtractType}" >> ${LOGNAME}
	echo "S3ExtractFilename=${S3ExtractFilename}" >> ${LOGNAME}
	
	# Get all the DOJ JIRA keys Ex. (DOJ_ANTI_TRUST1, DOJ_USA_V_KINDRED, DOJ_ABBOTT
	DOJ_FOIA_KEYS=`cat ${DATADIR}${JIRA_MAPPING_FILE} | sed 's/\r//' | cut -d= -f1  | grep "${p_ExtractType}" `
	echo "DOJ_FOIA_KEYS=${DOJ_FOIA_KEYS}"  >> ${LOGNAME}
	echo ""  >> ${LOGNAME}
	
	# And match filename to DOJ/FOIA key 
	for DOJ_FOIA_KEY in ${DOJ_FOIA_KEYS}
	do
		echo "DOJ_FOIA_KEY=${DOJ_FOIA_KEY}"  >> ${LOGNAME}
		
		keyMatch=`echo "${S3ExtractFilename}" | grep "${DOJ_FOIA_KEY}" `
		
		# if key matches extract filename	
		if [ -n "$keyMatch" ];then
			MatchingKey=${DOJ_FOIA_KEY}
			break
		fi
	done
	
	echo "MatchingKey=${MatchingKey}" >> ${LOGNAME}
	
	echo "Leaving function findDOJJiraTicketMatch " >> ${LOGNAME}
}


#############################################################
# Get list of S3 files to include in manifest.
#############################################################
echo "" >> ${LOGNAME}
echo "Get list of S3 files to include in manifest file(s) " >> ${LOGNAME}

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
nofFiles=`wc -l ${DATADIR}manTemp_${TMSTMP}.txt | awk '{print $1}' `  2>> ${LOGNAME}

if [ $nofFiles -eq 0 ]; then
	echo "" >> ${LOGNAME}
	echo "No S3 Files found to include in manifest file" >> ${LOGNAME}

	exit 12
fi


#############################################################
# Get S3 Files; Convert list to comma-delimited string
# NOTE: This is needed for DOJ/FOIA requests to get JIRA ticket.
#############################################################
S3Files=`cat ${DATADIR}manTemp_${TMSTMP}.txt | awk '{print $4}' | tr '\n' ','` 2>> ${LOGNAME}
echo "S3Files=${S3Files}" >> ${LOGNAME}


#############################################################
# Download JIRA_Extract_Mappings.txt file to Linux.
#############################################################
echo "" >> ${LOGNAME}

## Copy JIRA mapping file to Linux
aws s3 cp s3://${CONFIG_BUCKET}${JIRA_MAPPING_FILE} ${DATADIR}${JIRA_MAPPING_FILE}   1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Copying ${CONFIG_BUCKET}${JIRA_MAPPING_FILE} from S3 to Linux failed." >> ${LOGNAME}

	exit 12
fi	


#############################################################
# Extract JIRA URL for Extract type
# NOTE: Perform "exception" logic for DOJ or FOIA JIRA tickets.
#       Match each JIRA_MAPPING_FILE DOJ-JIRA-ticket entry to the filename
#############################################################
if [ "${ExtractType}" = "DOJ" ] || [ "${ExtractType}" = "FOIA" ];then
	echo "Look for DOJ Jira ticket entry"   >> ${LOGNAME}
	findDOJFOIAJiraTicketMatch "${ExtractType}" "${S3Files}"
	if [ "${MatchingKey}" = "" ];then
		echo "" >> ${LOGNAME}
		echo "No MatchingKey found for filename ${S3ExtractFilename}" >> ${LOGNAME}

		exit 12	
	fi
else
	echo "Look for non-DOJ-FOIA Jira ticket entry"   >> ${LOGNAME}
	MatchingKey=${ExtractType}
fi

echo "Find matching entry in JIRA_MAPPING_FILE for ${MatchingKey}"   >> ${LOGNAME}

# Find matching entry in JIRA_MAPPING_FILE
jiraEntry=`grep "^${MatchingKey}=" ${DATADIR}${JIRA_MAPPING_FILE} | sed 's/\r//' `  2>> ${LOGNAME}

if [ -z ${jiraEntry:=""}  ]; then
	echo "" >> ${LOGNAME}
	echo "${JIRA_MAPPING_FILE} missing Extract Type ${MatchingKey} mapping." >> ${LOGNAME}

	exit 12
fi

jiraURL=`echo "${jiraEntry}" | cut -d= -f2 `

if [ -z ${jiraURL:=""} ]; then
	echo "" >> ${LOGNAME}
	echo "${JIRA_MAPPING_FILE} missing JIRA URL for Extract Type ${MatchingKey}." >> ${LOGNAME}

	exit 12
fi


#############################################################
# Get manifest filename
#############################################################
# Ex. NYSPAP_Manifest_20221006.093854.json
ManifestFilename=${MatchingKey}_Manifest_${S3FilenameTmstmp}.json
echo "ManifestFilename=${ManifestFilename}" >> ${LOGNAME}


#################################################################################
# Does Manifest config file exist ? 
#################################################################################
echo "" >> ${LOGNAME}
echo "Find configuration file for possible override to use S3 staging/hold folder. " >> ${LOGNAME}

aws s3 ls s3://${CONFIG_BUCKET}${MANIFEST_CONFIG_FILE}  >> ${LOGNAME}  2>&1

RET_STATUS=$?

if [[ $RET_STATUS = 0 ]]; then

	echo "" >> ${LOGNAME}
	echo "Looking at configuration file for possible override to use S3 staging/hold folder."  >> ${LOGNAME}

	# if config file exists --> copy to data directory
	aws s3 cp s3://${CONFIG_BUCKET}${MANIFEST_CONFIG_FILE}  ${DATADIR}${MANIFEST_CONFIG_FILE}  >> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Shell script CreateManifestFile.sh failed." >> ${LOGNAME}
		echo "Copying ${MANIFEST_CONFIG_FILE} from S3 to data directory - Failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Copying ${MANIFEST_CONFIG_FILE} from S3 to data directory - Failed"
		MSG="Copying ${MANIFEST_CONFIG_FILE} from S3 to data directory has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi
	
	# remove any CR characters from file (in-place)
	sed -i 's/\r//g' ${DATADIR}${MANIFEST_CONFIG_FILE} 

	# Find specific config value
	REC=`grep "^${ExtractType}.HOLD" ${DATADIR}${MANIFEST_CONFIG_FILE} ` 

	# iS staging/hold folder override present 
	if [ -n "${REC}" ]; then
		echo "Using S3 staging/hold override from ${MANIFEST_CONFIG_FILE} configuration file."  >> ${LOGNAME}

		# Extract staging/hold folder override from config file	
		HoldManifestFile=`echo ${REC} | cut -d= -f2 `   2>> ${LOGNAME}
		
	else
		echo "No staging/hold folder override value was present. Using default value."  >> ${LOGNAME}
	fi

	echo "HoldManifestFile=${HoldManifestFile}" >> ${LOGNAME} 
	
fi

 
#############################################################
# Execute Python code to create manifest file.
#############################################################
echo "" >> ${LOGNAME}
echo "Start execution of CreateManifestFile.py program"  >> ${LOGNAME}

echo "S3Bucket=${S3Bucket}" >> ${LOGNAME}
echo "S3Folder=${S3Folder}" >> ${LOGNAME}
echo "runToken=${S3FilenameTmstmp}" >> ${LOGNAME}
echo "RecipientEmails=${RecipientEmails}" >> ${LOGNAME}
echo "jiraURL=${jiraURL}" >> ${LOGNAME}

ManifestPathAndFilename=${DATADIR}${ManifestFilename}
echo "ManifestPathAndFilename=${ManifestPathAndFilename}" >> ${LOGNAME}


#############################################################
# Check the status of python script  
#############################################################
echo "" >> ${LOGNAME}
${PYTHON_COMMAND} ${RUNDIR}CreateManifestFilev2.py --bucket ${S3Bucket} --folder ${S3Folder} --runToken ${S3FilenameTmstmp} --REmails ${RecipientEmails} --outfile ${ManifestPathAndFilename} --jiraURL ${jiraURL}  >> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script CreateManifestFile.py failed" >> ${LOGNAME}

        exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script CreateManifestFile.py completed successfully. " >> ${LOGNAME}


#################################################################################
# Move manifest file(s) to S3 manifest folder. 
#################################################################################
echo "" >> ${LOGNAME}
echo "Move manifest files to S3"  >> ${LOGNAME}

ManifestFilenameWildcard=`echo ${ManifestFilename} | sed 's/.json/*.json/g' `
echo "ManifestFilenameWildcard=${ManifestFilenameWildcard}"  >> ${LOGNAME}

# get filenames and remove path
ManifestFiles2Move=`ls -1 ${DATADIR}${ManifestFilenameWildcard} | xargs -i basename {} `

echo "ManifestFiles2Move=${ManifestFiles2Move}"  >> ${LOGNAME}

for ManifestFile2Move in ${ManifestFiles2Move}
do

	## Copy manifest file to s3 manifest folder
	if [ "${HoldManifestFile}" = "Y" ];then
		aws s3 cp ${DATADIR}${ManifestFile2Move} s3://${MANIFEST_HOLD_BUCKET}${ManifestFile2Move}  1>> ${LOGNAME} 2>&1
	else
		aws s3 cp ${DATADIR}${ManifestFile2Move} s3://${S3MANIFEST_FOLDER_TO_USE}${ManifestFile2Move}  1>> ${LOGNAME} 2>&1
	fi

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Copying Manifest file to S3 manifest folder failed." >> ${LOGNAME}

		exit 12
	fi	

done

	
#################################################################################
# Remove manifest file from Linux data directory 
#################################################################################
echo "" >> ${LOGNAME}
echo "Remove Manifest file from Linux data directory." >> ${LOGNAME}

rm ${DATADIR}${ManifestFilenameWildcard} 2>> ${LOGNAME} 	


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
echo "CreateManifestFile.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS