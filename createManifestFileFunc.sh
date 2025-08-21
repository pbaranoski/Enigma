#!/usr/bin/bash

############################################################################################################
# Name:  createManifestFileFunc.sh
#
# Desc: This is a child module/script to include in parent scripts to handle the creation of manifest files 
#       to limit the NOF extract files included in each manifest file.
#
# Author     : Paul Baranoski	
# Created    : 06/14/2024
#
# Modified:
#
# Paul Baranoski 2024-06-14 Create script.
###########################################################################################################

# !!!!NOTE: Below constant is expected to be defined in the script
# S3BUCKET

# CONSTANTS needed by function that can be referenced by main script
# MAX_FILES_2_MANIFEST can be overridden by call to setMaxFiles2Manifest. Otherwise, default value is 4.
# FORCE_MANIFEST_FILE_WRITE switch allows a partially filled manifest file to be written. Should be last call from parent script.

NOF_FILES_IN_MANIFEST=0
MAX_FILES_2_MANIFEST=4
EXTRACT_FILE_TMSTMP=`date +%Y%m%d.%H%M%S`

SCRIPT_NAME=`basename $0` 

function setMaxFiles2Manifest() {

	echo "" >> ${LOGNAME}
	echo "In setMaxFiles2Manifest function " >> ${LOGNAME}

	MAX_FILES_2_MANIFEST=$1
	
	echo "MAX_FILES_2_MANIFEST=${MAX_FILES_2_MANIFEST} " >> ${LOGNAME}
}


function createManifestFileFunc () {

	echo "" >> ${LOGNAME}
	echo "In createManifestFileFunc function " >> ${LOGNAME}

	#############################################################
	# Get parameters to function
	#############################################################
	BOX_EMAIL_RECIPIENT=$1
	FORCE_MANIFEST_FILE_WRITE=$2
	
	echo "BOX_EMAIL_RECIPIENT=${BOX_EMAIL_RECIPIENT}" >> ${LOGNAME}
	echo "FORCE_MANIFEST_FILE_WRITE=${FORCE_MANIFEST_FILE_WRITE}" >> ${LOGNAME}

	#############################################################
	# Get parameters to function
	#############################################################
	if [ ${FORCE_MANIFEST_FILE_WRITE} = "N" ];then	

		NOF_FILES_IN_MANIFEST=`expr ${NOF_FILES_IN_MANIFEST} + 1`
		echo "NOF_FILES_IN_MANIFEST=${NOF_FILES_IN_MANIFEST} " >> ${LOGNAME}
	
		if [ ${NOF_FILES_IN_MANIFEST} -eq ${MAX_FILES_2_MANIFEST}  ];then
			echo "Threshold met to create manifest file " >> ${LOGNAME}
			# fall-thru to code below if statement
		else
			echo "Threshold not met to create manifest file " >> ${LOGNAME}
			echo "Exit createManifestFile function " >> ${LOGNAME}
			
			return 0	
		fi
	else
		# force creation of manifest file to include remaining files 
		if [ ${NOF_FILES_IN_MANIFEST} -eq 0  ];then
			echo "No remaining extract files, so no manifest file created. " >> ${LOGNAME}
			echo "Exit createManifestFile function " >> ${LOGNAME}
			
			return 0
		fi
	fi	

		
	#####################################################
	# S3BUCKET             --> points to location of extract file. 
	#                      --> S3 folder is key token to config file to determine manifest files are in HOLD status   
	# EXTRACT_FILE_TMSTMP  --> uniquely identifies extract file(s) to be included in manifest file 
	# BOX_EMAIL_RECIPIENT  --> manifest file recipients
	#
	# Ex. CreateManifestFile.sh s3://aws-hhs-cms-eadg-bia-ddom-extracts/xtr/DOJ/ 20231211.125522 pbaranoski-con@index.com 
	#####################################################
	echo "Call createManifestFile.sh script " >> ${LOGNAME}

	${RUNDIR}CreateManifestFile.sh ${S3BUCKET} ${EXTRACT_FILE_TMSTMP} ${BOX_EMAIL_RECIPIENT} 

			
	#############################################################
	# Check the status of script
	#############################################################
	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Shell script CreateManifestFile.sh failed." >> ${LOGNAME}
		
		# Send Failure email	
		-- how to get current script name
		SUBJECT="Create Manifest file in ${SCRIPT_NAME} - Failed ($ENVNAME)"
		MSG="Create Manifest file in ${SCRIPT_NAME}  has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi

	#############################################################
	# Reset create manifest file variables for next manifest file
	#############################################################
	echo "Reset manifest file variables for next manifest file " >> ${LOGNAME}
	
	NOF_FILES_IN_MANIFEST=0
	EXTRACT_FILE_TMSTMP=`date +%Y%m%d.%H%M%S`

	echo "Next EXTRACT_FILE_TMSTMP=${EXTRACT_FILE_TMSTMP} " >> ${LOGNAME}
	echo "Exit createManifestFile function " >> ${LOGNAME}

}