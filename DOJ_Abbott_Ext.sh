#!/usr/bin/bash
############################################################################################################
# Name:  DOJ_Abbott_Ext.sh
#
# Desc: Extract for DOJ Abbott Extract
#
# Author     : Viren Khanna
# Created    : 08/26/2024
#
# Modified:
#
# Viren Khanna 2024-03-28 Create script
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP:=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/DOJ_Abbott_Ext_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

DOJ_ABBOTT_PARM_FILE=DOJ_ABBOTT_PARM_FILE.txt
##\/###
DOJ_ABBOTT_BOX_RECIPIENT="jcshah@millershah.com,bdparke@millershah.com,emwilson@millershah.com,shireenmatthews@jonesday.com,atrossum@jonesday.com,hohara@jonesday.com,sumathi.gayam1@cms.hhs.gov,Jagadeeshwar.Pagidimarri@cms.hhs.gov"

PTA_PTB_SW="B"
SINGLE_FILE_PHRASE=""


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "DOJ_Abbott_Ext.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh

source ${RUNDIR}FilenameCounts.bash

S3BUCKET=${DOJ_BUCKET} 

echo "DOJ bucket=${S3BUCKET}" >> ${LOGNAME}

source ${RUNDIR}createManifestFileFunc.sh


#################################################################################
# Download DOJ date Parameter file from S3 to data directory.
# NOTE: Make sure that the last record in the DOJ_PARM_FILE has an 
#       ending \n. (Press <ENTER> after each record in file). 
#################################################################################
echo "" >> ${LOGNAME}
echo "Copy DOJ parm file ${DOJ_ABBOTT_PARM_FILE} from S3 to linux" >> ${LOGNAME}


# Copy S3 file to linux
aws s3 cp s3://${CONFIG_BUCKET}${DOJ_ABBOTT_PARM_FILE} ${DATADIR}${DOJ_ABBOTT_PARM_FILE}  1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Copying S3 DOJ ${DOJ_ABBOTT_PARM_FILE} parameter file to Linux failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="DOJ_Abbott_Ext.sh - Failed ($ENVNAME)"
	MSG="Copying S3 DOJ ${DOJ_ABBOTT_PARM_FILE} parameter file from ${CONFIG_BUCKET} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	


#################################################################################
# Find SAF ENC HHA Extract Years Parameter file in data directory.
#################################################################################
ParmFile2Process=`ls ${DATADIR}${DOJ_ABBOTT_PARM_FILE}` 1>> ${LOGNAME}  2>&1

echo "" >> ${LOGNAME}
echo "DOJ Abbott Extract Parameter file on linux: ${ParmFile2Process}" >> ${LOGNAME}


#################################################################################
# Loop thru Date Ranges in DOJ parameter file.
# 
# CLM_TYPE_LIT,FROM-DT,TO-DT,FILE_LIT
# HHA,2021-01-01,2021-12-31,HHA2021
# HHA,2021-01-01,2022-12-31,HHA2022
#
# NOTE: The sed command will remove the \r that may appear in parameter file when
#       the file is uploaded from windows to S3. The \r character may 
#       prevent the file from being processed properly.
#################################################################################

sed -i 's/\r//g' ${ParmFile2Process}  2>> ${LOGNAME} 


#################################################################################
# Set Max NOF Extract files to include in Manifest file
#################################################################################
##\/###
NOF_EXT_FILES_PER_MANIFEST=1
setMaxFiles2Manifest ${NOF_EXT_FILES_PER_MANIFEST}


while read PARM_REC
do

		#############################################################
		# Start extract for next parameter year
		#############################################################
		echo " " >> ${LOGNAME}
		echo "-----------------------------------" >> ${LOGNAME}
		
		# Display Parameter file record
		echo "Parameter record=${PARM_REC}" >> ${LOGNAME}

		# skip blank lines
		if [ -z "${PARM_REC}" ];then
			continue
		fi
		
		# skip comment lines
		FIRST_CHAR=`echo ${PARM_REC} | cut -c1 ` >> ${LOGNAME}
		if [ "$FIRST_CHAR" = "#" ];then
			echo "Skip comment record"  >> ${LOGNAME}
			continue
		fi	

		#################################################################################
		# Load parameters for Extract
		#################################################################################
		echo " " >> ${LOGNAME}

		CLM_TYPE_LIT=`echo ${PARM_REC} | cut -d, -f1`  2>> ${LOGNAME}
		EXT_FROM_DT=`echo ${PARM_REC} | cut -d, -f2`  2>> ${LOGNAME}
		EXT_TO_DT=`echo ${PARM_REC} | cut -d, -f3`    2>> ${LOGNAME}
		FILE_LIT=`echo ${PARM_REC} | cut -d, -f4`    2>> ${LOGNAME}
		
		echo "CLM_TYPE_LIT=${CLM_TYPE_LIT}" >> ${LOGNAME}
		echo "EXT_FROM_DT=${EXT_FROM_DT}" >> ${LOGNAME}
		echo "EXT_TO_DT=${EXT_TO_DT}" >> ${LOGNAME}
		echo "FILE_LIT=${FILE_LIT}" >> ${LOGNAME}
			
		PART="B"
		
        #############################################################
        # Get claim-type codes
        #############################################################
		case $CLM_TYPE_LIT
		in

			CAR)
				#PART=4
				CLM_TYPE_CODES="71,72,81,82"
				PTA_PTB_SW=B
				SINGLE_FILE_PHRASE=""	
				
				##\/###
				NOF_EXT_FILES_PER_MANIFEST=1
				setMaxFiles2Manifest ${NOF_EXT_FILES_PER_MANIFEST}				
				;;
			
						
			*)
			
				echo "Invalid claim type literal ${CLM_TYPE_LIT} on parameter record." >> ${LOGNAME}
				
				# Send Failure email	
				SUBJECT="DOJ_Abbott_Ext  - Failed ($ENVNAME)"
				MSG="DOJ Abbott extract has failed. \nInvalid claim type literal ${CLM_TYPE_LIT} on parameter record."
				${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
				
				exit 12
			
		esac

		echo "PART=${PART}" >> ${LOGNAME}
		
		echo "CLM_TYPE_CODES=${CLM_TYPE_CODES} " >> ${LOGNAME}
		echo "PTA_PTB_SW=${PTA_PTB_SW}"  >> ${LOGNAME}
		echo "SINGLE_FILE_PHRASE=${SINGLE_FILE_PHRASE}"  >> ${LOGNAME}
		
	
        #############################################################
		# Export environment variables for Python code
		#
		# NOTE: Need a unique Timestamp for each extract so that we can
		#       create a single manifest file for each extract file.
		#       Apparently, BOX has concurrency issues, and possible
		#       download size limitations. 
        #############################################################
##\/###
		export EXTRACT_FILE_TMSTMP
        export CLM_TYPE_LIT
		export FILE_LIT
		export PART

		export CLM_TYPE_CODES
		export EXT_FROM_DT
		export EXT_TO_DT
		export SINGLE_FILE_PHRASE

		
        #############################################################
        # Execute Python code to extract data.
        #############################################################
		echo "" >> ${LOGNAME}
##\/###
		if [ "${PTA_PTB_SW}" = "A" ];then
			echo "Start execution of DOJ_Abbott_Ext_PTA.py program"  >> ${LOGNAME}
			${PYTHON_COMMAND} ${RUNDIR}DOJ_Abbott_Ext_PTA.py >> ${LOGNAME} 2>&1
		else
			echo "Start execution of DOJ_Abbott_Ext_PTB.py program"  >> ${LOGNAME}
			${PYTHON_COMMAND} ${RUNDIR}DOJ_Abbott_Ext_PTB.py >> ${LOGNAME} 2>&1
		fi
		
		#############################################################
		# Check the status of python script  
		#############################################################
		RET_STATUS=$?

		if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Python script DOJ_Abbott_Ext_[PTA/PTB].py failed" >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="DOJ_Abbott_Ext_[PTA/PTB].py - Failed ($ENVNAME)"
			MSG="DOJ Abbott extract has failed. Python script DOJ_Abbott_Ext_[PTA/PTB].py failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
		fi

		echo "" >> ${LOGNAME}
		if [ "PTA_PTB_SW" = "A" ];then
			echo "Python script DOJ_Abbott_Ext_PTA.py completed successfully. " >> ${LOGNAME}
		else
			echo "Python script DOJ_Abbott_Ext_PTB.py completed successfully. " >> ${LOGNAME}
		fi


        ####################################################################
		# Concatenate S3 files
		# NOTE: Multiple files with suffix "n_n_n.csv.gz" are created. 
		#       Will concatenate them into single file.
		#
		# Example --> blbtn_clm_ex_20220922.084321.csv.gz_0_0_0.csv.gz 
		#         --> blbtn_clm_ex_20220922.084321.csv.gz
		####################################################################
		echo "" >> ${LOGNAME}
		echo "Concatenate S3 files using CombineS3Files.sh   " >> ${LOGNAME}

		echo "S3BUCKET=${S3BUCKET} " >> ${LOGNAME} 

		concatFilename=DOJ_ABBOTT_PTB_FFS_${FILE_LIT}_${EXTRACT_FILE_TMSTMP}.txt.gz
		echo "concatFilename=${concatFilename}" >> ${LOGNAME} 

		${RUNDIR}CombineS3Files.sh ${S3BUCKET} ${concatFilename} 


		#############################################################
		# Check the status of script
		#############################################################
		RET_STATUS=$?

		if [[ $RET_STATUS != 0 ]]; then
				echo "" >> ${LOGNAME}
				echo "Shell script CombineS3Files.sh failed." >> ${LOGNAME}
				
				# Send Failure email	
				SUBJECT="Combining S3 files in DOJ_Abbott_Ext - Failed ($ENVNAME)"
				MSG="Combining S3 files in DOJ_Abbott_Ext.sh has failed."
				${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

				exit 12
		fi	

		PARTIAL_MANIFEST_FILE_ALLOWED="N"		
		createManifestFileFunc ${DOJ_ABBOTT_BOX_RECIPIENT}  ${PARTIAL_MANIFEST_FILE_ALLOWED}


done <  ${ParmFile2Process}


#############################################################
# Create manifest file for last files processed.
#############################################################
PARTIAL_MANIFEST_FILE_ALLOWED="Y"		
createManifestFileFunc ${DOJ_ABBOTT_BOX_RECIPIENT}  ${PARTIAL_MANIFEST_FILE_ALLOWED}

	
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

SUBJECT="DOJ Abbott extract ($ENVNAME) " 
MSG="The Extract for the creation of the DOJ Abbott data pull has completed.\n\nThe following file(s) were created:\n\n${S3Files}"

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Error in calling sendEmail.py" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Sending Success email in DOJ_Abbott_Ext.sh  - Failed ($ENVNAME)"
		MSG="Sending Success email in DOJ_Abbott_Ext.sh  has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
fi	


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME} 
echo "Remove ${DOJ_ABBOTT_PARM_FILE} from data directory" >> ${LOGNAME} 

rm ${DATADIR}${DOJ_ABBOTT_PARM_FILE}  >> ${LOGNAME} 2>&1


#############################################################
# end script
#############################################################
echo "" >> ${LOGNAME}
echo "DOJ_Abbott_Ext.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS