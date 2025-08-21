#!/usr/bin/bash
############################################################################################################
# Name:  DOJ_ModernVascular_Ext.sh
#
# Desc: Extract for DOJ Modern Vascular
#
# Author     : Paul Baranoski	
# Created    : 01/21/2025
#
# Modified:
#
# Paul Baranoski 2025-01-21 Create script.
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP:=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/DOJ_ModernVascular_Ext_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

DOJ_MODERN_VASCULAR_PARM_FILE=DOJ_ModernVascular_PARM_FILE.txt
PREFIX=DOJ_ModernVascular


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "DOJ_ModernVascular_Ext.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh

source ${RUNDIR}FilenameCounts.bash

S3BUCKET=${DOJ_BUCKET} 

echo "FOIA bucket=${S3BUCKET}" >> ${LOGNAME}


#################################################################################
# Download DOJ date Parameter file from S3 to data directory.
# NOTE: Make sure that the last record in the DOJ_PARM_FILE has an 
#       ending \n. (Press <ENTER> after each record in file). 
#################################################################################
echo "" >> ${LOGNAME}
echo "Copy DOJ parm file ${DOJ_MODERN_VASCULAR_PARM_FILE} from S3 to linux" >> ${LOGNAME}

# Copy S3 file to linux
aws s3 cp s3://${CONFIG_BUCKET}${DOJ_MODERN_VASCULAR_PARM_FILE} ${DATADIR}${DOJ_MODERN_VASCULAR_PARM_FILE}  1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Copying S3 DOJ ${DOJ_MODERN_VASCULAR_PARM_FILE} parameter file to Linux failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="DOJ_ModernVascular_Ext.sh - Failed ($ENVNAME)"
	MSG="Copying S3 DOJ ${DOJ_MODERN_VASCULAR_PARM_FILE} parameter file from ${CONFIG_BUCKET} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	


#################################################################################
# Find Parameter file in data directory.
#################################################################################
ParmFile2Process=`ls ${DATADIR}${DOJ_MODERN_VASCULAR_PARM_FILE}` 1>> ${LOGNAME}  2>&1

echo "" >> ${LOGNAME}
echo "DOJ Modern Vascular Extract Parameter file on linux: ${ParmFile2Process}" >> ${LOGNAME}

# Remove carriage returns from parm file
sed -i 's/\r//g' ${ParmFile2Process}  2>> ${LOGNAME} 

# Add ending newline character in case its missing for last record. 
printf "\n" >>  ${ParmFile2Process}


#################################################################################
# Loop thru Date Ranges in DOJ parameter file.
# 
# NPI_NUM, OBL, ENUM_DT, EXT_YR
# 1619407442,ModVascular_Institute,2017-06-14,2018
# 1619407442,ModVascular_Institute,2017-06-14,2019
#
#################################################################################
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

		NPI_NUM=`echo ${PARM_REC} | cut -d, -f1`  2>> ${LOGNAME}
		OBL=`echo ${PARM_REC} | cut -d, -f2`  2>> ${LOGNAME}
		ENUM_DT=`echo ${PARM_REC} | cut -d, -f3`    2>> ${LOGNAME}
		EXT_YR=`echo ${PARM_REC} | cut -d, -f4`    2>> ${LOGNAME}
		
		echo "NPI_NUM=${NPI_NUM}" >> ${LOGNAME}
		echo "OBL=${OBL}" >> ${LOGNAME}
		echo "ENUM_DT=${ENUM_DT}" >> ${LOGNAME}
		echo "EXT_YR=${EXT_YR}" >> ${LOGNAME}
		
	
        #############################################################
		# Export environment variables for Python code
		#
        #############################################################
		export TMSTMP

		export NPI_NUM
		export OBL
		export ENUM_DT
		export EXT_YR

		
        #############################################################
        # Execute Python code to extract data.
        #############################################################
		echo "" >> ${LOGNAME}

		echo "Start execution of DOJ_ModernVascular_Ext.py program"  >> ${LOGNAME}
		${PYTHON_COMMAND} ${RUNDIR}DOJ_ModernVascular_Ext.py >> ${LOGNAME} 2>&1

		
		#############################################################
		# Check the status of python script  
		#############################################################
		RET_STATUS=$?

		if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Python script DOJ_ModernVascular_Ext.py failed" >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="DOJ_ModernVascular_Ext.py - Failed ($ENVNAME)"
			MSG="DOJ Pistarino extract has failed. Python script DOJ_ModernVascular_Ext.py failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
		fi

		echo "" >> ${LOGNAME}
		echo "Python script DOJ_ModernVascular_Ext.py completed successfully. " >> ${LOGNAME}


		#################################################################################
		# Was an extract file created to combine?
		#################################################################################
		echo "" >> ${LOGNAME}

		concatFilename=DOJ_ModernVascular_${NPI_NUM}_${OBL}_Y${EXT_YR}_${TMSTMP}.txt.gz
		echo "concatFilename=${concatFilename}" >> ${LOGNAME} 
		
		echo "Was an exctract file created for ${concatFilename}" >> ${LOGNAME}

		NOF_FILES=`aws s3 ls s3://${S3BUCKET}${concatFilename} --summarize | grep 'Total Objects' | cut -d: -f2 | sed 's/ //g' ` 2>> ${LOGNAME}

		RET_STATUS=$?

		if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Counting NOF S3 extract files in s3://${S3BUCKET}${concatFilename} failed." >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="DOJ_ModernVascular_Ext.sh - Failed ($ENVNAME)"
			MSG="Counting NOF S3 extract files in s3://${S3BUCKET}${concatFilename} failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
		fi	

		echo "NOF_FILES=${NOF_FILES}"  >> ${LOGNAME}


        ####################################################################
		# Concatenate S3 files
		# NOTE: Multiple files with suffix "n_n_n.csv.gz" are created. 
		#       Will concatenate them into single file.
		#
		# Example --> blbtn_clm_ex_20220922.084321.csv.gz_0_0_0.csv.gz 
		#         --> blbtn_clm_ex_20220922.084321.csv.gz
		####################################################################
		if [ ${NOF_FILES} -gt 1 ];then
		
			echo "" >> ${LOGNAME}
			echo "Concatenate S3 files using CombineS3Files.sh   " >> ${LOGNAME}
			
			${RUNDIR}CombineS3Files.sh ${S3BUCKET} ${concatFilename} 


			#############################################################
			# Check the status of script
			#############################################################
			RET_STATUS=$?

			if [[ $RET_STATUS != 0 ]]; then
					echo "" >> ${LOGNAME}
					echo "Shell script CombineS3Files.sh failed." >> ${LOGNAME}
					
					# Send Failure email	
					SUBJECT="Combining S3 files in DOJ_ModernVascular_Ext - Failed ($ENVNAME)"
					MSG="Combining S3 files in DOJ_ModernVascular_Ext.sh has failed."
					${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

					exit 12
			fi	
		fi

done <  ${ParmFile2Process}
	
	
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

SUBJECT="DOJ Modern Vascular extract ($ENVNAME) " 
MSG="The Extract for the creation of the DOJ Modern Vascular data pull has completed.\n\nThe following file(s) were created:\n\n${S3Files}"

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Error in calling sendEmail.py" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Sending Success email in DOJ_ModernVascular_Ext.sh  - Failed ($ENVNAME)"
		MSG="Sending Success email in DOJ_ModernVascular_Ext.sh  has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
fi	


#############################################################
# Get list of .gz zipped files in S3/DOJ/DOJ_ModernVascular*
#############################################################
echo "" >> ${LOGNAME}
echo "Get s3 zipped files to process in ${S3BUCKET}${PREFIX}" >> ${LOGNAME}


echo "aws s3 ls s3://${S3BUCKET}${PREFIX} | grep '${TMSTMP}' | awk '{print $4}'" >> ${LOGNAME}

ZIPFILES2PROCESS=`aws s3 ls s3://${S3BUCKET}${PREFIX} | grep ${TMSTMP} | awk '{print $4}' ` 2>> ${LOGNAME}

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Get s3 zipped files to process in s3://${S3BUCKET}${PREFIX} failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="DOJ Modern Vascular Extract - Failed ($ENVNAME)"
	MSG="Get s3 zipped files to process in ${S3BUCKET}${PREFIX} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	

echo "ZIPFILES2PROCESS=${ZIPFILES2PROCESS}"  >> ${LOGNAME}


#############################################################
# Get parameters needed for UnzipS3File.py program
#
# Get the S3 bucket only
# bucket = 'aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod/xtr/DEV/' or 'aws-hhs-cms-eadg-bia-ddom-extracts/xtr/'
# echo 'aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod/xtr/DEV/' | cut -d/ -f1
#############################################################
S3BucketAndHLFolder=${S3BUCKET}
S3Bucket=`echo ${S3BucketAndHLFolder} | cut -d/ -f1 `  2>> ${LOGNAME}
S3HLFolder=`echo ${S3BucketAndHLFolder} | cut -d/ -f2- `  2>> ${LOGNAME}


#############################################################
# Loop thru zipped files to unzip and move back to S3.
#############################################################
for gz_filename in ${ZIPFILES2PROCESS}
do


	echo "" >> ${LOGNAME}
	echo "*****************************************************************" >> ${LOGNAME}
	echo "gz_filename=${gz_filename}" >> ${LOGNAME}

	unzipped_filename=`echo ${gz_filename} | sed -e 's/.gz//' `  2>> ${LOGNAME} 
	echo "unzipped_filename=${unzipped_filename}" >> ${LOGNAME}


	###################################################################
	# Call python program to unzip Extract file from Extract folder,
	# and place parts in Files2EFT folder.
	###################################################################
	# Ex. SOURCE_BUCKET=aws-hhs-cms-eadg-bia-ddom-extracts/xtr/DOJ
	# Ex. ZIP_FILE=xtr/DEV/Blbtn/filename.gz 
	# Ex. UNZIP_FILE=xtr/DEV/Files2EFT/filename.txt
	###################################################################
	SOURCE_BUCKET=${S3Bucket}
	ZIP_FILE=${S3HLFolder}${S3ExtractFolder}${gz_filename}
	UNZIP_FILE=${S3HLFolder}${unzipped_filename} 

	echo "SOURCE_BUCKET=${SOURCE_BUCKET}" >> ${LOGNAME}
	echo "ZIP_FILE=${ZIP_FILE}" >> ${LOGNAME}
	echo "UNZIP_FILE=${UNZIP_FILE}" >> ${LOGNAME}

	#############################################################
	# Execute Python code to extract data.
	#############################################################
	echo "" >> ${LOGNAME}
	echo "Start execution of UnzipS3File.py program"  >> ${LOGNAME}
	${PYTHON_COMMAND} ${RUNDIR}UnzipS3File.py --bucket ${SOURCE_BUCKET} --zipFile ${ZIP_FILE} --unzipFile ${UNZIP_FILE}   >> ${LOGNAME} 2>&1


	#############################################################
	# Check the status of python script  
	#############################################################
	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Python script UnzipS3File.py failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="DOJ Modern Vascular Extract - Failed ($ENVNAME)"
		MSG="Python script UnzipS3File.py failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi

	echo "Python script UnzipS3File.py completed successfully. " >> ${LOGNAME}	


	
done


#############################################################
# Get count of S3 files to include in manifest file.
#############################################################
echo "" >> ${LOGNAME}
echo "Count NOF extract files to include in the manifest file " >> ${LOGNAME}

NOF_FILES_4_MANIFEST=`aws s3 ls s3://${S3BUCKET} | grep ${TMSTMP} | wc -l `

RET_STATUS=$?

if [ $RET_STATUS != 0 ]; then
	echo "" >> ${LOGNAME}
	echo "Error in getting count of extract files to include in manifest file. DOJ_ModernVascular_Ext.sh Failed " >> ${LOGNAME}

	# Send Failure email	
	SUBJECT="Error getting count of extract files for manifest file. DOJ_ModernVascular_Ext.sh Failed ($ENVNAME)"
	MSG="Error in getting count of extract files to include in manifest file. DOJ_ModernVascular_Ext.sh has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1


	exit 12
fi

echo "NOF_FILES_4_MANIFEST=${NOF_FILES_4_MANIFEST}" >> ${LOGNAME}
	

#############################################################
# Get NOF files for manifest file. Cannot create manifest file 
#   when there are 0 files.
#############################################################
if [ ${NOF_FILES_4_MANIFEST} -gt 0 ];then 

	#############################################################
	# Create Manifest file
	#############################################################
	echo "" >> ${LOGNAME}
	echo "Create Manifest file for DOJ Modern Vascular Extract.  " >> ${LOGNAME}

	#####################################################
	# S3BUCKET --> points to location of extract file. 
	#          --> S3 folder is key token to config file to determine of manifest file is in HOLD status   
	# TMSTMP   --> uniquely identifies extract file(s) 
	# ENIGMA_EMAIL_SUCCESS_RECIPIENT --> manifest file recipients
	#
	# Ex. CreateManifestFile.sh s3://aws-hhs-cms-eadg-bia-ddom-extracts/xtr/DOJ/ 20231211.125522 pbaranoski-con@index.com 
	#####################################################
	BOX_RECIPIENTS="Jared.S.Wiesner2@usdoj.gov,Emily.Oren@hhs.gov,erica.h.ma@usdoj.gov,adithi.s.grama@usdoj.gov,lon.leavitt@usdoj.gov,jagadeeshwar.pagidimarri@cms.hhs.gov,monica.algozer@cms.hhs.gov" 
	
	${RUNDIR}CreateManifestFile.sh ${S3BUCKET} ${TMSTMP} ${BOX_RECIPIENTS} 
			
	#############################################################
	# Check the status of script
	#############################################################
	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Shell script CreateManifestFile.sh failed." >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="Create Manifest file in DOJ_ModernVascular_Ext.sh  - Failed ($ENVNAME)"
			MSG="Create Manifest file in DOJ_ModernVascular_Ext.sh  has failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
	fi	
fi


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME} 
echo "Remove ${DOJ_MODERN_VASCULAR_PARM_FILE} from data directory" >> ${LOGNAME} 

rm ${DATADIR}${DOJ_MODERN_VASCULAR_PARM_FILE}  >> ${LOGNAME} 2>&1


#############################################################
# end script
#############################################################
echo "" >> ${LOGNAME}
echo "DOJ_ModernVascular_Ext.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS