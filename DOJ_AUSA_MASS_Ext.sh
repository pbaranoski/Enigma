#!/usr/bin/bash
############################################################################################################
# Name:  DOJ_AUSA_MASS_Ext.sh
#
# Desc: Extract for DOJ AUSA MASS Extract. Extract INP, OPT, and Carrier for specific NPIs.
#
# Author     : Paul Baranoski	
# Created    : 02/26/2025
#
# Modified:
#
# Paul Baranoski 2025-02-26 Create script.
#
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP:=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/DOJ_AUSA_MASS_Ext_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

DOJ_AUSA_MASS_PARM_FILE=DOJ_AUSA_MASS_PARM_FILE.txt
PTA_PTB_SW=""
SQL_SAMPLE_PHRASE=""
SINGLE_FILE_PHRASE=""

PYTHON_ERROR_MSG=""


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "DOJ_AUSA_MASS_Ext.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh

source ${RUNDIR}FilenameCounts.bash

S3BUCKET=${DOJ_BUCKET} 

echo "DOJ bucket=${S3BUCKET}" >> ${LOGNAME}

DOJ_BOX_RECIPIENT=jagadeeshwar.pagidimarri@cms.hhs.gov


#################################################################################
# Download DOJ date Parameter file from S3 to data directory.
# NOTE: Make sure that the last record in the DOJ_PARM_FILE has an 
#       ending \n. (Press <ENTER> after each record in file). 
#################################################################################
echo "" >> ${LOGNAME}
echo "Copy DOJ parm file ${DOJ_AUSA_MASS_PARM_FILE} from S3 to linux" >> ${LOGNAME}


# Copy S3 file to linux
aws s3 cp s3://${CONFIG_BUCKET}${DOJ_AUSA_MASS_PARM_FILE} ${DATADIR}${DOJ_AUSA_MASS_PARM_FILE}  1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Copying S3 DOJ ${DOJ_AUSA_MASS_PARM_FILE} parameter file to Linux failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="DOJ_AUSA_MASS_Ext.sh - Failed ($ENVNAME)"
	MSG="Copying S3 DOJ ${DOJ_AUSA_MASS_PARM_FILE} parameter file from ${CONFIG_BUCKET} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	


#################################################################################
# Find SAF ENC HHA Extract Years Parameter file in data directory.
#################################################################################
ParmFile2Process=`ls ${DATADIR}${DOJ_AUSA_MASS_PARM_FILE}` 1>> ${LOGNAME}  2>&1

echo "" >> ${LOGNAME}
echo "DOJ AUSA MASS Extract Parameter file on linux: ${ParmFile2Process}" >> ${LOGNAME}


#################################################################################
# Loop thru Date Ranges in DOJ parameter file.
# 
# CLM_TYPE_LIT,FROM-DT,TO-DT,FILE_LIT
# INP,2021-01-01,2021-12-31,INP2021
# INP,2021-01-01,2022-12-31,INP2022
#
# NOTE: The sed command will remove the \r that may appear in parameter file when
#       the file is uploaded from windows to S3. The \r character may 
#       prevent the file from being processed properly.
#################################################################################

sed -i 's/\r//g' ${ParmFile2Process}

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

		FF_NPI_NUM=`echo ${PARM_REC} | cut -d, -f1`    2>> ${LOGNAME}
		CLM_TYPE_LIT=`echo ${PARM_REC} | cut -d, -f2`  2>> ${LOGNAME}
		EXT_FROM_YYYY=`echo ${PARM_REC} | cut -d, -f3`  2>> ${LOGNAME}
		EXT_TO_YYYY=`echo ${PARM_REC} | cut -d, -f4`    2>> ${LOGNAME}


		echo "FF_NPI_NUM=${FF_NPI_NUM}" >> ${LOGNAME}		
		echo "CLM_TYPE_LIT=${CLM_TYPE_LIT}" >> ${LOGNAME}
		echo "EXT_FROM_YYYY=${EXT_FROM_YYYY}" >> ${LOGNAME}
		echo "EXT_TO_YYYYY=${EXT_TO_YYYY}" >> ${LOGNAME}
			
		
        #############################################################
        # Get claim-type codes
        #############################################################
		case $CLM_TYPE_LIT
		in
	
			INP)
				CLM_TYPE_CODES="60,61"
				PTA_PTB_SW=A
				SINGLE_FILE_PHRASE="SINGLE=TRUE"
				;;
			
			OPT)
				CLM_TYPE_CODES="40"
				PTA_PTB_SW=A
				SINGLE_FILE_PHRASE="SINGLE=TRUE"
				;;

			CAR)
				CLM_TYPE_CODES="71,72"
				PTA_PTB_SW=B
                #SINGLE_FILE_PHRASE=""
				SINGLE_FILE_PHRASE="SINGLE=TRUE"	
				;;

			
			*)
			
				echo "Invalid claim type literal ${CLM_TYPE_LIT} on parameter record." >> ${LOGNAME}
				
				# Send Failure email	
				SUBJECT="DOJ_AUSA_MASS_Ext  - Failed ($ENVNAME)"
				MSG="DOJ AntiTrust extract has failed. \nInvalid claim type literal ${CLM_TYPE_LIT} on parameter record."
				${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
				
				exit 12
			
		esac

		
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

		export TMSTMP
        export CLM_TYPE_LIT
		export FF_NPI_NUM

		export CLM_TYPE_CODES
		export EXT_FROM_YYYY
		export EXT_TO_YYYY
		export SINGLE_FILE_PHRASE

		
        #############################################################
        # Execute Python code to extract data.
        #############################################################
		echo "" >> ${LOGNAME}

		if [ "${PTA_PTB_SW}" = "A" ];then
			echo "Start execution of DOJ_AUSA_MASS_Ext_PTA.py program"  >> ${LOGNAME}
			${PYTHON_COMMAND} ${RUNDIR}DOJ_AUSA_MASS_Ext_PTA.py >> ${LOGNAME} 2>&1
		else
			echo "Start execution of DOJ_AUSA_MASS_Ext_PTB.py program"  >> ${LOGNAME}
			${PYTHON_COMMAND} ${RUNDIR}DOJ_AUSA_MASS_Ext_PTB.py >> ${LOGNAME} 2>&1
		fi
		
		#############################################################
		# Check the status of python script  
		#############################################################
		RET_STATUS=$?

		if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Python script DOJ_AUSA_MASS_Ext_[PTA/PTB].py failed" >> ${LOGNAME}

			PYTHON_ERROR_MSG=`printenv | grep "PYTHON_ERROR_MSG" | cut -d= -f2 ` >> ${LOGNAME} 2>&1
			echo "PYTHON_ERROR_MSG: ${PYTHON_ERROR_MSG}" >> ${LOGNAME}
		
			# Send Failure email	
			SUBJECT="DOJ_AUSA_MASS_Ext_[PTA/PTB].py - Failed ($ENVNAME)"
			MSG="DOJ AntiTrust extract has failed. Python script DOJ_AUSA_MASS_Ext_[PTA/PTB].py failed with error: ${PYTHON_ERROR_MSG}."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
		fi

		echo "" >> ${LOGNAME}
		if [ "PTA_PTB_SW" = "A" ];then
			echo "Python script DOJ_AUSA_MASS_Ext_PTA.py completed successfully. " >> ${LOGNAME}
		else
			echo "Python script DOJ_AUSA_MASS_Ext_PTB.py completed successfully. " >> ${LOGNAME}
		fi

        ####################################################################
		# Did we create a file that needs combining
        ####################################################################
		#NOF_FILES=`aws s3 ls s3://${S3BUCKET}${DOJ_AUSA_MASS_${FF_NPI_NUM}_${CLM_TYPE_LIT}_${EXT_FROM_YYYY}_${EXT_TO_YYYY}_${TMSTMP}.txt.gz} --summarize | grep 'Total Objects' | cut -d: -f2 | sed 's/ //g' ` 2>> ${LOGNAME}

        ####################################################################
		# Concatenate S3 files
		# NOTE: Multiple files with suffix "n_n_n.csv.gz" are created. 
		#       Will concatenate them into single file.
		#
		# Example --> blbtn_clm_ex_20220922.084321.csv.gz_0_0_0.csv.gz 
		#         --> blbtn_clm_ex_20220922.084321.csv.gz
		####################################################################
		#if [ ${NOF_FILES} -gt 0 ];then
		#	echo "" >> ${LOGNAME}
		#	echo "Concatenate S3 files using CombineS3Files.sh   " >> ${LOGNAME}
        #
		#	echo "S3BUCKET=${S3BUCKET} " >> ${LOGNAME} 
        #
		#	concatFilename=DOJ_AUSA_MASS_${FF_NPI_NUM}_${CLM_TYPE_LIT}_${EXT_FROM_YYYY}_${EXT_TO_YYYY}_${TMSTMP}.txt.gz
        #  
		#	echo "concatFilename=${concatFilename}" >> ${LOGNAME} 
        # 
		#	${RUNDIR}CombineS3Files.sh ${S3BUCKET} ${concatFilename} 
		#fi

		#############################################################
		# Check the status of script
		#############################################################
		#RET_STATUS=$?
        #
		#if [[ $RET_STATUS != 0 ]]; then
		#	echo "" >> ${LOGNAME}
		#	echo "Shell script CombineS3Files.sh failed." >> ${LOGNAME}
			
		#	# Send Failure email	
		#	SUBJECT="Combining S3 files in DOJ_AUSA_MASS_Ext - Failed ($ENVNAME)"
		#	MSG="Combining S3 files in DOJ_AUSA_MASS_Ext.sh has failed."
		#	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
        #
		#	exit 12
		#fi	



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

SUBJECT="DOJ AUSA MASS extract ($ENVNAME) " 
MSG="The Extract for the creation of the DOJ AUSA MASS data pull has completed.\n\nThe following file(s) were created:\n\n${S3Files}"

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${DOJ_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Error in calling sendEmail.py" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Sending Success email in DOJ_AUSA_MASS_Ext.sh  - Failed ($ENVNAME)"
	MSG="Sending Success email in DOJ_AUSA_MASS_Ext.sh  has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	


#############################################################
# Create Manifest file
#############################################################
echo "" >> ${LOGNAME}
echo "Create Manifest file for DOJ AUSA MASS Extract.  " >> ${LOGNAME}

#####################################################
# S3BUCKET --> points to location of extract file. 
#          --> S3 folder is key token to config file to determine of manifest file is in HOLD status   
# TMSTMP   --> uniquely identifies extract file(s) 
# DOJ_EMAIL_SUCCESS_RECIPIENT --> manifest file recipients
#
# Ex. CreateManifestFile.sh s3://aws-hhs-cms-eadg-bia-ddom-extracts/xtr/DOJ/ 20231211.125522 pbaranoski-con@index.com 
#####################################################
${RUNDIR}CreateManifestFile.sh ${S3BUCKET} ${TMSTMP} ${DOJ_BOX_RECIPIENT} 


#############################################################
# Check the status of script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script CreateManifestFile.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Create Manifest file in DOJ_AUSA_MASS_Ext.sh  - Failed ($ENVNAME)"
	MSG="Create Manifest file in DOJ_AUSA_MASS_Ext.sh  has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME} 
echo "Remove ${DOJ_AUSA_MASS_PARM_FILE} from data directory" >> ${LOGNAME} 

rm ${DATADIR}${DOJ_AUSA_MASS_PARM_FILE}  >> ${LOGNAME} 2>&1


#############################################################
# end script
#############################################################
echo "" >> ${LOGNAME}
echo "DOJ_AUSA_MASS_Ext.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS