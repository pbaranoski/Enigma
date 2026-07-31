#!/usr/bin/bash
############################################################################################################
# Name:  FOIA_Pistorino_EXT.sh
#
# Desc: Extract FOIA Pistorino PTD
#
# NOTE:!!!! Modify log filename to new REQ # so it will reflect that way in the Dashboard.
#
# Modified:
#
# Paul Baranoski 2025-12-23 Create script.
# Paul Baranoski 2026-01-14 Add PartA and B processing from DOJ_Pistorino.
# Paul Baranoski 2026-01-21 Update log filename from "FOIA_REQ120_Pistarino_" to "FOIA_REQ128_Pistarino_".
#                           Add calls to PTA and PTB python modules.
# Paul Baranoski 2026-03-04 Update log filename from "FOIA_REQ128_Pistarino_" to "FOIA_REQ137_Pistarino_".
# Paul Baranoski 2026-03-04 Update log filename from "FOIA_REQ137_Pistarino_" to "FOIA_REQ138_Pistarino_".
# Paul Baranoski 2026-04-06 Update log filename from "FOIA_REQ138_Pistarino_" to "FOIA_REQ150_Pistarino_".
# Paul Baranoski 2026-05-04 Update log filename from "FOIA_REQ150_Pistarino_" to "FOIA_REQ159_Pistarino_".
# Paul Baranoski 2026-06-18 Update log filename from "FOIA_REQ159_Pistarino_" to "FOIA_REQ176_Pistarino_".
# Paul Baranoski 2026-07-30 Update log filename from "FOIA_REQ176_Pistarino_" to "FOIA_REQ193_Pistarino_".
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP:=`date +%Y%m%d.%H%M%S`}
UNIQUE_FILE_TMSTMP=${TMSTMP}
LOGNAME=/app/IDRC/XTR/CMS/logs/FOIA_REQ193_Pistarino_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

FOIA_PISTORINO_PARM_FILE=FOIA_PISTORINO_PARM_FILE.txt
REQ=REQ193

touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "FOIA_Pistorino_Ext.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh

source ${RUNDIR}FilenameCounts.bash

S3BUCKET=${FOIA_BUCKET} 

echo "FOIA bucket=${S3BUCKET}" >> ${LOGNAME}


#################################################################################
# Download FOIA date Parameter file from S3 to data directory.
# NOTE: Make sure that the last record in the FOIA_PARM_FILE has an 
#       ending \n. (Press <ENTER> after each record in file). 
#################################################################################
echo "" >> ${LOGNAME}
echo "Copy FOIA parm file ${FOIA_PISTORINO_PARM_FILE} from S3 to linux" >> ${LOGNAME}


# Copy S3 file to linux
aws s3 cp s3://${CONFIG_BUCKET}${FOIA_PISTORINO_PARM_FILE} ${DATADIR}${FOIA_PISTORINO_PARM_FILE}  1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Copying S3 DOJ ${FOIA_PISTORINO_PARM_FILE} parameter file to Linux failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="FOIA_Pistorino_Ext.sh - Failed ($ENVNAME)"
	MSG="Copying S3 DOJ ${FOIA_PISTORINO_PARM_FILE} parameter file from ${CONFIG_BUCKET} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	


#################################################################################
# Find SAF ENC HHA Extract Years Parameter file in data directory.
#################################################################################
ParmFile2Process=`ls ${DATADIR}${FOIA_PISTORINO_PARM_FILE}` 1>> ${LOGNAME}  2>&1

echo "" >> ${LOGNAME}
echo "FOIA Pistorino Extract Parameter file on linux: ${ParmFile2Process}" >> ${LOGNAME}


#################################################################################
# Loop thru Date Ranges in DOJ parameter file.
# 
# CLM_TYPE_LIT,FROM-DT,TO-DT,FILE_LIT
# PTD,2020-01-01,2020-01-31,PTD2020JAN
# PTD,2020-02-01,2020-02-28,PTD2020FEB
#
# NOTE: The sed command will remove the \r that may appear in parameter file when
#       the file is uploaded from windows to S3. The \r character may 
#       prevent the file from being processed properly.
#################################################################################

sed -i 's/\r//g' ${ParmFile2Process}  2>> ${LOGNAME} 

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
			
		PART=""
		
        #############################################################
        # Get claim-type codes
        #############################################################
		case $CLM_TYPE_LIT
		in
			PTD)
				#PART=3
				CLM_TYPE_CODES="1,2,4"
				PTA_PTB_PTD_SW=D
				#SINGLE_FILE_PHRASE="SINGLE=TRUE"
				SINGLE_FILE_PHRASE=""
				;;

			HHA)
				#PART=3
				CLM_TYPE_CODES="10"
				PTA_PTB_PTD_SW=A
				SINGLE_FILE_PHRASE="SINGLE=TRUE"
				;;
		 
			HSP)
				#PART=3
				CLM_TYPE_CODES="50"
				PTA_PTB_PTD_SW=A
				SINGLE_FILE_PHRASE="SINGLE=TRUE"
				;;

			SNF)
				#PART=3
				CLM_TYPE_CODES="20,30"
				PTA_PTB_PTD_SW=A
				SINGLE_FILE_PHRASE="SINGLE=TRUE"
				;;
	
			INP)
				#PART=4
				CLM_TYPE_CODES="60"
				PTA_PTB_PTD_SW=A
				SINGLE_FILE_PHRASE="SINGLE=TRUE"
				;;
			
			OPT)
				#PART=4
				CLM_TYPE_CODES="40"
				PTA_PTB_PTD_SW=A
				SINGLE_FILE_PHRASE=""
				;;

			CAR)
				#PART=4
				CLM_TYPE_CODES="71,72"
				PTA_PTB_PTD_SW=B
				SINGLE_FILE_PHRASE=""	
				;;
			
			DME)
				#PART=4
				CLM_TYPE_CODES="81,82"
				PTA_PTB_PTD_SW=B
                SINGLE_FILE_PHRASE="SINGLE=TRUE"
				;;			
			*)
			
				echo "Invalid claim type literal ${CLM_TYPE_LIT} on parameter record." >> ${LOGNAME}
				
				# Send Failure email	
				SUBJECT="FOIA_REQ193_Pistarino_Ext  - Failed ($ENVNAME)"
				MSG="FOIA Pistorino extract has failed. \nInvalid claim type literal ${CLM_TYPE_LIT} on parameter record."
				${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
				
				exit 12
			
		esac

		echo "PART=${PART}" >> ${LOGNAME}
		
		echo "CLM_TYPE_CODES=${CLM_TYPE_CODES} " >> ${LOGNAME}
		echo "PTA_PTB_PTD_SW=${PTA_PTB_PTD_SW}"  >> ${LOGNAME}
		echo "SINGLE_FILE_PHRASE=${SINGLE_FILE_PHRASE}"  >> ${LOGNAME}
		
	
        #############################################################
		# Export environment variables for Python code
        #############################################################
		export UNIQUE_FILE_TMSTMP
        export CLM_TYPE_LIT
		export FILE_LIT
		export PART
		export REQ

		export CLM_TYPE_CODES
		export EXT_FROM_DT
		export EXT_TO_DT
		export SINGLE_FILE_PHRASE

		
        #############################################################
        # Execute Python code to extract data.
        #############################################################
		echo "" >> ${LOGNAME}

		if [ "${PTA_PTB_PTD_SW}" = "D" ];then
			echo "Start execution of FOIA_Pistorino_Ext_PTD.py program"  >> ${LOGNAME}
			${PYTHON_COMMAND} ${RUNDIR}FOIA_Pistorino_Ext_PTD.py >> ${LOGNAME} 2>&1

		elif [ "${PTA_PTB_PTD_SW}" = "A" ];then
			echo "Start execution of FOIA_Pistarino_Ext_PTA.py program"  >> ${LOGNAME}
			${PYTHON_COMMAND} ${RUNDIR}FOIA_Pistarino_Ext_PTA.py >> ${LOGNAME} 2>&1
			
		elif [ "${PTA_PTB_PTD_SW}" = "B" ];then
			echo "Start execution of FOIA_Pistarino_Ext_PTB.py program"  >> ${LOGNAME}
			${PYTHON_COMMAND} ${RUNDIR}FOIA_Pistarino_Ext_PTB.py >> ${LOGNAME} 2>&1
		fi
		
		#############################################################
		# Check the status of python script  
		#############################################################
		RET_STATUS=$?

		if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Python script FOIA_REQ193_Pistarino_Ext_[PTA/PTB/PTD].py failed" >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="FOIA_REQ193_Pistarino_Ext_[PTA/PTB/PTD].py - Failed ($ENVNAME)"
			MSG="FOIA Pistorino extract has failed. Python script FOIA_REQ193_Pistarino_Ext_[PTA/PTB/PTD].py failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
		fi

		echo "" >> ${LOGNAME}
		#if [ "PTA_PTB_PTD_SW" = "A" ];then
			echo "Python script FOIA_Pistorino_Ext_PTA/PTB/PTD.py completed successfully. " >> ${LOGNAME}
		#fi


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


		concatFilename=FOIA_${REQ}_PISTORINO_EXT_${CLM_TYPE_LIT}_${FILE_LIT}_${UNIQUE_FILE_TMSTMP}.txt.gz
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
				SUBJECT="Combining S3 files in FOIA_REQ193_Pistarino_Ext - Failed ($ENVNAME)"
				MSG="Combining S3 files in FOIA_Pistorino_Ext.sh has failed."
				${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

				exit 12
		fi	


done <  ${ParmFile2Process}


#############################################################
# Create Manifest file
#############################################################
echo "" >> ${LOGNAME}
echo "Create Manifest file for FOIA Pistorino Extract.  " >> ${LOGNAME}

#####################################################
# S3BUCKET --> points to location of extract file. 
#          --> S3 folder is key token to config file to determine of manifest file is in HOLD status   
# TMSTMP   --> uniquely identifies extract file(s) 
# ENIGMA_EMAIL_SUCCESS_RECIPIENT --> manifest file recipients
#
# Ex. CreateManifestFile.sh s3://aws-hhs-cms-eadg-bia-ddom-extracts/xtr/DOJ/ 20231211.125522 pbaranoski-con@index.com 
#####################################################
BOX_RECIPIENTS="james.pistorino@gmail.com,jagadeeshwar.pagidimarri@cms.hhs.gov,monica.algozer@cms.hhs.gov" 

${RUNDIR}CreateManifestFile.sh ${S3BUCKET} ${TMSTMP} ${BOX_RECIPIENTS} 
		
#############################################################
# Check the status of script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script CreateManifestFile.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Create Manifest file in FOIA_Pistorino_Ext.sh - Failed ($ENVNAME)"
	MSG="Create Manifest file in FOIA_Pistorino_Ext.sh has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	
	
	
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

SUBJECT="FOIA Pistorino extract ($ENVNAME) " 
MSG="The Extract for the creation of the FOIA Pistorino data pull has completed.\n\nThe following file(s) were created:\n\n${S3Files}"

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Error in calling sendEmail.py" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Sending Success email in FOIA_Pistorino_Ext.sh  - Failed ($ENVNAME)"
		MSG="Sending Success email in FOIA_Pistorino_Ext.sh  has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
fi	


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME} 
echo "Remove ${FOIA_PISTORINO_PARM_FILE} from data directory" >> ${LOGNAME} 

rm ${DATADIR}${FOIA_PISTORINO_PARM_FILE}  >> ${LOGNAME} 2>&1


#############################################################
# end script
#############################################################
echo "" >> ${LOGNAME}
echo "FOIA_Pistorino_Ext.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS