#!/usr/bin/bash
############################################################################################################
# Name:  DOJ_Regeneron_Ext.sh
#
# Desc: Extract for DOJ Regeneron Extract
#
# Author     : Paul Baranoski	
# Created    : 05/21/2024
#
# Modified:
#
# Paul Baranoski 2024-05-021Create script.
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP:=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/DOJ_Regeneron_Ext_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

DOJ_REGENERON_PARM_FILE=DOJ_REGENERON_PARM_FILE.txt
PTA_PTB_SW=""
SQL_SAMPLE_PHRASE=""
SINGLE_FILE_PHRASE=""


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "DOJ_Regeneron_Ext.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh

source ${RUNDIR}FilenameCounts.bash

S3BUCKET=${DOJ_BUCKET} 

echo "DOJ bucket=${S3BUCKET}" >> ${LOGNAME}


#################################################################################
# Download DOJ date Parameter file from S3 to data directory.
# NOTE: Make sure that the last record in the DOJ_PARM_FILE has an 
#       ending \n. (Press <ENTER> after each record in file). 
#################################################################################
echo "" >> ${LOGNAME}
echo "Copy DOJ parm file ${DOJ_REGENERON_PARM_FILE} from S3 to linux" >> ${LOGNAME}


# Copy S3 file to linux
aws s3 cp s3://${CONFIG_BUCKET}${DOJ_REGENERON_PARM_FILE} ${DATADIR}${DOJ_REGENERON_PARM_FILE}  1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Copying S3 DOJ ${DOJ_REGENERON_PARM_FILE} parameter file to Linux failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="DOJ_Regeneron_Ext.sh - Failed ($ENVNAME)"
	MSG="Copying S3 DOJ ${DOJ_REGENERON_PARM_FILE} parameter file from ${CONFIG_BUCKET} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	


#################################################################################
# Find Parameter file in data directory.
#################################################################################
ParmFile2Process=`ls ${DATADIR}${DOJ_REGENERON_PARM_FILE}` 1>> ${LOGNAME}  2>&1

echo "" >> ${LOGNAME}
echo "DOJ Regeneron Extract Parameter file on linux: ${ParmFile2Process}" >> ${LOGNAME}


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

		CLM_TYPE_LIT=`echo ${PARM_REC} | cut -d, -f1`  2>> ${LOGNAME}
		EXT_FROM_DT=`echo ${PARM_REC} | cut -d, -f2`  2>> ${LOGNAME}
		EXT_TO_DT=`echo ${PARM_REC} | cut -d, -f3`    2>> ${LOGNAME}
		FILE_LIT=`echo ${PARM_REC} | cut -d, -f4`    2>> ${LOGNAME}
		
		echo "CLM_TYPE_LIT=${CLM_TYPE_LIT}" >> ${LOGNAME}
		echo "EXT_FROM_DT=${EXT_FROM_DT}" >> ${LOGNAME}
		echo "EXT_TO_DT=${EXT_TO_DT}" >> ${LOGNAME}
		echo "FILE_LIT=${FILE_LIT}" >> ${LOGNAME}
				
		OUTPUT_FILE=DOJ_REGENERON_EXT_${CLM_TYPE_LIT}_${FILE_LIT}_${TMSTMP}.txt.gz
		echo "OUTPUT_FILE=${OUTPUT_FILE}" >> ${LOGNAME}
		
		# Needed for DOJ_PartB_Standard.py
		SQL_FILTER_FILE=""
		DOJ_TITLE="DOJ_Regeneron"
		COUNT_OPT="0"
		
        #############################################################
        # Get claim-type codes
        #############################################################
		case $CLM_TYPE_LIT
		in

			PTA)
				PTA_PTB_SW=A
				SINGLE_FILE_PHRASE="SINGLE=TRUE"
				
				SQL_FILTER=" C.CLM_TYPE_CD BETWEEN 10 AND 64 
				                AND C.CLM_FINL_ACTN_IND = 'Y'
								AND CL.CLM_LINE_FROM_DT BETWEEN TO_DATE('${EXT_FROM_DT}','YYYY-MM-DD') AND TO_DATE('${EXT_TO_DT}','YYYY-MM-DD')
								AND (   CL.CLM_LINE_HCPCS_CD IN ('Q2046','J0178','C9291','J9035','J2778')
								   OR CL.CLM_LINE_NDC_CD IN ('50242006101','50242008001','61755000501','61755000502','61755000554','61755000555'))"
				;;

			PTB)
				PTA_PTB_SW=B
				SINGLE_FILE_PHRASE="SINGLE=TRUE"
				
				SQL_FILTER=" C.CLM_TYPE_CD BETWEEN 71 and 82 
				                AND C.CLM_FINL_ACTN_IND = 'Y'
								AND CL.CLM_LINE_FROM_DT BETWEEN TO_DATE('${EXT_FROM_DT}','YYYY-MM-DD') AND TO_DATE('${EXT_TO_DT}','YYYY-MM-DD')
								AND (   CL.CLM_LINE_HCPCS_CD IN ('Q2046','J0178','C9291','J9035','J2778')
								   OR CL.CLM_LINE_NDC_CD IN ('50242006101','50242008001','61755000501','61755000502','61755000554','61755000555'))"
				;;
			
			HHA)
				PTA_PTB_SW=A
				SINGLE_FILE_PHRASE="SINGLE=TRUE"
				
				SQL_FILTER="WHERE C.CLM_TYPE_CD IN ('10')
				                AND C.CLM_FINL_ACTN_IND = 'Y'
								AND CL.CLM_LINE_FROM_DT BETWEEN TO_DATE('${EXT_FROM_DT}','YYYY-MM-DD') AND TO_DATE('${EXT_TO_DT}','YYYY-MM-DD')
								AND (   CL.CLM_LINE_HCPCS_CD IN ('Q2046','J0178','C9291','J9035','J2778')
								   OR CL.CLM_LINE_NDC_CD IN ('50242006101','50242008001','61755000501','61755000502','61755000554','61755000555'))"
				;;
		 
			HSP)
				PTA_PTB_SW=A
				SINGLE_FILE_PHRASE="SINGLE=TRUE"

				SQL_FILTER="WHERE C.CLM_TYPE_CD IN ('50')
				                AND C.CLM_FINL_ACTN_IND = 'Y'
								AND CL.CLM_LINE_FROM_DT BETWEEN TO_DATE('${EXT_FROM_DT}','YYYY-MM-DD') AND TO_DATE('${EXT_TO_DT}','YYYY-MM-DD')
								AND (   CL.CLM_LINE_HCPCS_CD IN ('Q2046','J0178','C9291','J9035','J2778')
								   OR CL.CLM_LINE_NDC_CD IN ('50242006101','50242008001','61755000501','61755000502','61755000554','61755000555'))"	

				;;

			SNF)
				PTA_PTB_SW=A
				SINGLE_FILE_PHRASE="SINGLE=TRUE"

				SQL_FILTER="WHERE C.CLM_TYPE_CD IN ('20, 30')
				                AND C.CLM_FINL_ACTN_IND = 'Y'
								AND CL.CLM_LINE_FROM_DT BETWEEN TO_DATE('${EXT_FROM_DT}','YYYY-MM-DD') AND TO_DATE('${EXT_TO_DT}','YYYY-MM-DD')
								AND (   CL.CLM_LINE_HCPCS_CD IN ('Q2046','J0178','C9291','J9035','J2778')
								   OR CL.CLM_LINE_NDC_CD IN ('50242006101','50242008001','61755000501','61755000502','61755000554','61755000555'))"
				;;
	
			INP)
				PTA_PTB_SW=A
				SINGLE_FILE_PHRASE="SINGLE=TRUE"
				
				SQL_FILTER="WHERE C.CLM_TYPE_CD BETWEEN 60 AND 64 
				                AND C.CLM_FINL_ACTN_IND = 'Y'
								AND CL.CLM_LINE_FROM_DT BETWEEN TO_DATE('${EXT_FROM_DT}','YYYY-MM-DD') AND TO_DATE('${EXT_TO_DT}','YYYY-MM-DD')
								AND (   CL.CLM_LINE_HCPCS_CD IN ('Q2046','J0178','C9291','J9035','J2778')
								   OR CL.CLM_LINE_NDC_CD IN ('50242006101','50242008001','61755000501','61755000502','61755000554','61755000555'))"

				;;
			
			OPT)
				PTA_PTB_SW=A
				SINGLE_FILE_PHRASE="SINGLE=TRUE"

				SQL_FILTER="WHERE C.CLM_TYPE_CD IN ('40')
				                AND C.CLM_FINL_ACTN_IND = 'Y'
								AND CL.CLM_LINE_FROM_DT BETWEEN TO_DATE('${EXT_FROM_DT}','YYYY-MM-DD') AND TO_DATE('${EXT_TO_DT}','YYYY-MM-DD')
								AND (   CL.CLM_LINE_HCPCS_CD IN ('Q2046','J0178','C9291','J9035','J2778')
								   OR CL.CLM_LINE_NDC_CD IN ('50242006101','50242008001','61755000501','61755000502','61755000554','61755000555'))"

				;;

			CAR)
				PTA_PTB_SW=B
                #SINGLE_FILE_PHRASE=""
				SINGLE_FILE_PHRASE="SINGLE=TRUE"	

				;;
			
			DME)
				PTA_PTB_SW=B
                SINGLE_FILE_PHRASE="SINGLE=TRUE"

				;;
			
			*)
			
				echo "Invalid claim type literal ${CLM_TYPE_LIT} on parameter record." >> ${LOGNAME}
				
				# Send Failure email	
				SUBJECT="DOJ_Regeneron_Ext  - Failed ($ENVNAME)"
				MSG="DOJ Regeneron extract has failed. \nInvalid claim type literal ${CLM_TYPE_LIT} on parameter record."
				${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
				
				exit 12
			
		esac

		echo "PTA_PTB_SW=${PTA_PTB_SW}"  >> ${LOGNAME}
		echo "SINGLE_FILE_PHRASE=${SINGLE_FILE_PHRASE}"  >> ${LOGNAME}
		echo "SQL_FILTER=${SQL_FILTER}"  >> ${LOGNAME}	
		
        #############################################################
		# Export environment variables for Python code
		#
		# NOTE: Need a unique Timestamp for each extract so that we can
		#       create a single manifest file for each extract file.
		#       Apparently, BOX has concurrency issues, and possible
		#       download size limitations. 
        #############################################################
		export SINGLE_FILE_PHRASE
		export OUTPUT_FILE
		export SQL_FILTER
		export SQL_FILTER_FILE
		export DOJ_TITLE
		export COUNT_OPT		
		
        #############################################################
        # Execute Python code to extract data.
        #############################################################
		echo "" >> ${LOGNAME}

		if [ "${PTA_PTB_SW}" = "A" ];then
			echo "Start execution of DOJ_PartA_Standard.py program"  >> ${LOGNAME}
			${PYTHON_COMMAND} ${RUNDIR}DOJ_PartA_Standard.py >> ${LOGNAME} 2>&1
		else
			echo "Start execution of DOJ_PartB_Standard.py program"  >> ${LOGNAME}
			${PYTHON_COMMAND} ${RUNDIR}DOJ_PartB_Standard.py >> ${LOGNAME} 2>&1
		fi
		
		#############################################################
		# Check the status of python script  
		#############################################################
		RET_STATUS=$?

		if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Python script DOJ_Part[A/B]_Standard.py failed" >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="DOJ_Part[A/B]_Standard.py - Failed ($ENVNAME)"
			MSG="DOJ Regeneron extract has failed. Python script DOJ_Part[A/B]_Standard.py failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
		fi

		echo "" >> ${LOGNAME}
		if [ "PTA_PTB_SW" = "A" ];then
			echo "Python script DOJ_Regeneron_Ext_PTA.py completed successfully. " >> ${LOGNAME}
		else
			echo "Python script DOJ_Regeneron_Ext_PTB.py completed successfully. " >> ${LOGNAME}
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

		concatFilename=${OUTPUT_FILE}

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
				SUBJECT="Combining S3 files in DOJ_Regeneron_Ext - Failed ($ENVNAME)"
				MSG="Combining S3 files in DOJ_Regeneron_Ext.sh has failed."
				${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

				exit 12
		fi	


done <  ${ParmFile2Process}


#############################################################
# Create Manifest file
#############################################################
echo "" >> ${LOGNAME}
echo "Create Manifest file for DOJ Regeneron Extract.  " >> ${LOGNAME}

#####################################################
# S3BUCKET --> points to location of extract file. 
#          --> S3 folder is key token to config file to determine of manifest file is in HOLD status   
# TMSTMP   --> uniquely identifies extract file(s) 
# DOJ_EMAIL_SUCCESS_RECIPIENT --> manifest file recipients
#
# Ex. CreateManifestFile.sh s3://aws-hhs-cms-eadg-bia-ddom-extracts/xtr/DOJ/ 20231211.125522 pbaranoski-con@index.com 
#####################################################
echo "S3BUCKET=${S3BUCKET}"  >> ${LOGNAME}
echo "TMSTMP=${TMSTMP}" >> ${LOGNAME}
echo "DOJ_EMAIL_SUCCESS_RECIPIENT=${DOJ_EMAIL_SUCCESS_RECIPIENT}" >> ${LOGNAME}

${RUNDIR}CreateManifestFile.sh ${S3BUCKET} ${TMSTMP} ${DOJ_EMAIL_SUCCESS_RECIPIENT} 


#############################################################
# Check the status of script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Shell script CreateManifestFile.sh failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Create Manifest file in DOJ_Regeneron_Ext.sh  - Failed ($ENVNAME)"
		MSG="Create Manifest file in DOJ_Regeneron_Ext.sh  has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

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

SUBJECT="DOJ Regeneron extract ($ENVNAME) " 
MSG="The Extract for the creation of the DOJ Regeneron data pull has completed.\n\nThe following file(s) were created:\n\n${S3Files}"

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Error in calling sendEmail.py" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Sending Success email in DOJ_Regeneron_Ext.sh  - Failed ($ENVNAME)"
		MSG="Sending Success email in DOJ_Regeneron_Ext.sh  has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
fi	


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME} 
echo "Remove ${DOJ_REGENERON_PARM_FILE} from data directory" >> ${LOGNAME} 

rm ${DATADIR}${DOJ_REGENERON_PARM_FILE}  >> ${LOGNAME} 2>&1


#############################################################
# end script
#############################################################
echo "" >> ${LOGNAME}
echo "DOJ_Regeneron_Ext.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS