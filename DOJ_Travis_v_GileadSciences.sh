#!/usr/bin/bash
############################################################################################################
# Name:  DOJ_Travis_v_GileadSciences.sh
#
# Desc: Extract for DOJ_Travis_v_GileadSciences
#
# Author     : Paul Baranoski	
# Created    : 12/20/2023
#
# Modified:
#
# Paul Baranoski 2023-12-20 Create script.
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP:=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/DOJ_Travis_v_Gilead_Ext_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "DOJ_Travis_v_Gilead_Ext.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh

source ${RUNDIR}FilenameCounts.bash

S3BUCKET=${DOJ_BUCKET} 

echo "DOJ bucket=${S3BUCKET}" >> ${LOGNAME}

#############################################################
# function
#############################################################
function ExecuteExtract {

	FROM_DT=$1
	TO_DT=$2
	REQUESTOR=$3
	NDC_CODES=$4

	echo "NDC_CODES=${NDC_CODES}" >> ${LOGNAME}
	echo "REQUESTOR=${REQUESTOR}" >> ${LOGNAME}
	echo "FROM_DT=${FROM_DT}" >> ${LOGNAME}
	echo "TO_DT=${TO_DT}" >> ${LOGNAME}

	#############################################################
	# Export environment variables for Python code
	#
	# NOTE: Need a unique Timestamp for each extract so that we can
	#       create a single manifest file for each extract file.
	#       Apparently, BOX has concurrency issues, and possible
	#       download size limitations. 
	#############################################################
	UNIQUE_FILE_TMSTMP=`date +%Y%m%d.%H%M%S`

	export UNIQUE_FILE_TMSTMP
	export NDC_CODES
	export REQUESTOR
	export FROM_DT
	export TO_DT

	#############################################################
	# Execute python script  
	#############################################################
	echo "Start execution of DOJ_Travis_v_GileadSciences.py program"  >> ${LOGNAME}
	${PYTHON_COMMAND} ${RUNDIR}DOJ_Travis_v_GileadSciences.py >> ${LOGNAME} 2>&1


	#############################################################
	# Check the status of python script  
	#############################################################
	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Python script DOJ_Travis_v_GileadSciences.py failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="DOJ_Travis_v_GileadSciences.py - Failed ($ENVNAME)"
		MSG="Python script DOJ_Travis_v_GileadSciences.py failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi

	echo "" >> ${LOGNAME}
	echo "Python script DOJ_Travis_v_GileadSciences.py completed successfully. " >> ${LOGNAME}
	

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

	concatFilename=DOJ_TRAVIS_V_GILEAD_${REQUESTOR}_${UNIQUE_FILE_TMSTMP}.txt.gz

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
			SUBJECT="Combining S3 files in DOJ_Travis_v_Gilead_Ext.sh - Failed ($ENVNAME)"
			MSG="Combining S3 files in DOJ_Travis_v_Gilead_Ext.sh has failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
	fi	

	#############################################################
	# Create Manifest file
	#############################################################
	echo "" >> ${LOGNAME}
	echo "Create Manifest file for DOJ Travis_v_GileadSciences Extract.  " >> ${LOGNAME}

	#####################################################
	# S3BUCKET --> points to location of extract file. 
	#          --> S3 folder is key token to config file to determine of manifest file is in HOLD status   
	# TMSTMP   --> uniquely identifies extract file(s) 
	# DOJ_EMAIL_SUCCESS_RECIPIENT --> manifest file recipients
	#####################################################
	${RUNDIR}CreateManifestFile.sh ${S3BUCKET} ${UNIQUE_FILE_TMSTMP} ${DOJ_EMAIL_SUCCESS_RECIPIENT} 


	#############################################################
	# Check the status of script
	#############################################################
	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Shell script CreateManifestFile.sh failed." >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="Create Manifest file in DOJ_Travis_v_Gilead_Ext.sh  - Failed ($ENVNAME)"
			MSG="Create Manifest file in DOJ_Travis_v_Gilead_Ext.sh  has failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
	fi	

}


#############################################################
# Run Extract for Harvoni 
#############################################################
echo "" >> ${LOGNAME}
echo "Run Extract for Harvoni" >> ${LOGNAME}

P_FROM_DT=2014-10-10
P_TO_DT=2022-12-31
P_REQUESTOR=Harvoni
P_NDC_CODES="'61958180101','61958180201','61958180301','61958180401','61958180501'"

ExecuteExtract ${P_FROM_DT} ${P_TO_DT} ${P_REQUESTOR} ${P_NDC_CODES}
	
	
#############################################################
# Run Extract for Sovaldi
#############################################################
echo "" >> ${LOGNAME}
echo "Run Extract for Sovaldi" >> ${LOGNAME}

P_FROM_DT=2013-12-06
P_TO_DT=2022-12-31
P_REQUESTOR=Sovaldi
P_NDC_CODES="'61958150101','61958150301','61958150401','61958150501'"

ExecuteExtract ${P_FROM_DT} ${P_TO_DT} ${P_REQUESTOR} ${P_NDC_CODES}
		

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

SUBJECT="DOJ Travis_v_GileadSciences extract ($ENVNAME) " 
MSG="The Extract for the creation of the DOJ Travis_v_GileadSciences data pull has completed.\n\nThe following file(s) were created:\n\n${S3Files}"

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Error in calling sendEmail.py" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Sending Success email in DOJ_Travis_v_Gilead_Ext.sh  - Failed ($ENVNAME)"
		MSG="Sending Success email in DOJ_Travis_v_Gilead_Ext.sh  has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
fi	


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME} 
echo "Remove ${DOJ_Travis_v_Gilead_PARM_FILE} from data directory" >> ${LOGNAME} 

rm ${DATADIR}${DOJ_Travis_v_Gilead_PARM_FILE}  >> ${LOGNAME} 2>&1


#############################################################
# end script
#############################################################
echo "" >> ${LOGNAME}
echo "DOJ_Travis_v_Gilead_Ext.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS