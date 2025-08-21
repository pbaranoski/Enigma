#!/usr/bin/bash
#
######################################################################################
# Name:  DemoFinderFileExtracts.sh
#
# Desc: Create PartA, PartB, PartD extracts for a specific plan Finder file.
#
#       Finder Files: DEMO.FINDER.PLNXXXXX.*.txt  -> Modified Finder file without Header and Trailer 
#
# Created: Paul Baranoski  09/01/2022
# Modified: 
#
# Paul Baranoski 2022-11-09 Added call to CreateManifestFile.sh
# Paul Baranoski 2022-11-09 Added code to send Success emails with filenames from script
#                           instead of python code.  
# Paul Baranoski 2023-04-19 Modify code that derives PLAN_NUM to remove literal "PLN"
#                           to allow proper conversion from SF filename to EFT filename.
# Paul Baranoski 2023-04-24 Modify logic to get extract files with record counts for email.
# Paul Baranoski 2023-05-16 Add logic to calculate Extract date range values and passing 
#                           as parameters to python extract code.
#                           Also, add ability to look for override configuration file.
#                           Add call to DemoFinderFilePrep.sh. 
# Paul Baranoski 2023-05-17 Modify override date range code to check if no dates are present.
# Paul Baranoski 2023-11-16 Add Environment from SET_XTR_ENV.sh to success/failure emails.
# Paul Baranoski 2023-11-30 Add log formatting (extra blank lines) to aid in log parsing for Dashboard.
# Joshua Turner  2024-01-03 Removed code for date overrides from S3 parm file. Overrides
#                           will come directly from the Rundeck job
# Paul Baranoski 2024-08-09 Remove references to EXT_DT_CONFIG_FILE.
# Paul Baranoski 2024-08-20 Add request/finder filenames to success email. 
######################################################################################
set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`

# Export TMSTMP variable for child scripts
export TMSTMP 
LOGNAME=/app/IDRC/XTR/CMS/logs/DemoFinderFileExtracts_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "DemoFinderFileExtracts.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh >> ${LOGNAME}

LOGDIR=${LOG_PATH}/

S3BUCKET=${DEMO_FNDR_BUCKET} 
PREFIX=DEMOFNDR


echo "Demo Finder bucket=${S3BUCKET}" >> ${LOGNAME}
echo "Finder file bucket=${FINDER_FILE_BUCKET}" >> ${LOGNAME}

source ${RUNDIR}FilenameCounts.bash


#################################################################################
# Download Finder files and remove header and trailer.
#################################################################################
echo " " >> ${LOGNAME}
echo "Run script DemoFinderFilePrep.sh  " >> ${LOGNAME}
${RUNDIR}DemoFinderFilePrep.sh  >> ${LOGNAME} 2>&1

#############################################################
# Check the status of script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script DemoFinderFileExtracts.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Shell script DemoFinderFileExtracts.sh - Failed ($ENVNAME)"
	MSG="Shell script DemoFinderFilePrep.sh failed"
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DEMO_FINDER_EMAIL_SENDER}" "${DEMO_FINDER_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


#################################################################################
# Calculate Extract dates
# If date override parameters are sent from Rundeck, use those.
# Else
# 	EXT_FROM_DT:	First day of current month, 1 year ago
#   EXT_TO_DT:		Last day of prior month
#################################################################################
if [ -z $1 ]; then
	EXT_FROM_DT=`date -d "-12 month" +%Y-%m-01`   2>> ${LOGNAME}
	EXT_TO_DT=`date -d "$(date +%Y-%m-01) -1 day" +%Y-%m-%d` 2>> ${LOGNAME}
else
	if [ -z $2 ]; then
		# Send failure email for not supplying both arguments
		SUBJECT="Shell script DemoFinderFileExtracts.sh - Failed ($ENVNAME)"
		MSG="Shell script DemoFinderFilePrep.sh failed. A date override was detected but only one date parameter was supplied."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DEMO_FINDER_EMAIL_SENDER}" "${DEMO_FINDER_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
		exit 12
	else
		echo "Using override dates from Rundeck job."  >> ${LOGNAME}
		EXT_FROM_DT=$1
		EXT_TO_DT=$2
	fi
fi


############################################
# Extract current and prior month based on
# Extract parameters date values 
############################################
CUR_YR=`echo ${EXT_TO_DT} | cut -c1-4 `
PRIOR_MN=`echo ${EXT_TO_DT} | cut -c6-7`

echo "" >> ${LOGNAME}
echo "EXT_FROM_DT: ${EXT_FROM_DT}" >> ${LOGNAME}
echo "EXT_TO_DT: ${EXT_TO_DT}" >> ${LOGNAME}

echo "CUR_YR=${CUR_YR}" >> ${LOGNAME}
echo "PRIOR_MN=${PRIOR_MN}" >> ${LOGNAME}

# Export variables to 
export EXT_FROM_DT
export EXT_TO_DT
export CUR_YR
export CUR_MN  
export PRIOR_MN


#################################################################################
# Get a list of Demo Finder files in data directory.
#################################################################################
Files2Process=`ls ${DATADIR}DEMO.FINDER.PLN*` 1>> ${LOGNAME}  2>&1

echo "" >> ${LOGNAME}
echo "Demo Finder files found: ${Files2Process}" >> ${LOGNAME}


#################################################################################
# Loop thru Demo Finder files in data directory.
#################################################################################
echo "" >> ${LOGNAME}

for pathAndFilename in ${Files2Process}
do
	
	# Extract filename from full PathAndFilename
	filename=`basename ${pathAndFilename}`

	echo "" >> ${LOGNAME}
	echo "-----------------------------------" >> ${LOGNAME}
	echo "Starting Extracts for DEMO Finder File ${filename} started at `date` " >> ${LOGNAME}

	# Get Plan Number from filename --> Node 3 will have Plan Number  (DEMO.FINDER.PLNXXXXX.*.txt) 
    # Extract plan number without "PLN" literal	
	PLAN_NUM=`echo ${filename} | cut -d. -f3 | cut -c4-`
	echo "Finder file Plan Number: ${PLAN_NUM}" >> ${LOGNAME}
	
	DEMO_FINDERFILE=${filename}
	echo "Finder file: ${DEMO_FINDERFILE}" >> ${LOGNAME} 
	
	# Export Plan Number and Finder Filename for Python code
	export PLAN_NUM
	export DEMO_FINDERFILE


	##############################################
	# Load finder file into DEMO_HICN_PLAN table 
	# used in PartA, PartB, and PartD extracts.
	##############################################
	echo " " >> ${LOGNAME}
	echo "Load Demo Finder table for Plan Number ${PLAN_NUM} " >> ${LOGNAME}
	${RUNDIR}LOAD_DEMOFNDR_FNDR_FILE.sh  >> ${LOGNAME} 2>&1

	#############################################################
	# Check the status of script
	#############################################################
	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Shell script LOAD_DEMOFNDR_FNDR_FILE.sh failed." >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="Load Demo Finder table DEMO_HICN_PLAN - Failed ($ENVNAME)"
			MSG="Load Demo Finder table DEMO_HICN_PLAN has failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DEMO_FINDER_EMAIL_SENDER}" "${DEMO_FINDER_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
	fi

	
	##############################################
	# Execute PartA Extract script for Finder File
	##############################################
	echo " " >> ${LOGNAME}
	echo "Extract PartA data for Plan Number ${PLAN_NUM} " >> ${LOGNAME}
	${RUNDIR}DEMOFNDR_PTA.sh  >> ${LOGNAME} 2>&1


	#############################################################
	# Check the status of extract script
	#############################################################
	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Shell script DEMOFNDR_PTA.sh failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Demo Finder PartA Extract - Failed ($ENVNAME)"
		MSG="Demo Finder PartA Extract has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DEMO_FINDER_EMAIL_SENDER}" "${DEMO_FINDER_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi
	
	
	##############################################
	# Execute PartB Extract script for Finder File
	##############################################
	echo " " >> ${LOGNAME}
	echo "Extract PartB data for Plan Number ${PLAN_NUM} " >> ${LOGNAME}
	${RUNDIR}DEMOFNDR_PTB.sh  >> ${LOGNAME} 2>&1


	#############################################################
	# Check the status of script
	#############################################################
	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Shell script DEMOFNDR_PTB.sh failed." >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="Demo Finder PartB Extract - Failed ($ENVNAME)"
			MSG="Demo Finder PartB Extract has failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DEMO_FINDER_EMAIL_SENDER}" "${DEMO_FINDER_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
     
			exit 12
	fi

	##############################################
	# Execute PartD Extract script for Finder File
	##############################################
	echo " " >> ${LOGNAME}
	echo "Extract PartD data for Plan Number ${PLAN_NUM} " >> ${LOGNAME}
	${RUNDIR}DEMOFNDR_PTD.sh  >> ${LOGNAME} 2>&1


	#############################################################
	# Check the status of script
	#############################################################
	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Shell script DEMOFNDR_PTD.sh failed." >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="Demo Finder PartD Extract - Failed ($ENVNAME)"
			MSG="Demo Finder PartD Extract has failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DEMO_FINDER_EMAIL_SENDER}" "${DEMO_FINDER_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
    
			exit 12
	fi	

	
	#############################################################
	# Move Finder File in S3 to archive folder
	#############################################################
	# Derive S3 filename from linux filename --> DEMO.FINDER.PLNXXXXX.*.txt to DEMO_FINDER_PLNXXXXX_*.txt 
	S3Filename=`echo ${filename} | sed 's/\./_/g' | sed 's/_txt/\.txt/g'` 
	echo " " >> ${LOGNAME}
	echo "Moving S3 Demo Finder file ${S3Filename} to S3 archive folder." >> ${LOGNAME}
	
	aws s3 mv s3://${FINDER_FILE_BUCKET}${S3Filename} s3://${FINDER_FILE_BUCKET}archive/${S3Filename}  1>> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Moving S3 Demo Finder file to S3 archive folder failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Demo Finder File Extract - Failed ($ENVNAME)"
		MSG="Moving S3 Finder file to S3 archive folder failed.  ( ${FINDER_FILE_BUCKET}${S3Filename} to ${FINDER_FILE_BUCKET}archive/${S3Filename} )"
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DEMO_FINDER_EMAIL_SENDER}" "${DEMO_FINDER_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi	

	
	#############################################################
	# Delete Finder File in Linux
	#############################################################
	echo "" >> ${LOGNAME}
	echo "Delete finder file ${pathAndFilename} from linux data directory." >> ${LOGNAME}
	rm ${pathAndFilename} 2>> ${LOGNAME}
	
done


#############################################################
# Get list of S3 files for success email.
#############################################################
echo "" >> ${LOGNAME}
echo "Get S3 Extract file list and record counts" >> ${LOGNAME}

getExtractFilenamesAndCounts ${LOGDIR}DEMOFNDR_PTA_Extract_${TMSTMP}.log >> ${LOGNAME}  2>&1
S3PTAFiles="${filenamesAndCounts}" 
echo "" >> ${LOGNAME}

getExtractFilenamesAndCounts ${LOGDIR}DEMOFNDR_PTB_Extract_${TMSTMP}.log >> ${LOGNAME}  2>&1
S3PTBFiles="${filenamesAndCounts}" 
echo "" >> ${LOGNAME}

getExtractFilenamesAndCounts ${LOGDIR}DEMOFNDR_PTD_Extract_${TMSTMP}.log >> ${LOGNAME}  2>&1
S3PTDFiles="${filenamesAndCounts}"
echo "" >> ${LOGNAME}

S3Files=`echo -e "${S3PTAFiles}\n${S3PTBFiles}\n${S3PTDFiles}" ` >> ${LOGNAME}  2>&1

# Files2Process includes linux path. To remove path from filename, use basename, and xargs to pass one argument at a time to basename
FinderFiles4Email=`echo ${Files2Process} | xargs -n1 basename`  >> ${LOGNAME}  2>&1

#############################################################
# Send success email of Demo Finder Extract files
#############################################################
echo "" >> ${LOGNAME}
echo "Send success email." >> ${LOGNAME}

# Send Success email	
SUBJECT="Demo Finder Monthly File Extract - completed ($ENVNAME)"
MSG="Demo Finder Monthly File Extract completed.  

Data extracted from ${EXT_FROM_DT} to ${EXT_TO_DT} 

Request files processed: 
${FinderFiles4Email}

The following files were created: 
${S3Files}"
${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DEMO_FINDER_EMAIL_SENDER}" "${DEMO_FINDER_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
  

#############################################################
# EFT Extract files
#############################################################
echo " " >> ${LOGNAME}
echo "EFT DEMO Finder Extract Files " >> ${LOGNAME}
${RUNDIR}ProcessFiles2EFT.sh ${S3BUCKET}  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of extract script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script ProcessFiles2EFT.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Demo Finder File Extract - Failed ($ENVNAME)"
	MSG="EFT Demo Finder Extract Files process has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DEMO_FINDER_EMAIL_SENDER}" "${DEMO_FINDER_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi

 
#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "DemoFinderFileExtracts.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS
