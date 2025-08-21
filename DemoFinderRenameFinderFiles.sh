#!/usr/bin/bash
############################################################################################################
# Name:  DemoFinderRenameFinderFiles.sh
#
# Desc: Rename Demo Finder Files to our standard before processing.
#
# NOTE: Finder files will be renamed  
# FROM: marx.prod2.reports.outbound.mmpenrolle.PLNH0022.20250208 
#   TO: DEMO_FINDER_PLNH0022_JAN_20250101.114800.txt
#
# Execute as ./DemoFinderRenameFinderFiles.sh $1 
#
# $1 = Month (MMM) value to use in naming finder file (e.g., JAN, FEB, MAR, APR)   
#
# Author     : Paul Baranoski	
# Created    : 02/10/2025
#
# Modified:
#
# Paul Baranoski 2025-02-10 Created script.
############################################################################################################

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP:=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/DemoFinderRenameFinderFiles_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/


#############################################################
# Verify that required NOF parameters have been sent 
#############################################################
if ! [[ $# -eq 1  ]]
then
	echo "Incorrect # of parameters sent to script. NOF parameters: $#" >> ${LOGNAME}
	exit 12
fi

#############################################################
# Get override extract dates if passed 
#############################################################
MON=$1

echo "Parameters to script: " >> ${LOGNAME}
echo "   MON=${MON} " >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh

S3BUCKET=${bucket}

FF_DEMO_FOLDER=Finder_Files_Demo_Fndr/
FF_FOLDER=Finder_Files/
FF_PREFIX=marx.

echo "" >> ${LOGNAME}
echo "S3BUCKET=${S3BUCKET} " >> ${LOGNAME}
echo "FF_FOLDER=${FF_FOLDER} " >> ${LOGNAME}
echo "FF_DEMO_FOLDER=${FF_DEMO_FOLDER} " >> ${LOGNAME}
echo "FF_PREFIX=${FF_PREFIX} " >> ${LOGNAME}


#################################################################################
# Are there finder files to rename?
#################################################################################
NOF_FILES=`aws s3 ls s3://${S3BUCKET}${FF_DEMO_FOLDER}${FF_PREFIX} --summarize | grep 'Total Objects' | cut -d: -f2 | sed 's/ //g' ` 2>> ${LOGNAME}

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Counting NOF S3 finder files in s3://${S3BUCKET}${FF_DEMO_FOLDER}${FF_PREFIX} failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="DemoFinderRenameFinderFiles.sh - Failed ($ENVNAME)"
	MSG="Counting NOF S3 extract files in s3://${S3BUCKET}${FF_DEMO_FOLDER}${FF_PREFIX} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	

echo "NOF_FILES=${NOF_FILES}"  >> ${LOGNAME}


#################################################################################
# No finder files --> end gracefully
#################################################################################		
if [ ${NOF_FILES} -eq 0 ];then
	echo "" >> ${LOGNAME}
	echo "No S3 finder files in s3://${S3BUCKET}${FF_DEMO_FOLDER}${FF_PREFIX} . Nothing to do." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="DemoFinderRenameFinderFiles.sh nothing to do ($ENVNAME)"
	MSG="No S3 finder files in s3://${S3BUCKET}${FF_DEMO_FOLDER}${FF_PREFIX} . Nothing to do."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 0
fi


#################################################################################
# Get list of Finder Files to rename
#################################################################################
echo ""  >> ${LOGNAME}
echo "Get list of Finder Files to rename"  >> ${LOGNAME}

FF2Process=`aws s3 ls s3://${S3BUCKET}${FF_DEMO_FOLDER}${FF_PREFIX} | awk '{print $4}' | egrep "^${FF_PREFIX}"`

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Getting list of S3 finder files in s3://${S3BUCKET}${FF_DEMO_FOLDER}${FF_PREFIX} failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="DemoFinderRenameFinderFiles.sh - Failed ($ENVNAME)"
	MSG="Getting list of S3 finder files in s3://${S3BUCKET}${FF_DEMO_FOLDER}${FF_PREFIX} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	

echo ""  >> ${LOGNAME}
echo "FF2Process=${FF2Process}"  >> ${LOGNAME}


#################################################################################
# Rename Finder Files
#################################################################################
echo ""  >> ${LOGNAME}
echo "Begin renaming of Finder Files"  >> ${LOGNAME}

for FF in ${FF2Process}
do

	echo "FF=${FF}" >> ${LOGNAME}

	################################################################
	# Extract Plan and timestamp from FF filename
	#  Ex.1 FF=marx.prod2.reports.outbound.mmpenrolle.PLNH0022.20250208
	#  Ex 2 FF=marx.prod2.reports.outbound.mmpenrolle.PLNH0137.20250208	
	################################################################
	PLN=`echo "${FF}" | cut -d. -f6 `  2>> ${LOGNAME}
	
	FF_TMSTMP=`echo "${FF}" | cut -d. -f7 `  2>> ${LOGNAME}
	echo "PLN=${PLN}" >> ${LOGNAME}
	echo "FF_TMSTMP=${FF_TMSTMP}" >> ${LOGNAME}	
	
	################################################################
	# Rename FF to our standard FF name and move to our S3://Finder_Files folder. 
	# Ex. 1 DEMO.FINDER.PLNH0022.JAN.20250101.114800.txt
	#  echo "aws s3 mv s3://${S3BUCKET}${FF_DEMO_FOLDER}${FF} s3://${BUCKET}Finder_Files/DEMO_FINDER_PLN${PLN}_${MON}_${PLN}_${FF_TMSTMP}.txt"
	################################################################
	aws s3 mv s3://${S3BUCKET}${FF_DEMO_FOLDER}${FF} s3://${S3BUCKET}Finder_Files/DEMO_FINDER_${PLN}_${MON}_${TMSTMP}.txt  >> ${LOGNAME} 2>&1 

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Renaming S3 finder file s3://${BUCKET}${FF_DEMO_FOLDER}marx.prod2.reports.outbound.mmpenrolle.${PLN}.${FF_TMSTMP} failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="DemoFinderRenameFinderFiles.sh - Failed ($ENVNAME)"
		MSG="Renaming S3 finder file s3://${BUCKET}${FF_DEMO_FOLDER}marx.prod2.reports.outbound.mmpenrolle.${PLN}.${FF_TMSTMP} failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi	
	
done

#############################################################
# script clean-up
#############################################################
echo " " >> ${LOGNAME}


#############################################################
# End script
#############################################################
echo "" >> ${LOGNAME}
echo "DemoFinderRenameFinderFiles.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

exit $RET_STATUS
		

