#!/usr/bin/bash
############################################################################################################
# Script Name: LOAD_TRICARE_FNDR_FILE.sh
# Description: This script uploads theTRICARE  Monthly finder file to BIA_{ENV}.CMS_TARGET_XTR_{ENV}.MNNUP_FF table.
#
# Author     : Paul Baranoski	
# Created    : 09/12/2023
#
# Paul Baranoski 2023-09-12 Create to download Finder Files from S3:/Finder_Files bucket and load into 
#                           Finder file table.
# Paul Baranoski 2023-12-18 Modify script to gracefully end with RC=4 when no Finder Files are found.
#                           Update emails to add ENVNAME to subject line.
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/LOAD_TRICARE_FNDR_FILE_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "LOAD_TRICARE_FNDR_FILE.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh

S3BUCKET=${TRICARE_BUCKET} 
PREFIX=TRICARE_FNDR_
COMBINED_TRICARE_FNDR_FILE=TricareCombinedFinderFile.txt
SORTED_COMBINED_TRICARE_FNDR_FILE=TricareCombinedFinderFileSorted.txt

echo "TRICARE bucket=${S3BUCKET}" >> ${LOGNAME}
echo "Finder files bucket=${FINDER_FILE_BUCKET}" >> ${LOGNAME}


#################################################################################
# Remove any residual TRICARE Finder files in data directory.
#################################################################################
echo " " >> ${LOGNAME}
echo "Remove any residual Finder Files in data directory." >> ${LOGNAME}
rm ${DATADIR}${PREFIX}*  >> ${LOGNAME}  2>&1
rm ${DATADIR}${COMBINED_TRICARE_FNDR_FILE} >> ${LOGNAME}  2>&1
rm ${DATADIR}${SORTED_COMBINED_TRICARE_FNDR_FILE} >> ${LOGNAME}  2>&1


#################################################################################
# Find TRICARE Finder Files in S3
#################################################################################
echo "" >> ${LOGNAME}
echo "Find TRICARE Finder Files in S3." >> ${LOGNAME}

# Get all filenames in S3 bucket that match filename prefix
aws s3 ls s3://${FINDER_FILE_BUCKET}${PREFIX} > ${DATADIR}tempTRICARE.txt  

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "No TRICARE finder files found in S3 folder ${FINDER_FILE_BUCKET}${PREFIX}*. " >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="LOAD_TRICARE_FNDR_FILE.sh script - No Finder files found (${ENVNAME})"
	MSG="No TRICARE finder files found in S3 folder ${FINDER_FILE_BUCKET}${PREFIX}."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${TRICARE_EMAIL_SENDER}" "${TRICARE_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 4
fi


#################################################################################
# if zero files found --> end script
#################################################################################
NOF_FILES=`wc -l ${DATADIR}tempTRICARE.txt | awk '{print $1}' `	2>> ${LOGNAME}

echo "${NOF_FILES} TRICARE Finder files found in S3." >> ${LOGNAME}

if [ ${NOF_FILES} -eq 0 ]; then
	echo "" >> ${LOGNAME}
	echo "No TRICARE Finder files found in ${FINDER_FILE_BUCKET}${PREFIX}." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="LOAD_TRICARE_FNDR_FILE.sh script - No Finder files found(${ENVNAME})"
	MSG="No TRICARE Finder Files found in ${FINDER_FILE_BUCKET}${PREFIX}."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${TRICARE_EMAIL_SENDER}" "${TRICARE_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 4
 	
fi


#################################################################################
# List S3 TRICARE Finder Files.
#################################################################################
echo "" >> ${LOGNAME}
echo "Display list of TRICARE Finder Files in S3." >> ${LOGNAME}

S3FinderFilenames=`cat ${DATADIR}tempTRICARE.txt | awk '{print $4}' ` 
echo "TRICARE S3 Finder files found: ${S3FinderFilenames}" >> ${LOGNAME}


#################################################################################
# Copy TRICARE Finder Files in S3 to linux data dir (Could be as many as 20).
#################################################################################
echo "" >> ${LOGNAME}
echo "Copy S3 TRICARE Finder Files to linux data directory" >> ${LOGNAME}

for S3FinderFilename in ${S3FinderFilenames}
do
	aws s3 cp s3://${FINDER_FILE_BUCKET}${S3FinderFilename} ${DATADIR}${S3FinderFilename}  1>> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Copying S3 TRICARE Finder file to Linux failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="LOAD_TRICARE_FNDR_FILE.sh  - Failed (${ENVNAME})"
		MSG="Copying S3 finder file ${S3FinderFilename} from ${FINDER_FILE_BUCKET} to linux failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${TRICARE_EMAIL_SENDER}" "${TRICARE_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi	

done


#################################################################################
# Verify that there are TRICARE Finder files in data directory.
#################################################################################
echo "" >> ${LOGNAME}
echo "Find TRICARE Finder files in linux data directory." >> ${LOGNAME}

NOF_FILES=`ls ${DATADIR}${PREFIX}* | wc -l ` 2>> ${LOGNAME}

echo "${NOF_FILES} TRICARE Finder files found in linux data directory" >> ${LOGNAME}

if [ ${NOF_FILES} -eq 0 ]; then
	echo "" >> ${LOGNAME}
	echo "No TRICARE Finder files found in on linux data directory." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="LOAD_TRICARE_FNDR_FILE.sh script - Failed (${ENVNAME})"
	MSG="No TRICARE Finder Files found on linux data directory."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${TRICARE_EMAIL_SENDER}" "${TRICARE_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
 	
fi


#################################################################################
# Create single combined/sorted Tricare Finder file in data directory.
#################################################################################
echo "" >> ${LOGNAME}
echo "Get list of TRICARE Finder filenames in linux data directory." >> ${LOGNAME}

Files2Process=`ls ${DATADIR}${PREFIX}*` 1>> ${LOGNAME}  2>&1
echo "TRICARE Finder files found: ${Files2Process}" >> ${LOGNAME}

echo "" >> ${LOGNAME}
echo "Concatenate TRICARE Finder files into single file ${COMBINED_TRICARE_FNDR_FILE}" >> ${LOGNAME}
cat ${Files2Process} > ${DATADIR}${COMBINED_TRICARE_FNDR_FILE} 2>> ${LOGNAME}  

echo "Remove any carriage returns from TRICARE Finder file" >> ${LOGNAME}
sed -i 's/\r//g' ${DATADIR}${COMBINED_TRICARE_FNDR_FILE} 2>> ${LOGNAME}  

echo "Sort combined Finder File and remove duplicate entries" >> ${LOGNAME}
sort ${DATADIR}${COMBINED_TRICARE_FNDR_FILE} | uniq  > ${DATADIR}${SORTED_COMBINED_TRICARE_FNDR_FILE} 2>> ${LOGNAME}  


#############################################################
# Execute Python code to load Finder File to TRICARE_FF table.
#############################################################
echo "" >> ${LOGNAME}
echo "Start execution of LOAD_TRICARE_FNDR_FILE.py program"  >> ${LOGNAME}

# Export environment variables for Python code
export DATADIR
export SORTED_COMBINED_TRICARE_FNDR_FILE

${PYTHON_COMMAND} ${RUNDIR}LOAD_TRICARE_FNDR_FILE.py >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script - Load Finder File
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Python script LOAD_TRICARE_FNDR_FILE.py failed" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="LOAD_TRICARE_FNDR_FILE.sh  - Failed (${ENVNAME})"
	MSG="Loading TRICARE finder file has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${TRICARE_EMAIL_SENDER}" "${TRICARE_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script LOAD_TRICARE_FNDR_FILE.py completed successfully. " >> ${LOGNAME}


##################################################################################
## Move TRICARE Finder Files in S3 to archive directory.
##################################################################################
#echo "" >> ${LOGNAME}
#echo "Move processed S3 TRICARE Finder Files to archive directory in S3." >> ${LOGNAME}

#for S3FinderFilename in ${S3FinderFilenames}
#do
#	aws s3 mv s3://${FINDER_FILE_BUCKET}${S3FinderFilename} s3://${FINDER_FILE_BUCKET}archive/${S3FinderFilename}   1>> ${LOGNAME} 2>&1
#
#	RET_STATUS=$?
#
#	if [[ $RET_STATUS != 0 ]]; then
#		echo "" >> ${LOGNAME}
#		echo "Moving S3 TRICARE Finder file to S3 archive folder failed." >> ${LOGNAME}
#		
#		# Send Failure email	
#		SUBJECT="LOAD_TRICARE_FNDR_FILE.sh  - Failed (${ENVNAME})"
#		MSG="Moving S3 finder file ${S3FinderFilename} from ${FINDER_FILE_BUCKET} to S3 archive folder failed."
#		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${TRICARE_EMAIL_SENDER}" "${TRICARE_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
#
#		exit 12
#	fi	
#
#done


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "Delete temporary files " >> ${LOGNAME}

rm ${DATADIR}${PREFIX}*  >> ${LOGNAME}  2>&1
rm ${DATADIR}${COMBINED_TRICARE_FNDR_FILE} >> ${LOGNAME}  2>&1
rm ${DATADIR}${SORTED_COMBINED_TRICARE_FNDR_FILE} >> ${LOGNAME}  2>&1


#############################################################
# end script
#############################################################
echo "" >> ${LOGNAME}
echo "LOAD_TRICARE_FNDR_FILE.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS