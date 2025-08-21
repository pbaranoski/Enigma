#!/usr/bin/sh
#
######################################################################################
# Name:  DemoFinderFilePrep.sh
#
# Desc: Retrieve Demo Finder Files from S3 to linux. Unzip finder files, remove header and trailer records (with awk script) 
#       from files (outputing new file, and remove original file. 
#       DEMO_FINDER_PLNXXXXX  -> Finder file with Header and Trailer 
#       DEMO.FINDER.PLNXXXXX  -> Modified Finder file without Header and Trailer 
#
# Created: Paul Baranoski  09/01/2022
# Modified: 
#
# Paul Baranoski 2023-04-04 Modify S3 location of Finder Files to be in Finder_Files folder instead of s3 application folder.
# Paul Baranoski 2023-05-16 Exit with RC 12 if no finder files processed.
#
# Could use this instead:
#sed -i '/TRLH/d' test.txt
#sed -i '/HDRH/d' test.txt
#
######################################################################################
set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
LOGNAME=/app/IDRC/XTR/CMS/logs/DemoFinderFilePrep_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "DemoFinderFilePrep.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh >> ${LOGNAME}


S3BUCKET=${DEMO_FNDR_BUCKET} 
PREFIX=DEMO_FINDER

echo "Demo Finder bucket=${S3BUCKET}" >> ${LOGNAME}
echo "Finder files bucket=${FINDER_FILE_BUCKET}" >> ${LOGNAME}


#################################################################################
# Remove any residual Demo Finder files.
#################################################################################
echo " " >> ${LOGNAME}
echo "Remove any residual Finder Files." >> ${LOGNAME}
rm ${DATADIR}DEMO.FINDER.PLN*  >> ${LOGNAME}  2>&1
rm ${DATADIR}DEMO_FINDER_PLN*  >> ${LOGNAME}  2>&1


#################################################################################
# Copy Finder Files from S3 to linux
# Assumption: The Demo Finder Files in S3 will have the same filename "prefix" 
#             Ex. DEMO.FINDER.PLNXXXXX 
#################################################################################
echo "" >> ${LOGNAME}
echo "Get list of S3 Demo Finder Files." >> ${LOGNAME}


# Get all filenames in S3 bucket that match filename prefix
aws s3 ls s3://${FINDER_FILE_BUCKET}${PREFIX}  > ${DATADIR}temp.txt  

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Listing S3 files from ${FINDER_FILE_BUCKET}${PREFIX} failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Demo Finder File Prep script - Failed"
	MSG="Listing Finder Files in S3 from ${FINDER_FILE_BUCKET}${PREFIX} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DEMO_FINDER_EMAIL_SENDER}" "${DEMO_FINDER_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi



# if zero files found --> end script
NOF_FILES=`wc -l ${DATADIR}temp.txt | awk '{print $1}' `	2>> ${LOGNAME}

echo "" >> ${LOGNAME}
echo "${NOF_FILES} Demo Finder files found in S3." >> ${LOGNAME}

if [ ${NOF_FILES} -eq 0 ]; then
	echo "" >> ${LOGNAME}
	echo "No Finder files found in ${FINDER_FILE_BUCKET}${PREFIX}." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Demo Finder File Prep script "
	MSG="No Finder Files found in ${FINDER_FILE_BUCKET}${PREFIX}."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DEMO_FINDER_EMAIL_SENDER}" "${DEMO_FINDER_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


# Extract the just the filenames from the S3 filename information
matches=`awk '{print $4}' ${DATADIR}temp.txt` 

echo "" >> ${LOGNAME}
echo "Demo Finder files found: ${matches}" >> ${LOGNAME}


#################################################################################
# iterate thru finder files. 
# S3 cp --include option does not properly filter results when copying contents 
#         from a folder
#################################################################################
echo "" >> ${LOGNAME}
for filename in ${matches}
do

	# Copy S3 file to linux
	aws s3 cp s3://${FINDER_FILE_BUCKET}${filename} ${DATADIR}${filename}  1>> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Copying S3 Demo Finder files to Linux failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Demo Finder File Prep - Failed"
		MSG="Copying S3 files from ${FINDER_FILE_BUCKET} failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DEMO_FINDER_EMAIL_SENDER}" "${DEMO_FINDER_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi	
	
	# uncompress file
	gzip -d ${DATADIR}filename  2>>  ${LOGNAME}

	
done


#################################################################################
# Remove header and trailer records from Finder Files.
#################################################################################
echo " " >> ${LOGNAME}
echo "Remove header and trailer records from Finder Files." >> ${LOGNAME}
echo "Started --> `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}

for filename in ${matches}
do

	# Replace '.' in filenames with '_'. Must "escape" the '.' in sed to get command to work properly.
	outputFilename=`echo ${filename} | sed 's/_/\./g'` 
	echo "outputFilename: ${outputFilename} " >> ${LOGNAME}
	
	# Remove header and trailer records from Finder Files
	${RUNDIR}DemoRemoveHdrTrlRecs.awk -v outfile="${DATADIR}${outputFilename}" ${DATADIR}${filename}  >> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "awk script removeHdrTrlRecs.awk failed." >> ${LOGNAME}
			echo "Removing header and trailer records from Finder files failed." >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="Demo Finder File Prep - Failed"
			MSG="The Demo Finder File Prep awk script has failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DEMO_FINDER_EMAIL_SENDER}" "${DEMO_FINDER_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
	fi

done

#################################################################################
# Delete Finder Files with Headers and Trailers
#################################################################################
echo " " >> ${LOGNAME}
echo "Delete Finder Files with Headers and Trailers." >> ${LOGNAME}
rm ${DATADIR}${PREFIX}_PLN* 2>> ${LOGNAME}

#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "DemoFinderFilePrep.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS
