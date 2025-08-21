#!/usr/bin/bash
############################################################################################################
# Script Name: FinderFilesReady4Processing.sh
#
# Description: This script can be run stand-alone and will report on what Finder Files are awaiting processing.
#
# Execute as ./FinderFilesReady4Processing.sh 
#
#
# Paul Baranoski 2024-01-16 Created script.
#
# Paul Baranoski 2024-01-16 Created script.
# Paul Baranoski 2024-08-30 Modify awk command to get finder file name to get full filename when it contains spaces.
# Paul Baranoski 2024-12-17 Update comments sed command to get S3_FINDER_FILE_FOLDERS_MASK.
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/FinderFilesReady4Processing_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

ACTIVE_FF_FILE=tmpActiveFinderFiles.txt
ACTIVE_FF_RPT_FILE=tmpFinderFileReport.txt	


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "FinderFilesReady4Processing.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}


#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh 


# Ensure that temp files do not exist before processing
rm ${DATADIR}${ACTIVE_FF_FILE}      >> ${LOGNAME} 2>&1
rm ${DATADIR}${ACTIVE_FF_RPT_FILE}	>> ${LOGNAME} 2>&1


#############################################################
# Get list of Finder_Files folders in S3 
#############################################################
echo "" >> ${LOGNAME}

# Remove ending slash from BUCKET name to create bucket/folder MASK
# 	NOTE: This MASK can then be used to find all folders that are like the MASK 
#         "xtr/DEV/Finder_Files/"  -->  "xtr/DEV/Finder_Files"
S3_FINDER_FILE_FOLDERS_MASK=`echo ${FINDER_FILE_BUCKET} | sed 's_/$__g' ` 
echo "S3_FINDER_FILE_FOLDERS_MASK=${S3_FINDER_FILE_FOLDERS_MASK}" >> ${LOGNAME}

# Remove "PRE" verbiage from returned results; remove all spaces
S3_FINDER_FILE_FOLDERS=`aws s3 ls s3://${S3_FINDER_FILE_FOLDERS_MASK} | sed 's/PRE//g' | sed s'/[ \t]*//g' `
echo "S3_FINDER_FILE_FOLDERS=${S3_FINDER_FILE_FOLDERS}" >> ${LOGNAME}


#############################################################
# Process each S3 Finder_File folder 
#############################################################
#  Loop thru Finder File folders
# 	1) Get counts of finder files in folder
# 	2) Add list of finder files in folder to temp file 
#############################################################
for S3_FF_FOLDER_2_PROCESS in ${S3_FINDER_FILE_FOLDERS}
do

	echo "" >> ${LOGNAME}
	echo "**********************************" >> ${LOGNAME}
	echo "S3_FF_FOLDER_2_PROCESS=${S3_FF_FOLDER_2_PROCESS}" >> ${LOGNAME}

	FF_BUCKET_FOLDER_2_PROCESS=${bucket}${S3_FF_FOLDER_2_PROCESS}
	echo "FF_BUCKET_FOLDER_2_PROCESS=${FF_BUCKET_FOLDER_2_PROCESS}" >> ${LOGNAME}

	#################################################################################
	# Are there active Finder files available for processing.
	#################################################################################
	echo "" >> ${LOGNAME}
	echo "Get count of active Finder Files " >> ${LOGNAME}

	# Ex. Total Objects: 14 --> " 14" --> "14"
	NOF_FILES=`aws s3 ls s3://${FF_BUCKET_FOLDER_2_PROCESS} --summarize | grep 'Total Objects' | cut -d: -f2 | sed 's/ //g' ` 2>> ${LOGNAME}

	RET_STATUS=$?

	if [[ $RET_STATUS = 0 ]]; then

		# We have files to report on
		if [[ ${NOF_FILES} -gt 0 ]]; then

			#################################################################################
			# Get list of Finder files available for processing.
			#################################################################################
			echo "" >> ${LOGNAME}
			echo "Get list of active finder files from S3://${FINDER_FILE_BUCKET} and append to temp file"  >> ${LOGNAME}

			# Remove 'PRE' from list of files; append FF folder name to each record in file list
			aws s3 ls s3://${FF_BUCKET_FOLDER_2_PROCESS}  --human-readable | grep -v 'PRE' | sed "s~^~${S3_FF_FOLDER_2_PROCESS} ~g"  >> ${DATADIR}${ACTIVE_FF_FILE} 

			if [[ $RET_STATUS != 0 ]]; then
				echo "" >> ${LOGNAME}
				echo "FinderFilesReady4Processing.sh failed." >> ${LOGNAME}
				
				# Send Failure email	
				SUBJECT="FinderFilesReady4Processing.sh - Failed (${ENVNAME})"
				MSG="Getting list of finder files from ${FF_BUCKET_FOLDER_2_PROCESS} in S3  has failed."
				${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

				exit 12
			fi
			
			echo "" >> ${LOGNAME}			
			cat ${DATADIR}${ACTIVE_FF_FILE} >> ${LOGNAME}
			
		fi			

	else

		echo "" >> ${LOGNAME}
		echo "FinderFilesReady4Processing.sh failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="FinderFilesReady4Processing.sh - Failed (${ENVNAME})"
		MSG="Getting count of finder files from ${FF_BUCKET_FOLDER_2_PROCESS} from S3 has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12

	fi

done


#################################################################################
# Generate Report for email
#################################################################################
echo "" >> ${LOGNAME}
echo "**********************************" >> ${LOGNAME}
echo "Generate Report" >> ${LOGNAME}

#################################################################################
# Write out HTML header.
#################################################################################
echo "<html><body><table cellspacing='1px' border='1' > " >> ${DATADIR}${ACTIVE_FF_RPT_FILE}	
echo "<tr bgcolor='#00B0F0'><th>Finder File folder</th><th>Finder File name</th><th>File Size</th><th>When added to S3</th></tr>" >> ${DATADIR}${ACTIVE_FF_RPT_FILE}	


################################################################################
# Process list of Finder files available for processing.
#################################################################################
echo "" >> ${LOGNAME}
echo "Display list of active Finder Files found in S3 Finder File folders" >> ${LOGNAME}

echo "" >> ${LOGNAME}

# send output to log file
cat ${DATADIR}${ACTIVE_FF_FILE} >> ${LOGNAME}

# process Finder file
while read FF_Rec
do

	echo "" >> ${LOGNAME}	
	echo "FF_Rec=${FF_Rec}"  >> ${LOGNAME}

	FF_Folder=`echo ${FF_Rec} | awk '{print $1}' `  2>> ${LOGNAME}
	FF_Name=`echo ${FF_Rec} | awk '{$1=$2=$3=$4=$5=""; print $0}' | sed 's/^ *//g' `  2>> ${LOGNAME}
	FF_Size=`echo ${FF_Rec} | awk '{print $4,$5}' `  2>> ${LOGNAME}
	FF_TMSTMP=`echo ${FF_Rec} | awk '{print $2,$3}' `  2>> ${LOGNAME}

	echo "FF_Folder=${FF_Folder}" >> ${LOGNAME}
	echo "FF_Name=${FF_Name}" >> ${LOGNAME}
	echo "FF_Size=${FF_Size}" >> ${LOGNAME}
	echo "FF_TMSTMP=${FF_TMSTMP}" >> ${LOGNAME}	

	# if FF_NAME is blank --> skip record 
	if [ "${FF_Name}" = "" ];then
		continue
	fi
	
	echo "<tr><td>${FF_Folder}</td><td>${FF_Name}</td><td>${FF_Size}</td><td>${FF_TMSTMP}</td></tr> " >> ${DATADIR}${ACTIVE_FF_RPT_FILE}	
	
done < ${DATADIR}${ACTIVE_FF_FILE}


#################################################################################
# Write out HTML trailer.
#################################################################################
echo "</table></body></html>" >> ${DATADIR}${MANIFEST_TMP_DIR}${ACTIVE_FF_RPT_FILE}	


#############################################################
# Send success email
#############################################################
RPT_INFO=`cat ${DATADIR}${ACTIVE_FF_RPT_FILE} `

echo "" >> ${LOGNAME}
echo "Send success email" >> ${LOGNAME}
echo "RPT_INFO=${RPT_INFO} "   >> ${LOGNAME}

SUBJECT="Active Finder Files ready for processing Report (${ENVNAME})"
MSG="Active Finder Files ready for processing . . .<br><br>${RPT_INFO}"
${PYTHON_COMMAND} ${RUNDIR}sendEmailHTML.py "${DOJ_EMAIL_SENDER}" "${DOJ_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME} 
echo "Remove temporary files from data directory" >> ${LOGNAME} 

rm ${DATADIR}${ACTIVE_FF_FILE}      >> ${LOGNAME} 2>&1
rm ${DATADIR}${ACTIVE_FF_RPT_FILE}	>> ${LOGNAME} 2>&1


#############################################################
# end script
#############################################################
echo "" >> ${LOGNAME}
echo "FinderFilesReady4Processing.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS