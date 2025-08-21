#!/usr/bin/bash

######################################################################################
# Name:  PSPS_Split_files.bash
#
# Desc: Split PSPS file into multiple files by HCPCS code (IDR#PBA4/IDR#PBA6). 
#       Q4 or Q6 file must exist on Linux as .txt file. 
#
# Created: Paul Baranoski  07/13/2022
# Modified:
#
# Paul Baranoski 2022-11-10 Added call to CreateManifestFile.sh
# Paul Baranoski 2022-11-10 Added code to send Success emails with all filenames from script.
# Paul Baranoski 2023-09-21 1) Removed manifest file logic, and replaced with EFT logic.
#                           2) Change logic to find Q4/Q6 files in S3, and download most recent
#                              file for processing (instead of processing file left on server
#                              by PSPS_Extract.sh). 
# Paul Baranoski 2024-01-25 Modify logic for building EMAIL_MF_FILENAME to be able to 
#                           substitute the proper Qtr value.  
# Paul Baranoski 2024-01-25 Add $ENVNAME to SUBJECT line of emails.
# Paul Baranoski 2024-11-06 Modified successful end of script messages to that the DashboardInfo_MS.sh will correctly
#                           identify that script ended successfully and get end time of script. 
#                           Modified a few lines to add code to route std err msgs to log file.
# Paul Baranoski 11/8/2024  Modify logic to get filenames and record counts to make it simpler. Add logic to use wc -lc to get record and byte counts 
#                           with DASHBOARD_INFO: label for DashboardInfo_MS.sh to get extract files, record counts, and byte counts.
######################################################################################
set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
LOGNAME=/app/IDRC/XTR/CMS/logs/PSPS_Split_files_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "PSPS_Split_files.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh >> ${LOGNAME}

S3BUCKET=${PSPS_BUCKET} 

echo "PSPS bucket=${S3BUCKET}" >> ${LOGNAME}


############################################
# Determine if doing Q4 or Q6 files.
############################################
MM=`date +%m`
if [ $MM = '01' -o $MM = "02" -o $MM = "03" ]; then
	QTR=Q4
	PREFIX=PSPS_Extract_Q4
	PSPS_HCPCS_PREFIX=PSPS_HCPCS_Q4_PSPS
else
	QTR=Q6
	PREFIX=PSPS_Extract_Q6
	PSPS_HCPCS_PREFIX=PSPS_HCPCS_Q6_PSPS
fi

echo "Processing for QTR=${QTR}" >> ${LOGNAME}
echo "S3 Filename prefix to search=${PREFIX}" >> ${LOGNAME}
echo "Split filename prefix=${PSPS_HCPCS_PREFIX}" >> ${LOGNAME}

EMAIL_MF_FILENAME=P#IDR.XTR.PBAR.${QTR}.PSPSXX.DYYMMDD.THHMMSST 
echo "EMAIL_MF_FILENAME=${EMAIL_MF_FILENAME}" >> ${LOGNAME}

############################################
# Remove residual files from linux
############################################
echo " " >> ${LOGNAME}
echo "Remove residual files from linux" >> ${LOGNAME}

rm ${DATADIR}${PREFIX}*  >> ${LOGNAME}  2>&1
rm ${DATADIR}${PSPS_HCPCS_PREFIX}*  >>  ${LOGNAME}  2>&1

	
############################################
# Get list of S3 Q4/Q6 files
############################################
echo " " >> ${LOGNAME}
echo "Get list of S3 Q4/Q6 files." >> ${LOGNAME}

aws s3 ls s3://${S3BUCKET}archive/${PREFIX} > ${DATADIR}tempPSPSSplit.txt  2>&1

RET_STATUS=$?	

if [[ $RET_STATUS != 0 ]]; then
	echo " " >> ${LOGNAME}
	echo "Getting list of S3 Q4/Q6 files from S3 failed." >> ${LOGNAME}
	
	## Send Failure email	
	SUBJECT="PSPS Split Files - Failed (${ENVNAME})"
	MSG="Getting list of S3 Q4/Q6 files from S3 failed. Possibly no Q4/Q6 files exist in S3."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_HCPCS_EMAIL_SENDER}" "${PSPS_HCPCS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

   exit 12
fi	


############################################
# Determine most recent file
############################################
echo " " >> ${LOGNAME}
echo "Find most recent S3 Q4/Q6 file." >> ${LOGNAME}

gz_filename=`sort -r ${DATADIR}tempPSPSSplit.txt | head -n 1 | awk '{print $4}' `   2>> ${LOGNAME}


############################################
# Download most recent file from S3 to linux
############################################
echo " " >> ${LOGNAME}
echo "Copy most recent S3 Q4/Q6 file ${gz_filename} to linux." >> ${LOGNAME}

aws s3 cp s3://${S3BUCKET}archive/${gz_filename} ${DATADIR}${gz_filename}  >> ${LOGNAME}

RET_STATUS=$?	

if [[ $RET_STATUS != 0 ]]; then
	echo " " >> ${LOGNAME}
	echo "Downloading s3://${S3BUCKET}archive/${gz_filename} from S3 failed." >> ${LOGNAME}
	
	## Send Failure email	
	SUBJECT="PSPS_Split_files.bash - Failed (${ENVNAME})"
	MSG="Downloading s3://${S3BUCKET}archive/${gz_filename} from S3 failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_HCPCS_EMAIL_SENDER}" "${PSPS_HCPCS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

   exit 12
fi	


############################################
# unzip gz file
############################################
echo " " >> ${LOGNAME}
echo "unzip ${gz_filename} on linux" >> ${LOGNAME}

gzip -d ${DATADIR}${gz_filename}   2>> ${LOGNAME}

txt_filename=`echo ${gz_filename} | sed s/.gz//g`
echo "txt_filename=${txt_filename}" >> ${LOGNAME}


############################################
# split extract file into 25 files by HCPCS
############################################
echo " " >> ${LOGNAME}
echo "Split extract file into 25 files by HCPCS code." >> ${LOGNAME}
echo "Started --> `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}

${RUNDIR}splitByHCPCS.awk -v outfile="${DATADIR}${PSPS_HCPCS_PREFIX}" ${DATADIR}${txt_filename}  >> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "awk script splitByHCPCS.awk failed." >> ${LOGNAME}
		echo "Spliting PSPS file into separate files by HCPCS failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="PSPS Split files - Failed (${ENVNAME})"
		MSG="The PSPS Split files awk script has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_HCPCS_EMAIL_SENDER}" "${PSPS_HCPCS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "Ended --> `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}


#################################
# Get list of split.txt files
#################################
echo " " >> ${LOGNAME}
echo "Get list of .txt split files" >> ${LOGNAME}

splitFiles=`ls ${DATADIR}${PSPS_HCPCS_PREFIX}*.txt` 2>>  ${LOGNAME}
echo "splitFiles=${splitFiles}" >>  ${LOGNAME}


#############################################
# Get filenames and record counts for email
#############################################
echo " " >> ${LOGNAME}
echo "Change working directory to ${DATADIR} "    >> ${LOGNAME}

cd ${DATADIR}  2>> ${LOGNAME}
echo "pwd:" `pwd`   >> ${LOGNAME}

echo " " >> ${LOGNAME}
echo "Get record counts " >> ${LOGNAME}

filenamesAndCounts=`wc -l ${PSPS_HCPCS_PREFIX}*.txt | grep -v 'total' | awk '{print $2,$1}' | xargs printf "%s %'14d\n"` 2>> ${LOGNAME}

# This is required for DashboardInfo.sh --> its looking for that keyword to retrieve record counts
echo "filenamesAndCounts: ${filenamesAndCounts} "   >> ${LOGNAME}

# New way for DashboardInfo.sh to get filenames, record counts, and byte counts
DASHBOARD_INFO=`wc -lc ${PSPS_HCPCS_PREFIX}*.txt | grep -v 'total' | awk '{print $3,$1,$2}' | xargs printf "DASHBOARD_INFO:%s %s %s \n" `  2>> ${LOGNAME}

# print DASHBOARD Info to log 	
echo ""  >> ${LOGNAME}
echo "${DASHBOARD_INFO}" >> ${LOGNAME}

echo " " >> ${LOGNAME}
echo "Change working directory to ${RUNDIR} "    >> ${LOGNAME}

cd ${RUNDIR}  2>> ${LOGNAME}
echo "pwd:" `pwd`   >> ${LOGNAME}


##############################
# Gzip split files
##############################
echo " " >> ${LOGNAME}
echo "gzip Split files --> Started `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}

echo " " >> ${LOGNAME} 
		
for pathAndFilename in ${splitFiles}
do

	##############################
	# gzip txt files
	##############################
	echo "gzip ${pathAndFilename}" >>  ${LOGNAME}
	
	# remove file before issuing gzip to avoid prompt "Do you want to overwrite existing file?"
	gzip ${pathAndFilename} 2>>  ${LOGNAME}

	RET_STATUS=$?	

	if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "creating .gz file ${pathAndFilename} failed." >> ${LOGNAME}
		
		## Send Failure email	
		SUBJECT="PSPS Extract - Failed (${ENVNAME})"
		MSG="Compressing the PSPS split files with gzip failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_HCPCS_EMAIL_SENDER}" "${PSPS_HCPCS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
	fi
	
done

echo "gzip Split files --> Ended `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}


#################################
# get list of .gz files
#################################
echo " " >> ${LOGNAME}
echo "Get list of .gz split files" >> ${LOGNAME}

gzFiles=`ls ${DATADIR}${PSPS_HCPCS_PREFIX}*.gz`  >> ${LOGNAME}
echo "${gzFiles}" >> ${LOGNAME} 


##############################
# copy .gz files to s3
##############################
echo " " >> ${LOGNAME}
echo "Copy gz files to s3 --> Started `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}

for pathAndFilename in ${gzFiles}
do
	echo "pathAndFilename=${pathAndFilename}"  >>  ${LOGNAME}
	gz_filename=`basename ${pathAndFilename}`
	echo "gz_filename=${gz_filename}" >> ${LOGNAME}
	
	aws s3 mv ${pathAndFilename} s3://${S3BUCKET}${gz_filename} 1>> ${LOGNAME} 

	RET_STATUS=$?	

	if [[ $RET_STATUS != 0 ]]; then
        echo " " >> ${LOGNAME}
        echo "Copying file ${gz_filename} to s3://${S3BUCKET}${gz_filename} failed." >> ${LOGNAME}
		
		## Send Failure email	
		SUBJECT="PSPS Split Files - Failed (${ENVNAME})"
		MSG="Copying PSPS split files to S3 has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_HCPCS_EMAIL_SENDER}" "${PSPS_HCPCS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

       exit 12
	fi	

done

echo "Copy gz files to s3 --> Ended `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}


#############################################################
# Send Success email.
#############################################################
echo "" >> ${LOGNAME}
echo "Send success email with S3 Extract filename." >> ${LOGNAME}

S3Files="${filenamesAndCounts}" 
echo "S3Files=${S3Files} "   >> ${LOGNAME}

SUBJECT="PSPS Split Files Extract for ${QTR} (${ENVNAME})" 
MSG="The PSPS Split Files Extract for ${QTR} has completed.\n\nMainframe versions of the files will be created like ${EMAIL_MF_FILENAME}.\n\nThe following file(s) were created:\n\n${S3Files}"


${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_HCPCS_EMAIL_SENDER}" "${PSPS_HCPCS_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Error in calling sendEmail.py" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Sending Success email in PSPS_Split_files.bash  - Failed (${ENVNAME})"
		MSG="Sending Success email in PSPS_Split_files.bash  has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_HCPCS_EMAIL_SENDER}" "${PSPS_HCPCS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
fi	


#############################################################
# EFT Extract file(s)
#############################################################
echo " " >> ${LOGNAME}
echo "EFT PSPS HCPCS Split files at `date`" >> ${LOGNAME}
${RUNDIR}ProcessFiles2EFT.sh ${S3BUCKET}  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of extract script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script ProcessFiles2EFT.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="PSPS_Split_files.sh - Failed (${ENVNAME})"
	MSG="PSPS HCPCS Split file EFT process has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_SUPPRESSION_EMAIL_SENDER}" "${PSPS_SUPPRESSION_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi
	


############################################
# script clean-up
############################################
echo " " >> ${LOGNAME}
echo "script clean-up"    >> ${LOGNAME}   2>&1
rm 	${DATADIR}${PREFIX}*  >> ${LOGNAME}   2>&1


#############################################################
# end script
#############################################################
echo "" >> ${LOGNAME}
echo "PSPS_Split_files.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS