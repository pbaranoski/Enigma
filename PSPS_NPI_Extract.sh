#!/usr/bin/sh
######################################################################################
# Name: PSPS_NPI_Extract.sh
# 
# Desc: Extract PBAR PSPS NPI data 
#       1) Script will run to create main extract file and split that file into the 25 HCPCS category files.
#       2) Main extract file will be place in archive folder.
#       3) After script completes, notify Sean Whitelock to run his script to create SAS file versions
#          of the 25 HCPCS category files. His program will move the non-SAS files to the archive directory.
#       4) After Sean Whitelock's script creates the 25 SAS files, run the ProcessFiles2EFT.sh script
#          to EFT the SAS files to the MF.
#       5) Verify that the SAS files land on the MF successfully.
#
#
# On-Prem Version: 
# Mainframe JCLs: IDR#PBN1,IDR#PBN2,IDR#PBS1,IDR#PBS2,IDR#PBS3,IDR#PBS4
# Date of Implementation: 01/20/2013
# Cloud Conversion scripts
# Created: Sumathi Gayam  12/15/2022
# Modified:
#
# Paul Baranoski 07/21/2023 Modified code to get REC_CNTS. Filename was hard-coded as JAN, 
#                          (so other month file counts were not captured to be displayed
#                           in email. 
# Paul Baranoski 01/22/2024 Add extract YYYY to pass to python script to add to extract filename.
#                           Convert hard-coded names in various spots to use a variable.
#                           Did a general clean-up/convert to standards.
# Paul Baranoski 05/06/2024 Add code to migrate main extract to archive folder after HCPCS category files have been created. 
#                           This way the PSPS_NPI folder only contains the HCPCS category files that the SAS script will process.
# Paul Baranoski 07/23/2024 Change extract filename from PSPS_NPI to PSPSNPI to distinguish it from the HCPCS category files (25 of them which are named PSPS_NPI).
#                           Can also EFT this file separately until we can send Cloud created SAS files to MF.
# Paul Baranoski 11/8/2024  Modify logic to get filenames and record counts to make it simpler. Add logic to use wc -lc to get record and byte counts 
#                           with DASHBOARD_INFO: label for DashboardInfo_MS.sh to get extract files, record counts, and byte counts.
# Paul Baranoski 02/04/2025 Rewrite script to EFT all-in-one-file to MF and archive the split files.
######################################################################################

set +x


#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
RUNDATE=`date +%Y%m%d`
LOGNAME=/app/IDRC/XTR/CMS/logs/PSPS_NPI_Extract_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

echo "################################### " >> ${LOGNAME}
echo "PSPS_NPI_Extract started at `date` " >> ${LOGNAME}


###############################################################################
# set Parm values for the file name 
###############################################################################
echo " " >> ${LOGNAME}
echo "Set Parm values for extract filename " >> ${LOGNAME}

ext_YYYY=$(date +%Y)
echo "ext_YYYY=${ext_YYYY}" >> ${LOGNAME}

month=$(date +%m)
echo "Current Month : $month" >> ${LOGNAME}
	if [ $month -lt 7 ]; then
		ext_mon='JAN'
	else

		ext_mon='JUL'
	fi
echo "Extract Month = $ext_mon" >> ${LOGNAME}


#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh >> ${LOGNAME}

S3BUCKET=${PSPSNPI_BUCKET} 

#############################################################
# Set filename variables for use by script
#############################################################
echo "" >> ${LOGNAME}
echo "Set extract filename variables for use by script" >> ${LOGNAME}

PSPSNPI_S3FILE=PBAR_PSPSNPI_${ext_YYYY}_${ext_mon}_${TMSTMP}.txt.gz
UNZIPPED_PSPSNPI_S3FILE=`echo ${PSPSNPI_S3FILE} | sed 's/.gz//g' ` 2>> ${LOGNAME}
PBAR_HCPCS_SPLIT_FILE_MASK=PBAR_PSPS_NPI_${ext_YYYY}_${ext_mon}_P
PBAR_PSPS_NPI_ALL_FILES_MASK=PBAR_PSPS

echo "PSPSNPI_S3FILE=${PSPSNPI_S3FILE}" >> ${LOGNAME}
echo "UNZIPPED_PSPSNPI_S3FILE=${UNZIPPED_PSPSNPI_S3FILE}" >> ${LOGNAME}
echo "PBAR_HCPCS_SPLIT_FILE_MASK=${PBAR_HCPCS_SPLIT_FILE_MASK}" >> ${LOGNAME}
echo "PBAR_PSPS_NPI_ALL_FILES_MASK=${PBAR_PSPS_NPI_ALL_FILES_MASK}" >> ${LOGNAME}


#############################################################
# Remove residual files from Data Directory   
#############################################################
echo " " >> ${LOGNAME}
echo "Remove residual files from linux data directory." >> ${LOGNAME}
rm -f "${DATADIR}${PBAR_PSPS_NPI_ALL_FILES_MASK}"*  2>>  ${LOGNAME}


#############################################################
# Make variables available for substitution in Python code
#############################################################
export TMSTMP
export ext_mon
export ext_YYYY


#############################################################
# Execute Python code
#############################################################
echo "" >> ${LOGNAME}
echo "Start execution of PSPS_NPI_Extract.py program" >> ${LOGNAME}

${PYTHON_COMMAND} ${RUNDIR}PSPS_NPI_Extract.py  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script PSPS_NPI_Extract.py failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="PSPS NPI Extract - Failed (${ENVNAME})"
		MSG="The PSPS NPI extract has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPSNPI_EMAIL_SENDER}" "${PSPSNPI_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script PSPS_NPI_Extract.py completed successfully." >> ${LOGNAME}


###########################################################################################
# Concatenate PSPS NPI S3 files into a single file 
###########################################################################################
echo "" >> ${LOGNAME}
echo "Concatenate S3 files using CombineS3Files.sh." >> ${LOGNAME}

${RUNDIR}CombineS3Files.sh ${PSPSNPI_BUCKET} ${PSPSNPI_S3FILE} 

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script CombineS3Files.sh failed." >> ${LOGNAME}
	
	# Send failure email
	SUBJECT="PSPS NPI S3 files concatenation FAILED (${ENVNAME})"
	MSG="PSPS NPI Extract has failed in PSPS NPI S3 files concatenation step"
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSSPNPI_EMAIL_SENDER}" "${PSSPNPI_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
	exit 12
fi


#################################################################################
# Copy PSPS NPI files from S3 to linux
#################################################################################
echo "" >> ${LOGNAME}
echo "Starting copy of S3 PSPS NPI file to Linux." >> ${LOGNAME}
	
aws s3 cp s3://${PSPSNPI_BUCKET}${PSPSNPI_S3FILE} ${DATADIR}${PSPSNPI_S3FILE}  1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Copying PSPSNPI S3 file to Linux failed." >> ${LOGNAME}

	# Send Failure email	
	SUBJECT="PSPS NPI Extract - Failed (${ENVNAME})"
	MSG="PSPS NPI Extract copy S3 file failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPSNPI_EMAIL_SENDER}" "${PSPSNPI_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
	exit 12
fi	

	
##############################
# gunzip PSPS NPI file from S3
##############################
echo " " >> ${LOGNAME}
echo "Unzip .gz PSPS NPI file" >> ${LOGNAME}
echo "Started Unzipping process --> `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}

gzip -d ${DATADIR}${PSPSNPI_S3FILE}  2>>  ${LOGNAME}


#################################################
# Does PSPS NPI file to split exist on the server?
#################################################
if [ ! -e ${DATADIR}${UNZIPPED_PSPSNPI_S3FILE} ]; then
	echo "" >> ${LOGNAME}
	echo "File to split ${DATADIR}${UNZIPPED_PSPSNPI_S3FILE} does not exist." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="PSPS NPI file not found in the server - Failed (${ENVNAME})"
	MSG="The ${UNZIPPED_PSPSNPI_S3FILE} file does not exist to split into separate files. "
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPSNPI_EMAIL_SENDER}" "${PSPSNPI_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


############################################
# split extract file into 25 files by HCPCS
############################################
echo " " >> ${LOGNAME}
echo "Split PSPS NPI extract file into 25 files by HCPCS code range." >> ${LOGNAME}
echo "Started --> `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}
                                               
${RUNDIR}/splitByHCPCS.awk -v outfile="${DATADIR}${PBAR_HCPCS_SPLIT_FILE_MASK}" ${DATADIR}${UNZIPPED_PSPSNPI_S3FILE}  >> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "awk script splitByHCPCS.awk failed." >> ${LOGNAME}
		echo "Spliting PSPS NPI file into separate files by HCPCS failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="PSPS NPI Split files - Failed (${ENVNAME})"
		MSG="The PSPS NPI Split files awk script has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPSNPI_EMAIL_SENDER}" "${PSPSNPI_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi


#################################
# get list of split.txt files
#################################
echo " " >> ${LOGNAME}
echo "Get list of .txt files" >> ${LOGNAME}
echo "Started --> `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}

splitFiles=`ls -1 ${DATADIR}${PBAR_HCPCS_SPLIT_FILE_MASK}*.txt` 2>>  ${LOGNAME}
echo ${splitFiles} >>  ${LOGNAME}


#############################################################
# Get record count of PSPS NPI files.
#############################################################
echo " " >> ${LOGNAME}
echo "Change working directory to ${DATADIR} "    >> ${LOGNAME}

cd ${DATADIR}  2>> ${LOGNAME}
echo "pwd:" `pwd`   >> ${LOGNAME}

echo " " >> ${LOGNAME}
echo "Get record counts " >> ${LOGNAME}

#REC_CNTS=`ls -1 ${DATADIR}${PBAR_PSPS_NPI_ALL_FILES_MASK}*.txt | xargs wc -l | grep -v 'total' | awk '{print $2 " " $1}' | cut -d/ -f7 | xargs printf "%s %'14d\n"` 2>> ${LOGNAME}
REC_CNTS=`wc -l ${PBAR_PSPS_NPI_ALL_FILES_MASK}*.txt | grep -v 'total' | awk '{print $2,$1}' | xargs printf "%s %'14d\n"` 2>> ${LOGNAME}

# This is required for DashboardInfo.sh --> its looking for that keyword to retrieve record counts
echo "filenamesAndCounts: ${REC_CNTS} "   >> ${LOGNAME}

# New way for DashboardInfo.sh to get filenames, record counts, and byte counts
DASHBOARD_INFO=`wc -lc ${PBAR_PSPS_NPI_ALL_FILES_MASK}*.txt | grep -v 'total' | awk '{print $3,$1,$2}' | xargs printf "DASHBOARD_INFO:%s %s %s \n" `  2>> ${LOGNAME}

# print DASHBOARD Info to log 	
echo ""  >> ${LOGNAME}
echo "${DASHBOARD_INFO}" >> ${LOGNAME}

echo " " >> ${LOGNAME}
echo "Change working directory to ${RUNDIR} "    >> ${LOGNAME}

cd ${RUNDIR}  2>> ${LOGNAME}
echo "pwd:" `pwd`   >> ${LOGNAME}


##############################
# gzip txt files
##############################
echo " " >> ${LOGNAME}
echo "gzip txt files" >> ${LOGNAME}
echo "Started --> `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}

echo " " >> ${LOGNAME} 
		
for pathAndFilename in ${splitFiles}
do
	echo "gzip ${pathAndFilename}" >>  ${LOGNAME}

	gzip ${pathAndFilename} 2>>  ${LOGNAME}

	RET_STATUS=$?	

	if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "creating .gz file ${pathAndFilename} failed." >> ${LOGNAME}
		
		## Send Failure email	
		SUBJECT="PSPS NPI Extract - Failed (${ENVNAME})"
		MSG="Compressing the PSPS NPI split files with gzip failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPSNPI_EMAIL_SENDER}" "${PSPSNPI_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
	fi

done


####################################
# get list of split .gz files
####################################
echo " " >> ${LOGNAME}
echo "Get list of gz files" >> ${LOGNAME}

gzFiles=`ls -1 ${DATADIR}${PBAR_HCPCS_SPLIT_FILE_MASK}*.gz`  >> ${LOGNAME}
echo "${gzFiles}" >> ${LOGNAME} 


####################################
# Copy split .gz files from linux to s3 archive folder
####################################
echo " " >> ${LOGNAME}
echo "Copy split gz files to s3" >> ${LOGNAME}
echo "Started --> `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}


for pathAndFilename in ${gzFiles}
do
	echo "pathAndFilename:${pathAndFilename}"  >>  ${LOGNAME}
	filename=`basename ${pathAndFilename}`
	echo "File Name :${filename}" >> ${LOGNAME}

	aws s3 cp ${pathAndFilename} s3://${PSPSNPI_BUCKET}archive/${filename} 1>> ${LOGNAME} 

	RET_STATUS=$?	

	if [[ $RET_STATUS != 0 ]]; then
        echo " " >> ${LOGNAME}
        echo "Copying ${pathAndFilename} to s3 failed." >> ${LOGNAME}
		echo "S3 bucket: ${PSPSNPI_BUCKET}" >> ${LOGNAME}
		
		## Send Failure email	
		SUBJECT="PSPS NPI Split Files - Failed (${ENVNAME})"
		MSG="Copying PSPS NPI split files to S3 has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPSNPI_EMAIL_SENDER}" "${PSPSNPI_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

       exit 12
	fi	

done


#############################################################
# Send Success email.
#############################################################
echo "" >> ${LOGNAME}
echo "Send success email with S3 Extract filename." >> ${LOGNAME}
echo "S3Files=${REC_CNTS} "   >> ${LOGNAME}

SUBJECT="PSPS NPI ${ext_mon} extract (${ENVNAME})" 
MSG="The Extract for the creation of the PSPS NPI ${ext_mon} file from Snowflake has completed.\n\nThe following file(s) were created:\n\n${REC_CNTS}"

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPSNPI_EMAIL_SENDER}" "${PSPSNPI_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Error in calling sendEmail.py" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Sending Success email in PSPS_NPIExt.sh  - Failed (${ENVNAME})"
	MSG="Sending Success email in PSPS_NPI_Extract has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPSNPI_EMAIL_SENDER}" "${PSPSNPI_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	


#################################################################################
# Move PSPS NPI main extract file to S3 archive folder
# -- Do NOT move main extract file to S3 archive folder. We will be EFTing that file to MF.
#################################################################################
#echo "" >> ${LOGNAME}
#echo "Move S3 PSPS NPI file ${PSPSNPI_S3FILE} to S3 archive directory." >> ${LOGNAME}
#	
#aws s3 mv s3://${PSPSNPI_BUCKET}${PSPSNPI_S3FILE} s3://${PSPSNPI_BUCKET}archive/${PSPSNPI_S3FILE}  1>> ${LOGNAME} 2>&1
#
#RET_STATUS=$?
#
#if [[ $RET_STATUS != 0 ]]; then
#	echo "" >> ${LOGNAME}
#	echo "Move PSPSNPI S3 file ${PSPSNPI_S3FILE} to S3 archive folder failed." >> ${LOGNAME}
#
#	# Send Failure email	
#	SUBJECT="PSPS NPI Extract - Failed (${ENVNAME})"
#	MSG="Move PSPSNPI S3 file ${PSPSNPI_S3FILE} to S3 archive folder failed."
#	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPSNPI_EMAIL_SENDER}" "${PSPSNPI_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
#	exit 12
#fi	


#############################################################
# Script clean-up - (so EFT process will work when it tries to unzip file from S3)
#############################################################
echo " " >> ${LOGNAME}
echo "Remove residual files from linux data directory." >> ${LOGNAME}
rm -f "${DATADIR}${PBAR_PSPS_NPI_ALL_FILES_MASK}"*  2>>  ${LOGNAME}  2>>  ${LOGNAME}


#############################################################
# EFT Extract files
#############################################################
echo " " >> ${LOGNAME}
echo "EFT PSPS NPI Extract File " >> ${LOGNAME}
${RUNDIR}ProcessFiles2EFT.sh ${S3BUCKET}  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of extract script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script ProcessFiles2EFT.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="PSPS NPI Extract EFT process  - Failed (${ENVNAME})"
	MSG="PSPS NPI Extract EFT process has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


#############################################################
# end script
#############################################################
echo "" >> ${LOGNAME}
echo "PSPS_NPI_Extract completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS
