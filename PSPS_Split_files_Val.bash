#!/usr/bin/bash

######################################################################################
# Name:  PSPS_Split_files.bash
#
# Desc: Split PSPS file into multiple files by HCPCS code (IDR#PBA6). 
#       Q4 or Q6 file must exist on Linux as .txt file. 
#
# Created: Paul Baranoski  07/13/2022
# Modified:
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

############################################
# Set appropriate S3 bucket/path name
# in SET_XTR_ENV.sh
############################################
#if [ $ENVNAME = 'PRD' ]; then
#	bucket=aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod/xtr/PRD/
#else
#	bucket=aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod/xtr/${ENVNAME}/
#fi	
echo "bucket=${bucket}" >> ${LOGNAME}

############################################
# Determine if doing Q4 or Q6 files.
############################################
MM=`date +%m`
MM="07"
if [ $MM = '01' ]; then
	QTR=Q4
else
	QTR=Q6
fi

############################################
# Does file to split exist?
############################################
if [ ! -e ${DATADIR}PBAR_PSPS${QTR}*.txt ]; then
	echo "" >> ${LOGNAME}
	echo "File to split ${DATADIR}PBAR_PSPS${QTR}.txt does not exist." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="PSPS Split files - Failed"
	MSG="The PSPS PBAR_PSPS${QTR}*.txt file does not exist to split into separate files. "
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_EMAIL_SENDER}" "${PSPS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi

############################################
# split extract file into 25 files by HCPCS
############################################
echo " " >> ${LOGNAME}
echo "Split extract file into 25 files by HCPCS code." >> ${LOGNAME}
echo "Started --> `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}

${RUNDIR}/splitByHCPCS.awk -v outfile="${DATADIR}PBAR_${QTR}_PSPS" ${DATADIR}PBAR_PSPS${QTR}*.txt  >> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "awk script splitByHCPCS.awk failed." >> ${LOGNAME}
		echo "Spliting PSPS file into separate files by HCPCS failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="PSPS Split files - Failed"
		MSG="The PSPS Split files awk script has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_EMAIL_SENDER}" "${PSPS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi


#################################
# get list of split.txt files
# PBAR.PSPSQ6
#################################
echo " " >> ${LOGNAME}
echo "Get list of .txt files" >> ${LOGNAME}
echo "Started --> `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}

splitFiles=`ls ${DATADIR}PBAR_${QTR}_PSPS*.txt` 2>>  ${LOGNAME}
##echo ${splitFiles} >>  ${LOGNAME}


##############################
# gzip txt files
##############################
echo " " >> ${LOGNAME}
echo "gzip txt files" >> ${LOGNAME}
echo "Started --> `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}

rm "${DATADIR}"PBAR_"${QTR}"_PSPS*.gz 2>>  ${LOGNAME}

echo " " >> ${LOGNAME} 
		
for pathAndFilename in ${splitFiles}
do
	echo "gzip ${pathAndFilename}" >>  ${LOGNAME}
	# remove file before issuing gzip to avoid prompt "Do you want to overwrite existing file?"

	gzip ${pathAndFilename} 2>>  ${LOGNAME}

	RET_STATUS=$?	

	if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "creating .gz file ${pathAndFilename} failed." >> ${LOGNAME}
		
		## Send Failure email	
		SUBJECT="PSPS Extract - Failed"
		MSG="Compressing the PSPS split files with gzip failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_EMAIL_SENDER}" "${PSPS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
	fi

done


#################################
# get list of .gz files
#################################
echo " " >> ${LOGNAME}
echo "Get list of gz files" >> ${LOGNAME}
echo "Started --> `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}

gzFiles=`ls ${DATADIR}PBAR_${QTR}_PSPS*.gz`  >> ${LOGNAME}
#echo "${gzFiles}" >> ${LOGNAME} 


##############################
# put .gz files to s3
##############################
echo " " >> ${LOGNAME}
echo "Copy gz files to s3" >> ${LOGNAME}
echo "Started --> `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}


for pathAndFilename in ${gzFiles}
do
	echo "pathAndFilename:${pathAndFilename}"  >>  ${LOGNAME}
	filename=`basename ${pathAndFilename}`
	
	aws s3 cp ${pathAndFilename} s3://${bucket}${filename} 1>> ${LOGNAME} 

	RET_STATUS=$?	

	if [[ $RET_STATUS != 0 ]]; then
        echo " " >> ${LOGNAME}
        echo "Copying ${pathAndFilename} to s3 failed." >> ${LOGNAME}
		echo "S3 bucket: ${bucket}" >> ${LOGNAME}
		
		## Send Failure email	
		SUBJECT="PSPS Split Files - Failed"
		MSG="Copying PSPS split files to S3 has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_EMAIL_SENDER}" "${PSPS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

       exit 12
	fi	

done


####################################################
# Get filename to display in success email
# Ex: PBAR_Q6_PSPS01_20220722.090223.txt.gz
####################################################
ExampleFilename=`ls ${DATADIR}PBAR_${QTR}_PSPS01*.gz | sed 's/PSPS01/PSPSXX/g'` 2>>  ${LOGNAME}
EmailFilename=`basename ${ExampleFilename}`


#########################################
# clean-up .gz files in data directory
# clean-up Q4/Q6 file in data directory
#########################################
echo " " >> ${LOGNAME}
echo "Remove .gz files from data directory" >> ${LOGNAME}
echo "Started --> `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}

rm "${DATADIR}"PBAR_${QTR}_PSPS*.gz 2>>  ${LOGNAME}

echo "Remove PBAR_PSPS${QTR}*.txt from data directory" >> ${LOGNAME}
rm "${DATADIR}"PBAR_PSPS${QTR}*.txt 2>>  ${LOGNAME}

echo " " >> ${LOGNAME}


#########################################
# Send success email
#########################################
SUBJECT="PSPS Split Files Process for ${QTR} "
MSG="PSPS Split Files Process for ${QTR} has completed. 

Twenty-five files were created as ${EmailFilename} where XX is 01 - 25. Files were compressed using gzip."


${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PSPS_EMAIL_SENDER}" "${PSPS_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1


##############################
# script completed.
##############################
echo " " >> ${LOGNAME}
echo "Script PSPS_Split_files.sh completed successfully." >> ${LOGNAME}
echo `date` >> ${LOGNAME}