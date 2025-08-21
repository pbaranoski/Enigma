#!/usr/bin/bash

######################################################################################
# Name:  PAC_Copy_files_S3.bash
#
# Desc: Copy PAC Files to S3
#
# Created: Viren Khanna  12/12/2022
# Modified:
#
# Paul Baranoski 03/11/2024 Modified code to get REC_CNTS for DashboardInfo.sh.
######################################################################################
set +x

#############################################################
# Establish log file  
#############################################################
#TMSTMP = If TMSTMP value set by caller via export --> use that value. 
#         Else use the timestamp created in this script
TMSTMP=${TMSTMP=`date +%Y%m%d.%H%M%S`}

LOGNAME=/app/IDRC/XTR/CMS/logs/PAC_Copy_files_S3_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "PAC_Copy_files_S3.bash started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
echo "TMSTMP=${TMSTMP} " >> ${LOGNAME}


#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh >> ${LOGNAME}



#############################################################
# Verify that required NOF parameters have been sent 
#############################################################
if [ $# != 2 ]; then
	echo "Incorrect # of parameters sent to script. NOF parameters: $#" >> ${LOGNAME}
	exit 12
fi

echo "Parameters to script: " >> ${LOGNAME}
echo "   S3BucketAndFldr=${S3BucketAndFldr} " >> ${LOGNAME}
echo "   S3FilenameTmstmp=${S3FilenameTmstmp} " >> ${LOGNAME}

#############################################################
# Display parameters passed to script 
#############################################################
S3BucketAndFldr=$1
S3FilenameTmstmp=$2


#########################################
# clean-up .csv files in data directory
#########################################
echo " " >> ${LOGNAME}
echo "Remove .csv files from data directory" >> ${LOGNAME}
echo "Started --> `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}

echo "Remove PAC*.csv from data directory" >> ${LOGNAME}
rm "${DATADIR}"PAC*.csv 2>>  ${LOGNAME}


############################################
# get list of split files and record counts
# for DashboardInfo.sh
############################################
echo " " >> ${LOGNAME}

REC_CNTS=`ls -1 ${DATADIR}${DATADIR}PAC* | xargs wc -l | grep -v 'total' | awk '{print $2 " " $1}' | cut -d/ -f7 | xargs printf "%s %'14d\n"` 2>> ${LOGNAME}

# This is required for DashboardInfo.sh --> its looking for that keyword to retrieve record counts
echo "filenamesAndCounts: ${REC_CNTS} "   >> ${LOGNAME}


#################################
# get list of split.csv files
#################################
echo " " >> ${LOGNAME}
echo "Get list of .csv files" >> ${LOGNAME}
echo "Started --> `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}

splitFiles=`ls ${DATADIR}PAC*` 2>>  ${LOGNAME}
echo ${splitFiles} >>  ${LOGNAME}


##############################
# gzip csv files
##############################
echo " " >> ${LOGNAME}
echo "gzip csv files" >> ${LOGNAME}
echo "Started --> `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}

rm "${DATADIR}"PAC*.gz 2>>  ${LOGNAME}

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
		SUBJECT="PAC Extract - Failed"
		MSG="Compressing the PAC split files with gzip failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PAC_EMAIL_SENDER}" "${PAC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
	fi

done


#################################
# get list of .gz files
#################################
echo " " >> ${LOGNAME}
echo "Get list of gz files" >> ${LOGNAME}
echo "Started --> `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}

gzFiles=`ls ${DATADIR}PAC*.gz`  >> ${LOGNAME}
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
	
	aws s3 cp ${pathAndFilename} s3://${S3BucketAndFldr}${filename} 1>> ${LOGNAME} 

	RET_STATUS=$?	

	if [[ $RET_STATUS != 0 ]]; then
        echo " " >> ${LOGNAME}
        echo "Copying ${pathAndFilename} to s3 failed." >> ${LOGNAME}
		echo "S3 bucket: ${bucket}" >> ${LOGNAME}
		
		## Send Failure email	
		SUBJECT="PAC Split Files - Failed"
		MSG="Copying PAC split files to S3 has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PAC_EMAIL_SENDER}" "${PAC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

       exit 12
	fi	

done

#########################################
# clean-up .gz files in data directory
#########################################
echo " " >> ${LOGNAME}
echo "Remove .gz files from data directory" >> ${LOGNAME}
echo "Started --> `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}

rm "${DATADIR}"PAC*.gz 2>>  ${LOGNAME}


echo " " >> ${LOGNAME}





#############################################################
# Send Success email.
#############################################################
echo "" >> ${LOGNAME}

echo "" >> ${LOGNAME}
echo "Get S3 Extract file list" >> ${LOGNAME}

S3Files=`aws s3 ls s3://${S3BucketAndFldr} | awk '{print $4}' | grep ${S3FilenameTmstmp} | tr ' ' '\n' `  2>> ${LOGNAME}

echo "Send success email with S3 Extract filename." >> ${LOGNAME}
echo "S3Files=${S3Files} "   >> ${LOGNAME}

SUBJECT="PAC Split Files Extract for ${FYQ} " 
MSG="The PAC Split Files Extract for ${FYQ} has completed.\n\nThe following file(s) were created:\n\n${S3Files}"


${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PAC_EMAIL_SENDER}" "${PAC_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Error in calling sendEmail.py" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Sending Success email in PAC_Copy_files_S3.bash  - Failed"
		MSG="Sending Success email in PAC_Copy_files_S3.bash  has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PAC_EMAIL_SENDER}" "${PAC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
fi	

##############################
# script completed.
##############################
echo " " >> ${LOGNAME}
echo "Script PAC_Copy_files_S3.bash completed successfully." >> ${LOGNAME}
echo `date` >> ${LOGNAME}