#!/usr/bin/bash
#
######################################################################################
# Name:  PAC_QTR_Extract.sh
#
# Desc: PAC Extract for Q1 thru Q4 extracts. 
#
# Created: Viren Khanna  11/25/2022
#
# Viren Khanna added Split files and copying from S3 functionality
#
# Modified:
# Paul Baranoski 2024-07-31 Add ENV to Subject line for emails.
######################################################################################
set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
LOGNAME=/app/IDRC/XTR/CMS/logs/PAC_Extract_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/
TMPDIR=/app/IDRC/XTR/CMS/tmp/



touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "PAC_Extract.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh >> ${LOGNAME}

S3BUCKET=${PAC_BUCKET} 

echo "PAC Bucket=${S3BUCKET}" >> ${LOGNAME}


############################################
# Extract current and prior year
############################################
CUR_YR=`date +%Y`
PRIOR_YR=`expr ${CUR_YR} - 1` 

echo "CUR_YR=${CUR_YR}" >> ${LOGNAME}
echo "PRIOR_YR=${PRIOR_YR}" >> ${LOGNAME}

############################################
# Determine Processing Qtr
############################################
MM=`date +%m`
if [   $MM = "08" -o $MM = "09" -o $MM = "10" ];  then
	FYQ="${CUR_YR}Q1"
elif [   $MM = "11" -o $MM = "12" -o $MM = "01" ]; then
	FYQ="${CUR_YR}Q2"
elif [   $MM = "01" ]; then
	FYQ="${PRIOR_YR}Q2"
elif [   $MM = "05" -o $MM = "06" -o $MM = "07" ]; then
	FYQ="${PRIOR_YR}Q4"
elif [   $MM = "02" -o $MM = "03" -o $MM = "04" ]; then
	FYQ="${PRIOR_YR}Q3"
else
	echo "Extract is processed quarterly for months February, May, August, and November. " >> ${LOGNAME}
	echo "Extract is not scheduled to run for this time period. " >> ${LOGNAME}
	echo "Processing completed." >> ${LOGNAME}
	
	# Send Did not run email	
	#SUBJECT="PAC Extract did not run."
	#MSG="Extract is processed quarterly for months April, July, October, and January. Extract is not scheduled to run for this time period. "
	#${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PAC_EMAIL_SENDER}" "${PAC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
	
	exit 0 	
fi

echo "FYQ=${FYQ}" >> ${LOGNAME}

#############################################################
# Make variables available to Python code module.
#############################################################
export TMSTMP	
export FYQ
#export CLNDR_CYQ_END_DT



############################################
# Execute PAC_IRF_CLMS Qtr Extract
############################################
echo " " >> ${LOGNAME}
echo "Extract processing for PAC_IRF_CLMS for appropriate Fiscal Year Qtr. " >> ${LOGNAME}
${PYTHON_COMMAND} ${RUNDIR}PAC_IRF_CLMS.py  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script PAC_IRF_CLMS.py failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="PAC_IRF_CLMS Extract ${FYQ} - Failed (${ENVNAME})"
		MSG="PAC_IRF_CLMS extract ${FYQ} has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PAC_EMAIL_SENDER}" "${PAC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi




############################################
# Execute PAC_LTCH_CLMS Qtr Extract
############################################
echo " " >> ${LOGNAME}
echo "Extract processing for PAC_LTCH_CLMS for appropriate Fiscal Year Qtr " >> ${LOGNAME}
${PYTHON_COMMAND} ${RUNDIR}PAC_LTCH_CLMS.py  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script PAC_LTCH_CLMS.py failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="PAC_LTCH_CLMS Extract ${FYQ} - Failed (${ENVNAME})"
		MSG="PAC_LTCH_CLMS extract ${FYQ}  has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PAC_EMAIL_SENDER}" "${PAC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi





############################################
# Execute PAC_HOSP_CLMS Qtr Extract
############################################
echo " " >> ${LOGNAME}
echo "Extract processing for PAC_HOSP_CLMS for appropriate Fiscal Year Qtr. " >> ${LOGNAME}
${PYTHON_COMMAND} ${RUNDIR}PAC_HOSP_CLMS.py  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script PAC_HOSP_CLMS.py failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="PAC_HOSP_CLMS Extract ${FYQ} - Failed (${ENVNAME})"
		MSG="PAC_HOSP_CLMS extract ${FYQ} has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PAC_EMAIL_SENDER}" "${PAC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi




############################################
# Execute PAC_SNF_CLMS Qtr Extract
############################################
echo " " >> ${LOGNAME}
echo "Extract processing for PAC_SNF_CLMS for appropriate Fiscal Year Qtr. " >> ${LOGNAME}
${PYTHON_COMMAND} ${RUNDIR}PAC_SNF_CLMS.py  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script PAC_SNF_CLMS.py failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="PAC_SNF_CLMS Extract ${FYQ} - Failed (${ENVNAME})"
		MSG="PAC_SNF_CLMS extract ${FYQ} has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PAC_EMAIL_SENDER}" "${PAC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi



############################################
# Execute PAC_SNF_RC Qtr Extract
############################################
echo " " >> ${LOGNAME}
echo "Extract processing for PAC_SNF_RC for appropriate Fiscal Year Qtr. " >> ${LOGNAME}
${PYTHON_COMMAND} ${RUNDIR}PAC_SNF_RC.py  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script PAC_SNF_RC.py failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="PAC_SNF_RC Extract ${FYQ} - Failed (${ENVNAME})"
		MSG="PAC_SNF_RC extract ${FYQ} has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PAC_EMAIL_SENDER}" "${PAC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi



#################################################################################
# Copy PAC_SNF_RC file to Linux as .txt file for Splitting purposes
#################################################################################
	echo " " >> ${LOGNAME}
	echo "Starting copy of S3 PAC_SNF_RC file to Linux." >> ${LOGNAME}

			
	S3Filename=PAC_SNF_RC_Y${FYQ}_${TMSTMP}.csv.gz
	aws s3 cp s3://${S3BUCKET}${S3Filename} ${DATADIR}PAC_SNF_RC_Y${FYQ}_${TMSTMP}.csv.gz  1>> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Copying ${FYQ} S3 file to Linux failed." >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="PAC Extract - Failed (${ENVNAME})"
			MSG="PAC Extract copy PAC_SNF_RC file failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PAC_EMAIL_SENDER}" "${PAC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
	fi	
	
	##############################
	# gunzip PAC_SNF_RC file from S3
	##############################
	echo " " >> ${LOGNAME}
	echo "Unzip .gz PAC_SNF_RC file" >> ${LOGNAME}
	echo "Started --> `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}

	gzip -d ${DATADIR}PAC_SNF_RC_Y${FYQ}_${TMSTMP}.csv.gz  2>>  ${LOGNAME}


#################################################################################
# Split PAC_SNF_RC file 
#################################################################################
	echo " " >> ${LOGNAME}
	echo "Starting splitting of S3 PAC_SNF_RC file" >> ${LOGNAME}

			
	Filename=PAC_SNF_RC_Y${FYQ}_${TMSTMP}.csv
        #Number of files to split into
        num_files=4
       # Work out lines per file.
       total_lines=$(wc -l <${DATADIR}/${Filename})
       ((lines_per_file = ($total_lines + $num_files - 1) / $num_files))

       echo "total_lines:" ${total_lines}
       echo "lines_per_file:" ${lines_per_file} 

       # Split the actual file, maintaining lines.
      split --numeric-suffixes=1  --lines=${lines_per_file} -a 1 ${DATADIR}/${Filename} ${DATADIR}/${Filename}.F
 
  RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Splitting ${FYQ} S3 file to Linux failed." >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="PAC Extract - Failed (${ENVNAME})"
			MSG="PAC Extract Splitting PAC_SNF_RC file failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PAC_EMAIL_SENDER}" "${PAC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
	fi	




############################################
# Execute PAC_HOSP_RC Qtr Extract
############################################
echo " " >> ${LOGNAME}
echo "Extract processing for PAC_HOSP_RC for appropriate Fiscal Year Qtr. " >> ${LOGNAME}
${PYTHON_COMMAND} ${RUNDIR}PAC_HOSP_RC.py  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script PAC_HOSP_RC.py failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="PAC_HOSP_RC Extract ${FYQ} - Failed (${ENVNAME})"
		MSG="PAC_HOSP_RC extract ${FYQ} has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PAC_EMAIL_SENDER}" "${PAC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi


#################################################################################
# Copy PAC_HOSP_RC file to Linux as .txt file for Splitting purposes
#################################################################################
	echo " " >> ${LOGNAME}
	echo "Starting copy of S3 PAC_HOSP_RC file to Linux." >> ${LOGNAME}

			
	S3Filename=PAC_HOSP_RC_Y${FYQ}_${TMSTMP}.csv.gz
	aws s3 cp s3://${S3BUCKET}${S3Filename} ${DATADIR}PAC_HOSP_RC_Y${FYQ}_${TMSTMP}.csv.gz  1>> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Copying ${FYQ} S3 file to Linux failed." >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="PAC Extract - Failed (${ENVNAME})"
			MSG="PAC Extract copy PAC_HOSP_RC file failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PAC_EMAIL_SENDER}" "${PAC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
	fi	
	
	##############################
	# gunzip PAC_HOSP_RC file from S3
	##############################
	echo " " >> ${LOGNAME}
	echo "Unzip .gz PAC_HOSP_RC file" >> ${LOGNAME}
	echo "Started --> `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}

	gzip -d ${DATADIR}PAC_HOSP_RC_Y${FYQ}_${TMSTMP}.csv.gz  2>>  ${LOGNAME}


#################################################################################
# Split PAC_HOSP_RC file 
#################################################################################
	echo " " >> ${LOGNAME}
	echo "Starting splitting of S3 PAC_HOSP_RC file" >> ${LOGNAME}

			
	Filename=PAC_HOSP_RC_Y${FYQ}_${TMSTMP}.csv
        #Number of files to split into
        num_files=4
       # Work out lines per file.
       total_lines=$(wc -l <${DATADIR}/${Filename})
       ((lines_per_file = ($total_lines + $num_files - 1) / $num_files))
       echo "total_lines:" ${total_lines}
       echo "lines_per_file:" ${lines_per_file} 

       # Split the actual file, maintaining lines.
      split --numeric-suffixes=1  --lines=${lines_per_file} -a 1 ${DATADIR}/${Filename} ${DATADIR}/${Filename}.F
 
  RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Splitting ${FYQ} S3 file to Linux failed." >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="PAC Extract - Failed (${ENVNAME})"
			MSG="PAC Extract Splitting PAC_HOSP_RC file failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PAC_EMAIL_SENDER}" "${PAC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
	fi	




############################################
# Execute PAC_LTCH_RC Qtr Extract
############################################
echo " " >> ${LOGNAME}
echo "Extract processing for PAC_LTCH_RC for appropriate Fiscal Year Qtr.  " >> ${LOGNAME}
${PYTHON_COMMAND} ${RUNDIR}PAC_LTCH_RC.py  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script PAC_LTCH_RC.py failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="PAC_LTCH_RC Extract ${FYQ} - Failed (${ENVNAME})"
		MSG="PAC_LTCH_RC extract ${FYQ} has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PAC_EMAIL_SENDER}" "${PAC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi



############################################
# Execute PAC_IRF_RC Qtr Extract
############################################
echo " " >> ${LOGNAME}
echo "Extract processing for PAC_IRF_RC for appropriate Fiscal Year Qtr. " >> ${LOGNAME}
${PYTHON_COMMAND} ${RUNDIR}PAC_IRF_RC.py  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script PAC_IRF_RC.py failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="PAC_IRF_RC Extract ${FYQ} - Failed (${ENVNAME})"
		MSG="PAC_IRF_RC extract ${FYQ} has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PAC_EMAIL_SENDER}" "${PAC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi


############################################
# Execute PAC_LTCH_PC Qtr Extract
############################################
echo " " >> ${LOGNAME}
echo "Extract processing for PAC_LTCH_PC for appropriate Fiscal Year Qtr. " >> ${LOGNAME}
${PYTHON_COMMAND} ${RUNDIR}PAC_LTCH_PC.py  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script PAC_LTCH_PC.py failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="PAC_LTCH Extract ${FYQ} - Failed (${ENVNAME})"
		MSG="PAC_LTCH extract ${FYQ} has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PAC_EMAIL_SENDER}" "${PAC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

############################################
# Execute PAC_SNF_PC Qtr Extract
############################################
echo " " >> ${LOGNAME}
echo "Extract processing for PAC_SNF_PC for appropriate Fiscal Year Qtr. " >> ${LOGNAME}
${PYTHON_COMMAND} ${RUNDIR}PAC_SNF_PC.py  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script PAC_SNF_PC.py failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="PAC_SNF_PC Extract ${FYQ} - Failed (${ENVNAME})"
		MSG="PAC_SNF_PC extract ${FYQ} has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PAC_EMAIL_SENDER}" "${PAC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi


############################################
# Execute PAC_HOSP_DGNS Qtr Extract
############################################
echo " " >> ${LOGNAME}
echo "Extract processing for PAC_HOSP_DGNS for appropriate Fiscal Year Qtr. " >> ${LOGNAME}
${PYTHON_COMMAND} ${RUNDIR}PAC_HOSP_DGNS.py  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script PAC_HOSP_DGNS.py failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="PAC_HOSP_DGNS Extract ${FYQ} - Failed (${ENVNAME})"
		MSG="PAC_HOSP_DGNS extract ${FYQ} has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PAC_EMAIL_SENDER}" "${PAC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi



#################################################################################
# Copy PAC_HOSP_DGNS file to Linux as .txt file for Splitting purposes
#################################################################################
	echo " " >> ${LOGNAME}
	echo "Starting copy of S3 PAC_HOSP_DGNS file to Linux." >> ${LOGNAME}

			
	S3Filename=PAC_HOSP_DGNS_Y${FYQ}_${TMSTMP}.csv.gz
	aws s3 cp s3://${S3BUCKET}${S3Filename} ${DATADIR}PAC_HOSP_DGNS_Y${FYQ}_${TMSTMP}.csv.gz  1>> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Copying ${FYQ} S3 file to Linux failed." >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="PAC Extract - Failed (${ENVNAME})"
			MSG="PAC Extract copy PAC_HOSP_DGNS file failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PAC_EMAIL_SENDER}" "${PAC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
	fi	
	
	##############################
	# gunzip PAC_HOSP_DGNS file from S3
	##############################
	echo " " >> ${LOGNAME}
	echo "Unzip .gz PAC_HOSP_DGNS file" >> ${LOGNAME}
	echo "Started --> `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}

	gzip -d ${DATADIR}PAC_HOSP_DGNS_Y${FYQ}_${TMSTMP}.csv.gz  2>>  ${LOGNAME}


#################################################################################
# Split PAC_HOSP_DGNS file 
#################################################################################
	echo " " >> ${LOGNAME}
	echo "Starting splitting of S3 PAC_HOSP_DGNS file" >> ${LOGNAME}

			
	Filename=PAC_HOSP_DGNS_Y${FYQ}_${TMSTMP}.csv
        #Number of files to split into
        num_files=4
       # Work out lines per file.
       total_lines=$(wc -l <${DATADIR}/${Filename})
       ((lines_per_file = ($total_lines + $num_files - 1) / $num_files))

       echo "total_lines:" ${total_lines}
       echo "lines_per_file:" ${lines_per_file} 

       # Split the actual file, maintaining lines.
      split --numeric-suffixes=1  --lines=${lines_per_file} -a 1 ${DATADIR}/${Filename} ${DATADIR}/${Filename}.F
 
  RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Splitting ${FYQ} S3 file to Linux failed." >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="PAC Extract PAC_HOSP_DGNS - Failed (${ENVNAME})"
			MSG="PAC Extract Splitting PAC_HOSP_DGNS file failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PAC_EMAIL_SENDER}" "${PAC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
	fi	


############################################
# Execute PAC_LTCH_DGNS Qtr Extract
############################################
echo " " >> ${LOGNAME}
echo "Extract processing for PAC_LTCH_DGNS for appropriate Fiscal Year Qtr. " >> ${LOGNAME}
${PYTHON_COMMAND} ${RUNDIR}PAC_LTCH_DGNS.py  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script PAC_LTCH_DGNS.py failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="PAC_LTCH_DGNS Extract ${FYQ} - Failed (${ENVNAME})"
		MSG="PAC_LTCH_DGNS extract ${FYQ} has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PAC_EMAIL_SENDER}" "${PAC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi



############################################
# Execute PAC_IRF_DGNS Qtr Extract
############################################
echo " " >> ${LOGNAME}
echo "Extract processing for PAC_IRF_DGNS for appropriate Fiscal Year Qtr. " >> ${LOGNAME}
${PYTHON_COMMAND} ${RUNDIR}PAC_IRF_DGNS.py  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script PAC_IRF_DGNS.py failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="PAC_IRF_DGNS Extract ${FYQ} - Failed (${ENVNAME})"
		MSG="PAC_IRF_DGNS extract ${FYQ} has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PAC_EMAIL_SENDER}" "${PAC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi


############################################
# Execute PAC_SNF_DGNS Qtr Extract
############################################
echo " " >> ${LOGNAME}
echo "Extract processing for PAC_SNF_DGNS for appropriate Fiscal Year Qtr. " >> ${LOGNAME}
${PYTHON_COMMAND} ${RUNDIR}PAC_SNF_DGNS.py  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script PAC_SNF_DGNS.py failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="PAC_SNF_DGNS Extract ${FYQ} - Failed (${ENVNAME})"
		MSG="PAC_SNF_DGNS extract ${FYQ} has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PAC_EMAIL_SENDER}" "${PAC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi


#################################################################################
# Copy PAC_SNF_DGNS file to Linux as .txt file for Splitting purposes
#################################################################################
	echo " " >> ${LOGNAME}
	echo "Starting copy of S3 PAC_SNF_DGNS file to Linux." >> ${LOGNAME}

			
	S3Filename=PAC_SNF_DGNS_Y${FYQ}_${TMSTMP}.csv.gz
	aws s3 cp s3://${S3BUCKET}${S3Filename} ${DATADIR}PAC_SNF_DGNS_Y${FYQ}_${TMSTMP}.csv.gz  1>> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Copying ${FYQ} S3 file to Linux failed." >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="PAC Extract - Failed (${ENVNAME})"
			MSG="PAC Extract copy PAC_SNF_DGNS file failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PAC_EMAIL_SENDER}" "${PAC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
	fi	
	
	##############################
	# gunzip PAC_SNF_DGNS file from S3
	##############################
	echo " " >> ${LOGNAME}
	echo "Unzip .gz PAC_SNF_DGNS file" >> ${LOGNAME}
	echo "Started --> `date +%Y-%m-%d.%H:%M:%S`" >> ${LOGNAME}

	gzip -d ${DATADIR}PAC_SNF_DGNS_Y${FYQ}_${TMSTMP}.csv.gz  2>>  ${LOGNAME}


#################################################################################
# Split PAC_SNF_DGNS file 
#################################################################################
	echo " " >> ${LOGNAME}
	echo "Starting splitting of S3 PAC_SNF_DGNS file" >> ${LOGNAME}

			
	Filename=PAC_SNF_DGNS_Y${FYQ}_${TMSTMP}.csv
        #Number of files to split into
        num_files=4
       # Work out lines per file.
       total_lines=$(wc -l <${DATADIR}/${Filename})
       ((lines_per_file = ($total_lines + $num_files - 1) / $num_files))

       echo "total_lines:" ${total_lines}
       echo "lines_per_file:" ${lines_per_file} 

       # Split the actual file, maintaining lines.
      split --numeric-suffixes=1  --lines=${lines_per_file} -a 1 ${DATADIR}/${Filename} ${DATADIR}/${Filename}.F
 
  RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Splitting ${FYQ} S3 file to Linux failed." >> ${LOGNAME}
			
			# Send Failure email	
			SUBJECT="PAC Extract - Failed (${ENVNAME})"
			MSG="PAC Extract Splitting PAC_SNF_DGNS file failed."
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${PAC_EMAIL_SENDER}" "${PAC_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12
	fi	


#############################################################
# Copy Files to S3 For PAC Extract
#############################################################
echo "" >> ${LOGNAME}
echo "Copy Files to S3 For PAC Extract.  " >> ${LOGNAME}

${RUNDIR}PAC_Copy_files_S3.bash ${S3BUCKET} ${TMSTMP}



#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "PAC_Extract.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS
