#!/usr/bin/bash
############################################################################################################
# Script Name: DashboardVolReports.sh
#
# Description: This script will generate three email reports.
#
# Author     : Paul Baranoski	
# Created    : 01/07/2025
#
# Paul Baranoski 2024-01-07 Created script.
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/DashboardVolReports_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "DashboardVolReports.sh started at `date` " >> ${LOGNAME}


#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh 

echo "" >> ${LOGNAME}
echo "DASHBOARD_BUCKET=${DASHBOARD_BUCKET}" >> ${LOGNAME}


#############################################################
# Verify that required NOF parameters have been sent.
#############################################################
if ! [[ $# -eq 1 || $# -eq 2  ]]
then
	echo "Incorrect # of parameters sent to script. NOF parameters: $#" >> ${LOGNAME}
	exit 12
fi


#############################################################
# Display parameters passed to script
# NOTE: RptPeriods: CY, FY, M, CQ, FQ  
#       If parm2 not sent, calculate it
#############################################################
p_RptPeriod=$1
p_OverrideRptYear=$2

echo " " >> ${LOGNAME}
echo "Parameters to script: " >> ${LOGNAME}
echo "NOF parameters for script: " $# >> ${LOGNAME}
echo "  p_RptPeriod=${p_RptPeriod} " >> ${LOGNAME}
echo "  p_OverrideRptYear=${p_OverrideRptYear} " >> ${LOGNAME}


#################################################################################
# Determine Reporting Year for scheduled and on-demand reports
# 
# CY and "01" --> RPT_YYYY = Prior Year YYYY     Ex. CurrentDate=2025-01-08 --> reporting on 2024-01-01 thru 2024-12-31
# FY and "10" --> RPT_YYYY = current date YYYY   Ex. CurrentDate=2024-10-07 --> reporting on 2023-10-01 thru 2024-09-30
# M           --> RPT_MM   = Month report would run; CurrentDate=2025-01-07 --> reporting on 2024-12-01 thru 2024-12-31
#
# Overrider values must follow above rules: 
# Ex: CY --> OverrideDate YYYY = 2025 will report on 2024-01-01 thru 2024-12-31; To report on 2025 data OverrideDate YYYY = 2026
# Ex:  M --> OverrideDate YYYYMM = 202501 will report on 2024-12-01 thru 2024-12-31
#################################################################################
echo "" >> ${LOGNAME}

EXT_TYPE=${p_RptPeriod}
echo "EXT_TYPE=${EXT_TYPE}" >> ${LOGNAME}

# Determine RPT_YYYY based on type of report requested
if [ "${p_OverrideRptYear}" = "" ];then
	CUR_MONTH=`date +%m `  2>> ${LOGNAME}
	CUR_YYYY=`date +%Y `   2>> ${LOGNAME}
	
	echo "CUR_MONTH=${CUR_MONTH} " >> ${LOGNAME}
	echo "CUR_YYYY=${CUR_YYYY} "   >> ${LOGNAME}
	
	if [ ${CUR_MONTH} = "01" -a "${EXT_TYPE}" = "CY" ];then
		LAST_DAY_LAST_YYYY=`date -d "${CUR_YYYY}-01-01 -1 days" +%Y-%m-%d`  2>> ${LOGNAME}
		RPT_YYYY=`echo ${LAST_DAY_LAST_YYYY} | cut -c1-4 `  2>> ${LOGNAME}
	else
		# If monthly processing OR (FY and OCT) --> use RPT_YYYY=CUR_YYYY
		RPT_YYYY=${CUR_YYYY}
		RPT_MM=${CUR_MONTH}
	fi
else
	if [ "${EXT_TYPE}" = "M" ];then
		RPT_YYYY=`echo ${p_OverrideRptYear} | cut -c1-4 `
		RPT_MM=`echo ${p_OverrideRptYear} | cut -c5-6 `
	else
		RPT_YYYY=${p_OverrideRptYear}
	fi	
fi

echo "RPT_YYYY=${RPT_YYYY}" >> ${LOGNAME}
echo "RPT_MM=${RPT_MM}" >> ${LOGNAME}

#################################################################################
# Calculate Report date range and other variables based on Reporting year
#################################################################################
echo "" >> ${LOGNAME}

if [ "${EXT_TYPE}" = "CY" ];then

	EXT_FROM_DT=${RPT_YYYY}-01-01
	EXT_THRU_DT=${RPT_YYYY}-12-31
	RPT_EMAIL_TITLE="Calendar Year ${RPT_YYYY}"
	
	DASHBOARD_VOL_RPT_TXT_FILE=DashboardVolRptData_${RPT_YYYY}_${EXT_TYPE}_${TMSTMP}.txt 
	HTML_RPT_FILE=DashboardVolRptHTML_${RPT_YYYY}_${EXT_TYPE}_${TMSTMP}.txt
	
elif [ "${EXT_TYPE}" = "FY" ];then
	PRIOR_YYYY=`expr ${RPT_YYYY} - 1`
	
	EXT_FROM_DT=${PRIOR_YYYY}-10-01
	EXT_THRU_DT=${RPT_YYYY}-09-30
	RPT_EMAIL_TITLE="Fiscal Year ${RPT_YYYY}"
	
	DASHBOARD_VOL_RPT_TXT_FILE=DashboardVolRptData_${RPT_YYYY}_${EXT_TYPE}_${TMSTMP}.txt 
	HTML_RPT_FILE=DashboardVolRptHTML_${RPT_YYYY}_${EXT_TYPE}_${TMSTMP}.txt
	
elif [ "${EXT_TYPE}" = "M" ];then
	LAST_DAY_LAST_MONTH=`date -d "${RPT_YYYY}-${RPT_MM}-01 -1 days" +%Y-%m-%d`  2>> ${LOGNAME}
	echo "LAST_DAY_LAST_MONTH=${LAST_DAY_LAST_MONTH}" >> ${LOGNAME}

	LAST_MONTH_YYYY=`echo ${LAST_DAY_LAST_MONTH} | cut -d- -f1`  2>> ${LOGNAME}
	LAST_MONTH_MM=`echo ${LAST_DAY_LAST_MONTH} | cut -d- -f2`  2>> ${LOGNAME}
	FIRST_DAY_LAST_MONTH=${LAST_MONTH_YYYY}-${LAST_MONTH_MM}-01
	echo "FIRST_DAY_LAST_MONTH=${FIRST_DAY_LAST_MONTH}" >> ${LOGNAME}
	
	EXT_FROM_DT=${FIRST_DAY_LAST_MONTH}
	EXT_THRU_DT=${LAST_DAY_LAST_MONTH}
	RPT_EMAIL_TITLE="month of ${LAST_MONTH_YYYY}-${LAST_MONTH_MM} "

	DASHBOARD_VOL_RPT_TXT_FILE=DashboardVolRptData_${LAST_MONTH_YYYY}_${LAST_MONTH_MM}_${TMSTMP}.txt 
	HTML_RPT_FILE=DashboardVolRptHTML_${LAST_MONTH_YYYY}_${LAST_MONTH_MM}_${TMSTMP}.txt
	
else
	echo "" >> ${LOGNAME}
	echo "Invalid Report Period ${EXT_TYPE} was passed. Script DashboardVolReports.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="DashboardVolReports.sh - Failed ($ENVNAME)"
	MSG="Invalid Report Period ${EXT_TYPE} was passed. Script DashboardVolReports.sh failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi

echo "EXT_FROM_DT=${EXT_FROM_DT}" >> ${LOGNAME}
echo "EXT_THRU_DT=${EXT_THRU_DT}" >> ${LOGNAME}

echo "" >> ${LOGNAME}
echo "DASHBOARD_VOL_RPT_TXT_FILE=${DASHBOARD_VOL_RPT_TXT_FILE}" >> ${LOGNAME}

DASHBOARD_VOL_RPT_TXT_FILE_ZIP=${DASHBOARD_VOL_RPT_TXT_FILE}.gz 
echo "DASHBOARD_VOL_RPT_TXT_FILE_ZIP=${DASHBOARD_VOL_RPT_TXT_FILE_ZIP}" >> ${LOGNAME} 

DASHBOARD_VOL_RPT_CSV_FILE=`echo ${DASHBOARD_VOL_RPT_TXT_FILE} | sed 's/.txt/.csv/g' `  2>> ${LOGNAME}
echo "DASHBOARD_VOL_RPT_CSV_FILE=${DASHBOARD_VOL_RPT_CSV_FILE}" >> ${LOGNAME} 

echo "HTML_RPT_FILE=${HTML_RPT_FILE}" >> ${LOGNAME}
	
#############################################################
# Export variables for python code
#############################################################		
export EXT_FROM_DT
export EXT_THRU_DT
export DASHBOARD_VOL_RPT_TXT_FILE_ZIP
export EXT_TYPE


#############################################################
# Execute python script  
#############################################################
echo "" >> ${LOGNAME}
echo "Start execution of DashboardVolReports.py program"  >> ${LOGNAME}
${PYTHON_COMMAND} ${RUNDIR}DashboardVolReports.py >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script  
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Python script DashboardVolReports.py failed" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="DashboardVolReports.py - Failed ($ENVNAME)"
	MSG="Python script DashboardVolReports.py failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script DashboardVolReports.py completed successfully. " >> ${LOGNAME}


#############################################################
# Download extract file from S3 to linux data directory  
#############################################################
echo "" >> ${LOGNAME}
echo "Download S3 Extract file to linux data directory " >> ${LOGNAME}

aws s3 cp s3://${DASHBOARD_BUCKET}${DASHBOARD_VOL_RPT_TXT_FILE_ZIP} ${DATADIR}${DASHBOARD_VOL_RPT_TXT_FILE_ZIP}  >> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
echo "" >> ${LOGNAME}
echo "Shell script DashboardVolReports.sh failed." >> ${LOGNAME}

# Send Failure email	
SUBJECT="DashboardVolReports.sh - Failed (${ENVNAME})"
MSG="Copying Dashboard Volume report file DASHBOARD_VOL_RPT_TXT_FILE_ZIP from ${DASHBOARD_BUCKET} from S3 to data directory has failed."
${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

exit 12
fi
	
#############################################################
# Unzip extract file  
#############################################################
echo "" >> ${LOGNAME}
echo "Unzip Report Extract file on data directory " >> ${LOGNAME}

gzip -d ${DATADIR}${DASHBOARD_VOL_RPT_TXT_FILE_ZIP}  2>> ${LOGNAME}


#############################################################
# Move S3 extract file to archive directory 
#############################################################
echo "" >> ${LOGNAME}
echo "Move S3 Report Extract file to s3 archive folder " >> ${LOGNAME}

aws s3 mv s3://${DASHBOARD_BUCKET}${DASHBOARD_VOL_RPT_TXT_FILE_ZIP} s3://${DASHBOARD_BUCKET}archive/${DASHBOARD_VOL_RPT_TXT_FILE_ZIP}  >> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
echo "" >> ${LOGNAME}
echo "Shell script DashboardVolReports.sh failed." >> ${LOGNAME}

# Send Failure email	
SUBJECT="DashboardVolReports.sh - Failed (${ENVNAME})"
MSG="Moving Dashboard Volume Report file DASHBOARD_VOL_RPT_TXT_FILE_ZIP to ${DASHBOARD_BUCKET} archive folder has failed."
${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

exit 12
fi


#############################################################
# Build HTML for report 
#############################################################	
echo "" >> ${LOGNAME}
echo "Build HTML report " >> ${LOGNAME}

#################################################################################
# Write out HTML header.
#################################################################################
echo "<html><body><table cellspacing='1px' border='1' > " >> ${DATADIR}${HTML_RPT_FILE}

#################################################################################
# Loop thru records.
#################################################################################
bFirstRec=Y

while read extractRec
do
	echo "" >> ${LOGNAME}
	echo "extractRec=${extractRec}"  >> ${LOGNAME}

	#######################################
	# set tag type
	#######################################
	if [ "${bFirstRec}" == "Y" ];then
		bFirstRec=N
		fldTag=th
		
		echo "<tr bgcolor='#00B0F0'>"  >> ${DATADIR}${HTML_RPT_FILE}
	else
		fldTag=td

		echo -n "<tr>" >> ${DATADIR}${HTML_RPT_FILE}

	fi


	#######################################
	# create array of select fields
	#######################################
	IFS='|' FIELDS_ARRAY=(${extractRec})
	#echo "NOF of flds in array: ${#FIELDS_ARRAY[@]}"  >> ${LOGNAME}

	#######################################
	# Loop thru fields in record
	#######################################
	colNum=0
	for fld in "${FIELDS_ARRAY[@]}"
	do
		((++colNum))
		if [ ${colNum} -le 2 ];then
			echo -n "<${fldTag}>${fld}</${fldTag}>"  >> ${DATADIR}${HTML_RPT_FILE}
		else
			echo -n "<${fldTag} align='right'>${fld}</${fldTag}>"  >> ${DATADIR}${HTML_RPT_FILE}
		fi	

	done

	echo "</tr>" >> ${DATADIR}${HTML_RPT_FILE}

done  <  ${DATADIR}${DASHBOARD_VOL_RPT_TXT_FILE}			
		

#################################################################################
# Write out HTML trailer.
#################################################################################
echo "</table>" >> ${DATADIR}${HTML_RPT_FILE}
echo "</body></html>" >> ${DATADIR}${HTML_RPT_FILE}


#############################################################
# Create CSV version of file from pipe-delimited file
#############################################################
echo "" >> ${LOGNAME}
echo "Create CSV version of report file" >> ${LOGNAME}

${PYTHON_COMMAND} ${RUNDIR}utilConvertPipeFile2CSVFile.py "${DATADIR}${DASHBOARD_VOL_RPT_TXT_FILE}"  >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Error in calling sendEmail.py" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Error converting pipe delimited file to csv file in DashboardVolReports.sh (${ENVNAME})"
	MSG="Error converting pipe delimited file to csv file in DashboardVolReports.sh."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


#############################################################
# Email report with CSV attachment
#############################################################
RPT_INFO=`cat ${DATADIR}${HTML_RPT_FILE} `

echo "" >> ${LOGNAME}
echo "Send success email" >> ${LOGNAME}
echo "RPT_INFO=${RPT_INFO} "   >> ${LOGNAME}

SUBJECT="Dashboard report for ${RPT_EMAIL_TITLE} (${ENVNAME})"
MSG="Dashboard report for ${RPT_EMAIL_TITLE}. . .<br><br>${RPT_INFO}"
${PYTHON_COMMAND} ${RUNDIR}sendEmailHTML.py "${CMS_EMAIL_SENDER}" "${DASHBOARD_RPT_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" "${DATADIR}${DASHBOARD_VOL_RPT_CSV_FILE}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Error in calling sendEmail.py" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Sending Success email in DashboardVolReports.sh - Failed (${ENVNAME})"
	MSG="Sending Success email in DashboardVolReports.sh has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME} 
echo "Remove report files from data directory" >> ${LOGNAME} 

rm ${DATADIR}${DASHBOARD_VOL_RPT_TXT_FILE} >> ${LOGNAME} 2>&1
rm ${DATADIR}${DASHBOARD_VOL_RPT_CSV_FILE} >> ${LOGNAME} 2>&1
rm ${DATADIR}${HTML_RPT_FILE} >> ${LOGNAME} 2>&1	


#############################################################
# end script
#############################################################
echo "" >> ${LOGNAME}
echo "DashboardVolReports.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit 0