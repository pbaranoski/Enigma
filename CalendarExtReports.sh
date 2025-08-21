#!/usr/bin/bash
############################################################################################################
# Script Name: CalendarExtReports.sh
#
# Description: This script will generate three email reports.
#
# Author     : Paul Baranoski	
# Created    : 02/08/2024
#
# Paul Baranoski 2024-02-08 Created script.
# Paul Baranoski 2024-02-21 Modify logic to find TIMEFRAME_IND in fld 10 instead of 9 (yellow line logic).
# Paul Baranoski 2024-02-26 Add "Legend" for defining abbreviations like "FW".
# Paul Baranoski 2024-03-11 Add support for automated column. Change TIMEFRAME_IND to be fld 11 instead of 10. 
# Paul Baranoski 2024-06-05 Modify success email to use ENIGMA_EMAIL_SUCCESS_RECIPIENT.
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/CalendarExtReports_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "CalendarExtReports.sh started at `date` " >> ${LOGNAME}


#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh 

echo "" >> ${LOGNAME}
echo "CALENDAR_BUCKET=${CALENDAR_BUCKET}" >> ${LOGNAME}


#############################################################
# Display parameters passed to script
# NOTE: RptPeriods is a comma-delimited parameter 
# NOTE: p_overrideStartDt is optional.
#############################################################
p_rptperiods=$1
p_overrideStartDt=$2

echo " " >> ${LOGNAME}
echo "Parameters to script: " >> ${LOGNAME}
echo "NOF parameters for script: " $# >> ${LOGNAME}
echo "   p_rptperiods=${p_rptperiods} " >> ${LOGNAME}
echo "   p_overrideStartDt=${p_overrideStartDt} " >> ${LOGNAME}


#################################################################################
# Create array of NOF DAYS reporting periods  
#################################################################################
echo "" >> ${LOGNAME}
echo "Create array of reporting periods" >> ${LOGNAME}

# convert comma delimiters to spaces.
RPT_PERIODS=`echo ${p_rptperiods} |  sed 's/,/ /g'`

RPT_PERIODS_ARRAY=(${RPT_PERIODS})

echo "NOF of reporting periods in array: ${#RPT_PERIODS_ARRAY[@]}"  >> ${LOGNAME}
echo "Reporting periods: ${RPT_PERIODS_ARRAY[@]}"  >> ${LOGNAME}


#################################################################################
# Set begStartRptDt. Use current date by default. Otherwise, use override date.  
#################################################################################
if [ "${p_overrideStartDt:-""}" == "" ];then
	begStartRptDt=`date +%Y-%m-%d`
	echo "no override date" >> ${LOGNAME}
else
	echo "override date" >> ${LOGNAME}
	begStartRptDt=${p_overrideStartDt}

	# validate override parameter date
	date "+%Y-%m-%d" -d "${begStartRptDt}" > /dev/null 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Invalid override parameter date ${p_overrideStartDt} was passed. Script CalendarExtReports.sh failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="CalendarExtReports.sh - Failed ($ENVNAME)"
		MSG="Invalid override parameter date ${p_overrideStartDt} was passed. Script CalendarExtReports.sh failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi
	
fi	


#################################################################################
# Loop thru NOF DAYS reporting periods  
#################################################################################
for (( idx=0 ; idx < ${#RPT_PERIODS_ARRAY[@]}; idx++ ))
do

	echo "" >> ${LOGNAME}
	echo "*-----------------------------------*" >> ${LOGNAME}
	
	#############################################################
	# Get NOF days to report on; Build Reporting date range.
	#############################################################	
	NOF_DAYS=${RPT_PERIODS_ARRAY[idx]}	

	EXT_FROM_DT=${begStartRptDt} 	
	EXT_TO_DT=`date -d "${begStartRptDt} ${NOF_DAYS} days" +%Y-%m-%d`   2>> ${LOGNAME} 	
	echo "EXT_FROM_DT=${EXT_FROM_DT}" >> ${LOGNAME} 

	CALENDAR_EXTRACT_RPT_FILE=CalendarExtRptData_${NOF_DAYS}Days_${TMSTMP}.txt 
	CALENDAR_EXTRACT_RPT_FILE_ZIP=${CALENDAR_EXTRACT_RPT_FILE}.gz 

	echo "CALENDAR_EXTRACT_RPT_FILE=${CALENDAR_EXTRACT_RPT_FILE}" >> ${LOGNAME} 
	echo "CALENDAR_EXTRACT_RPT_FILE_ZIP=${CALENDAR_EXTRACT_RPT_FILE_ZIP}" >> ${LOGNAME} 

	HTML_RPT=CalendarHTMLReport_${NOF_DAYS}_${TMSTMP}.txt
	
	#############################################################
	# Export variables for python code
	#############################################################		
	export EXT_FROM_DT
	export EXT_TO_DT
	export CALENDAR_EXTRACT_RPT_FILE_ZIP

	#############################################################
	# Execute python script  
	#############################################################
	echo "" >> ${LOGNAME}
	echo "Start execution of CalendarExtReports.py program"  >> ${LOGNAME}
	${PYTHON_COMMAND} ${RUNDIR}CalendarExtReports.py >> ${LOGNAME} 2>&1


	#############################################################
	# Check the status of python script  
	#############################################################
	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Python script CalendarExtReports.py failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="CalendarExtReports.py - Failed ($ENVNAME)"
		MSG="Python script CalendarExtReports.py failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi

	echo "" >> ${LOGNAME}
	echo "Python script CalendarExtReports.py completed successfully. " >> ${LOGNAME}


	#############################################################
	# Download extract file from S3 to linux data directory  
	#############################################################
	echo "" >> ${LOGNAME}
	echo "Download S3 Extract file to linux data directory " >> ${LOGNAME}
	
	aws s3 cp s3://${CALENDAR_BUCKET}${CALENDAR_EXTRACT_RPT_FILE_ZIP} ${DATADIR}${CALENDAR_EXTRACT_RPT_FILE_ZIP}  >> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Shell script CalendarExtReports.sh failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="CalendarExtReports.sh - Failed (${ENVNAME})"
		MSG="Copying calendar extract file CALENDAR_EXTRACT_RPT_FILE_ZIP from ${CALENDAR_BUCKET} from S3 to data directory has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
	fi
			
	#############################################################
	# Unzip extract file  
	#############################################################
	echo "" >> ${LOGNAME}
	echo "Unzip Report Extract file on data directory " >> ${LOGNAME}
	
	gzip -d ${DATADIR}${CALENDAR_EXTRACT_RPT_FILE_ZIP}  2>> ${LOGNAME}

	
	#############################################################
	# Move S3 extract file to archive directory 
	#############################################################
	echo "" >> ${LOGNAME}
	echo "Move S3 Report Extract file to s3 archive folder " >> ${LOGNAME}
	
	aws s3 mv s3://${CALENDAR_BUCKET}${CALENDAR_EXTRACT_RPT_FILE_ZIP} s3://${CALENDAR_BUCKET}archive/${CALENDAR_EXTRACT_RPT_FILE_ZIP}  >> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Shell script CalendarExtReports.sh failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="CalendarExtReports.sh - Failed (${ENVNAME})"
		MSG="Moving calendar extract file CALENDAR_EXTRACT_RPT_FILE_ZIP to ${CALENDAR_BUCKET} archive folder has failed."
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
	echo "<html><body><table cellspacing='1px' border='1' > " >> ${DATADIR}${HTML_RPT}

	#################################################################################
	# Loop thru list.
	# Field #9 from results-set = TIMEFRAME_IND
	#################################################################################
	bFirstRec=Y

	while read extractRec
	do
		echo "" >> ${LOGNAME}
		echo "extractRec=${extractRec}"  >> ${LOGNAME}
		
		# add ending pipe delimiter
		extractRec="${extractRec}|"
		TIMEFRAME_IND=`echo "${extractRec}" | cut -d"|" -f11 `
		echo "TIMEFRAME_IND=${TIMEFRAME_IND} "  >> ${LOGNAME}

		#######################################
		# set tag type
		#######################################
		if [ "${bFirstRec}" == "Y" ];then
			bFirstRec=N
			fldTag=th
			
			echo "<tr bgcolor='#00B0F0'>"  >> ${DATADIR}${HTML_RPT}
		else
			fldTag=td
			
			if [ "${TIMEFRAME_IND}" == "W" ];then
				echo -n "<tr>" >> ${DATADIR}${HTML_RPT}
			else
				# highlight non-weekly extracts in yellow to stand out to the eye
				echo -n "<tr bgcolor='#FFFF00'>" >> ${DATADIR}${HTML_RPT}
			fi

		fi

		
		#######################################
		# create array of select fields
		#######################################
		IFS='|' FIELDS_ARRAY=(${extractRec})
		echo "NOF of flds in array: ${#FIELDS_ARRAY[@]}"  >> ${LOGNAME}

		#######################################
		# Loop thru fields in record
		#######################################
		for fld in "${FIELDS_ARRAY[@]}"
		do
			echo -n "<${fldTag}>${fld}</${fldTag}>"  >> ${DATADIR}${HTML_RPT}
		done

		echo "</tr>" >> ${DATADIR}${HTML_RPT}
			
		
	done  <  ${DATADIR}${CALENDAR_EXTRACT_RPT_FILE}

	#################################################################################
	# Write out HTML trailer.
	#################################################################################
	echo "</table>" >> ${DATADIR}${HTML_RPT}
	echo "<p>Legend: (W)eekly; (M)onthly; (Q)uarterly; (S)emi-Annual; (A)nnual; MF=Mainframe; FW=First Working Day; LW=Last Working Day</p>" >> ${DATADIR}${HTML_RPT}
    echo "</body></html>" >> ${DATADIR}${HTML_RPT}

	#############################################################
	# Email report 
	#############################################################
	RPT_INFO=`cat ${DATADIR}${HTML_RPT} `

	echo "" >> ${LOGNAME}
	echo "Send success email" >> ${LOGNAME}
	echo "RPT_INFO=${RPT_INFO} "   >> ${LOGNAME}

	SUBJECT="Pending Extracts in the next ${NOF_DAYS} days Report (${ENVNAME})"
	MSG="Pending Extracts in the next ${NOF_DAYS} days from ${begStartRptDt}. . .<br><br>${RPT_INFO}"
	${PYTHON_COMMAND} ${RUNDIR}sendEmailHTML.py "${CMS_EMAIL_SENDER}" "${CALENDAR_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1


	#############################################################
	# loop clean-up
	#############################################################
	echo "" >> ${LOGNAME} 
	echo "Remove temporary work files from data directory" >> ${LOGNAME} 

	rm ${DATADIR}${CALENDAR_EXTRACT_RPT_FILE} >> ${LOGNAME} 2>&1
	rm ${DATADIR}${HTML_RPT} >> ${LOGNAME} 2>&1	


done


#############################################################
# script clean-up
#############################################################


#############################################################
# end script
#############################################################
echo "" >> ${LOGNAME}
echo "CalendarExtReports.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit 0