#!/usr/bin/bash
#
########################################################################
# Name: PartABExtract.sh
#
# Desc:
#
# Created: Sean Whitelock
# Modified:
#
#########################################################################
set +x

####################################################
# Establish log file
####################################################
TMSTMP=${TMSTMP=`date +%Y%m%d.%H%M%S`}

# Export TMSTMP variable
export TMSTMP
LOGNAME=/app/IDRC/XTR/CMS/logs/Part_AB_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME}

echo "##################################### " >> ${LOGNAME}
echo "Part_AB.sh started at 'date' " >> ${LOGNAME}
echo "" >> ${LOGNAME}

####################################################
# SET DATABASE NAMES VARIABLES
####################################################
source ${RUNDIR}SET_XTR_ENV.sh >> ${LOGNAME}

LOGDIR=${LOG_PATH}/




####################################################
# Calculate Date Variables
####################################################
CUR_DT=$(date +%Y-%m-%d)
CUR_MNTH=$(date +%m)
CUR_YR=$(date +%Y)
CUR_TM=$(date +%H:%M:%S)

if [ "$CUR_MNTH" -ge 11 ]; then
    YEAR="${CUR_YR}-10-01"
    ENDMONTH="$(date -d "$(date -d "$CUR_DT -1 month" +%Y-%m-01) +1 month -1 day" +%Y-%m-%d)"
    BEGMONTH="$(date -d "$CUR_DT -1 month" +%Y-%m-01)"
    RPTMTH="$(date +%m/%d/%Y)"
    MONRPTMTH=$(date -d "$CUR_DT -1 month" +%B)
    FFRPTYEAR=$((CUR_YR + 1))
else 
    YEAR="$((CUR_YR - 1))-10-01"
    ENDMONTH="$(date -d "$(date -d "$CUR_DT -1 month" +%Y-%m-01) +1 month -1 day" +%Y-%m-%d)"
    BEGMONTH="$(date -d "$CUR_DT -1 month" +%Y-%m-01)"
    RPTMTH="$(date +%m/%d/%Y)"
    MONRPTMTH=$(date -d "$CUR_DT -1 month" +%B)
    FFRPTYEAR=$CUR_YR
fi


DATETIME="$(date +%m%d%y)_$(date +%H%M%S)"
MNTH=$(date -d "$ENDMONTH" +%m)
RPTYEAR=$(date -d "$ENDMONTH" +%Y)
Sheet1="YTD$FFRPTYEAR"
Sheet2="${RPTYEAR}${MNTH}"
TIME=$DATETIME

YEAR_FILE_ZIP="PartAB_Year_${TMSTMP}.txt.gz"
MONTH_FILE_ZIP="PartAB_Month_${TMSTMP}.txt.gz"


# Export variables needed by Python
export YEAR
export BEGMONTH
export ENDMONTH
export FISCAL_YEAR_START=$YEAR  # if you want FISCAL_YEAR_START to match YEAR
export TMSTMP
export YEAR_FILE_ZIP
export MONTH_FILE_ZIP


echo "Reporting Month=$MNTH" >> ${LOGNAME}
echo "Report Run Date=$RPTMTH" >> ${LOGNAME}
echo "YTD_Year=$YEAR" >> ${LOGNAME}
echo "BegMonth=$BEGMONTH" >> ${LOGNAME}
echo "EndMonth=$ENDMONTH" >> ${LOGNAME}
echo "Federal Fiscal Year=$FFRPTYEAR" >> ${LOGNAME}
echo "Sheet1=YTD$FFRPTYEAR" >> ${LOGNAME}
echo "Sheet2=${RPTYEAR}${MNTH}" >> ${LOGNAME}


############################################################################################################
# Run Python Report Script
############################################################################################################
RUNDIR="/app/IDRC/XTR/CMS/scripts/run/"  

${PYTHON_COMMAND} ${RUNDIR}PartABExtract.py >> ${LOGNAME} 2>&1
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
  echo "PartABExtract.py failed" >> ${LOGNAME}
  exit 12
else
  echo "PartABExtract.py completed successfully" >> ${LOGNAME}
fi

echo "PartABExtract.sh completed at $(date)" >> ${LOGNAME}



####################################################
# Check the status of script
####################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0  ]]; then
        echo "" >> ${LOGNAME}
        echo "Shell script PartABExtract.sh failed." >> ${LOGNAME}

        # Send Failure Email
        SUBJECT="Shell script PartABExtract.sh - Failed ($ENVNAME)"
        MSG="Shell script PartABExtract.sh failed"
        ${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${Part_AB_EMAIL_SENDER}" "${Part_AB_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>$1

        exit 12
fi


#############################################################
# Download extract files from S3 to linux data directory  
#############################################################
echo "" >> ${LOGNAME}
echo "Download S3 Extract files to linux data directory " >> ${LOGNAME}

# Download YEAR file
aws s3 cp s3://${PARTAB_BUCKET}${YEAR_FILE_ZIP} ${DATADIR}${YEAR_FILE_ZIP} >> ${LOGNAME} 2>&1
YEAR_RET=$?

# Download MONTH file
aws s3 cp s3://${PARTAB_BUCKET}${MONTH_FILE_ZIP} ${DATADIR}${MONTH_FILE_ZIP} >> ${LOGNAME} 2>&1
MONTH_RET=$?

if [[ $YEAR_RET != 0 || $MONTH_RET != 0 ]]; then
  echo "" >> ${LOGNAME}
  echo "Shell script PartABExtract.sh failed." >> ${LOGNAME}

  # Send Failure Email
  MSG=""

  if [[ $YEAR_RET != 0 ]]; then
    MSG+="Failed to copy YEAR file: ${YEAR_FILE_ZIP} from bucket ${PARTAB_BUCKET}. "
  fi

  if [[ $MONTH_RET != 0 ]]; then
    MSG+="Failed to copy MONTH file: ${MONTH_FILE_ZIP} from bucket ${PARTAB_BUCKET}. "
  fi

  SUBJECT="PartABExtract.sh - Failed (${ENVNAME})"

  ${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

  exit 12
fi

echo "S3 downloads completed successfully." >> ${LOGNAME}


############################################################################################################
# Unzip YEAR and MONTH files
############################################################################################################
echo "" >> ${LOGNAME}
echo "Unzip Extract files on data directory " >> ${LOGNAME}

gzip -d ${DATADIR}${YEAR_FILE_ZIP} >> ${LOGNAME} 2>&1
gzip -d ${DATADIR}${MONTH_FILE_ZIP} >> ${LOGNAME} 2>&1

YEAR_TXT_FILE=$(echo ${YEAR_FILE_ZIP} | sed 's/.gz$//')
MONTH_TXT_FILE=$(echo ${MONTH_FILE_ZIP} | sed 's/.gz$//')


#############################################################
# Move S3 Year and Month extract files to archive directory
#############################################################
echo "" >> ${LOGNAME}
echo "Move PartAB Year and Month extract files to S3 archive folder" >> ${LOGNAME}

# Move Year file
aws s3 mv s3://${PARTAB_BUCKET}${YEAR_FILE_ZIP} s3://${PARTAB_BUCKET}archive/${YEAR_FILE_ZIP} >> ${LOGNAME} 2>&1
YEAR_RET=$?

# Move Month file
aws s3 mv s3://${PARTAB_BUCKET}${MONTH_FILE_ZIP} s3://${PARTAB_BUCKET}archive/${MONTH_FILE_ZIP} >> ${LOGNAME} 2>&1
MONTH_RET=$?

# Check status of both moves
if [[ $YEAR_RET != 0 || $MONTH_RET != 0 ]]; then
    echo "" >> ${LOGNAME}
    echo "Shell script PartABReports.sh failed moving S3 extract files to archive." >> ${LOGNAME}

    # Build failure message
    MSG=""

    if [[ $YEAR_RET != 0 ]]; then
        MSG+="Failed to move YEAR file: ${YEAR_FILE_ZIP} to S3 archive folder. "
    fi

    if [[ $MONTH_RET != 0 ]]; then
        MSG+="Failed to move MONTH file: ${MONTH_FILE_ZIP} to S3 archive folder. "
    fi

    SUBJECT="PartABReports.sh - Failed moving S3 extract files (${ENVNAME})"

    ${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

    exit 12
fi

# Success log
echo "Successfully moved PartAB Year and Month extract files to S3 archive folder." >> ${LOGNAME}



############################################################################################################
# Build HTML Reports
############################################################################################################
HTML_YEAR_FILE=PartAB_Year_Report_${TMSTMP}.html
HTML_MONTH_FILE=PartAB_Month_Report_${TMSTMP}.html

echo "Building HTML report: ${OUTPUT_FILE} from ${INPUT_FILE}" >> ${LOGNAME}

build_html_report() {
  local INPUT_FILE=$1
  local OUTPUT_FILE=$2

  echo "<html><body><table cellspacing='1px' border='1'>" > ${DATADIR}${OUTPUT_FILE}
  local firstRecord=Y

  while read line; do
    # Add pipe to catch trailing blank column
    line="${line}|"
    IFS='|' read -ra FIELDS <<< "$line"

    if [ "${firstRecord}" == "Y" ]; then
      echo "<tr bgcolor='#C5D9F1'>" >> ${DATADIR}${OUTPUT_FILE}
      TAG=th
      firstRecord=N
    else
      echo "<tr>" >> ${DATADIR}${OUTPUT_FILE}
      TAG=td
    fi

    colNum=0

##*****\/****
    for fld in "${FIELDS[@]}"; do
      ((++colNum))

		if [ "${firstRecord}" == "Y" ]; then
			# Regular columns
			echo -n "<${TAG} style='font-family:Arial;font-size:8pt;font-weight:bold;'>${fld}</${TAG}>" >> ${DATADIR}${OUTPUT_FILE}
		else
			# Apply numeric formatting to currency fields (columns 4–9)
			if [[ ${colNum} -ge 4 && ${colNum} -le 9 ]]; then
				formatted=$(echo "${fld}" | awk '{printf "%\047.2f", $1}')
				echo -n "<${TAG} style='font-family:Arial;font-size:8pt;font-weight:bold;' align='right'>${formatted}</${TAG}>" >> ${DATADIR}${OUTPUT_FILE}
			else
				# Regular columns
				echo -n "<${TAG} style='font-family:Arial;font-size:8pt;font-weight:bold;'>${fld}</${TAG}>" >> ${DATADIR}${OUTPUT_FILE}
			fi
		fi
##*****/\****
    done

    echo "</tr>" >> ${DATADIR}${OUTPUT_FILE}
  done < ${DATADIR}${INPUT_FILE}

  echo "</table></body></html>" >> ${DATADIR}${OUTPUT_FILE}
}
build_html_report ${YEAR_TXT_FILE} ${HTML_YEAR_FILE}
build_html_report ${MONTH_TXT_FILE} ${HTML_MONTH_FILE}


#############################################################
# Create CSV version of file from pipe-delimited file
#############################################################
echo "" >> ${LOGNAME}
echo "Create CSV version of report file" >> ${LOGNAME}

# Convert YEAR file
tr '|' ',' < ${DATADIR}${YEAR_TXT_FILE} > ${DATADIR}PartAB_Year_${TMSTMP}.csv
RET_STATUS=$?
if [[ $RET_STATUS != 0 ]]; then
  echo "" >> ${LOGNAME}
  echo "Error converting YEAR file to CSV" >> ${LOGNAME}
  SUBJECT="Error converting YEAR report to CSV in PartABExtract.sh (${ENVNAME})"
  MSG="Error converting ${YEAR_TXT_FILE} to CSV format."
  ${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
  exit 12
fi

# Convert MONTH file
tr '|' ',' < ${DATADIR}${MONTH_TXT_FILE} > ${DATADIR}PartAB_Month_${TMSTMP}.csv
RET_STATUS=$?
if [[ $RET_STATUS != 0 ]]; then
  echo "" >> ${LOGNAME}
  echo "Error converting MONTH file to CSV" >> ${LOGNAME}
  SUBJECT="Error converting MONTH report to CSV in PartABExtract.sh (${ENVNAME})"
  MSG="Error converting ${MONTH_TXT_FILE} to CSV format."
  ${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
  exit 12
fi

#############################################################
# Email report with CSV attachment
#############################################################
echo "" >> ${LOGNAME}
echo "Attach CSVs to Email and send" >> ${LOGNAME}

# Load HTML contents for email body
RPT_YEAR_HTML=$(cat ${DATADIR}${HTML_YEAR_FILE})
RPT_MONTH_HTML=$(cat ${DATADIR}${HTML_MONTH_FILE})

SUBJECT="PartAB Report - ${ENVNAME}"
MSG="<p>Medicare Part A/B Payment Report</p><br><b>YTD Report:</b><br>${RPT_YEAR_HTML}<br><br><b>Monthly Report:</b><br>${RPT_MONTH_HTML}"

# Attach both CSVs and send HTML in body
${PYTHON_COMMAND} ${RUNDIR}sendEmailHTML.py "${CMS_EMAIL_SENDER}" "${PART_AB_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" "${DATADIR}PartAB_Year_${TMSTMP}.csv,${DATADIR}PartAB_Month_${TMSTMP}.csv" >> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
  echo "" >> ${LOGNAME}
  echo "Error in calling sendEmailHTML.py" >> ${LOGNAME}
  SUBJECT="Sending success email in PartABExtract.sh - Failed (${ENVNAME})"
  MSG="Failed to send success email for PartAB reports (CSV and HTML)."
  ${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
  exit 12
fi
exit 0

############################################################################################################
# Script Clean-up
############################################################################################################
echo "" >> ${LOGNAME}
echo "Clean up temporary report files from data directory" >> ${LOGNAME}

rm -f ${DATADIR}${YEAR_TXT_FILE}
rm -f ${DATADIR}${MONTH_TXT_FILE}
rm -f ${DATADIR}${HTML_YEAR_FILE}
rm -f ${DATADIR}${HTML_MONTH_FILE}
rm -f ${DATADIR}PartAB_Year_${TMSTMP}.csv
rm -f ${DATADIR}PartAB_Month_${TMSTMP}.csv

############################################################################################################
# End Script
############################################################################################################
echo "" >> ${LOGNAME}
echo "PartABExtract.sh completed successfully at $(date)" >> ${LOGNAME}
exit 0
