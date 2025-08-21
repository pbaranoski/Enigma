#!/usr/bin/bash
############################################################################################################
#
# !!!!! NOTE: This script is obsolete.
#
# Script Name: DashboardInfo.sh
#
# Description: This script will extract dashboard info from extract scripts.
#
#  ./DashboardInfo.sh $1
#  $1 --> NOF_DAYS (how far back to look at log files
# 
#
# Author     : Paul Baranoski	
# Created    : 10/24/2023
#
# Paul Baranoski 2023-10-24 Created script.
# Paul Baranoski 2024-03-08 Add additional scripts to ignore for processing. 
#                           Change logic for extract list of log files from "NOF Days ago" to a date range
#                           in format 'YYYYMMDD'
# Paul Baranoski 2024-03-11 Add additional logs to omit.
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/DashboardInfo_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

TMP_FILE=tmpDashboardLogFiles.txt
TMP_FILE1=tmpDashboardLogFiles1.txt
TMP_FILE2=tmpDashboardLogFiles2.txt
TMP_FILE3=tmpDashboardLogFiles3.txt
TMP_FILE4=tmpDashboardLogFiles4.txt

touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "DashboardInfo.sh started at `date` " >> ${LOGNAME}


##################################################################
# Extract can run stand-alone or as a called script.
##################################################################
if ! [[ $# -eq 2 || $# -eq 0 ]]
then
	echo "" >> ${LOGNAME}
	echo "Incorrect # of parameters sent to script. NOF parameters: $#" >> ${LOGNAME}
	exit 12
fi


##################################################################
# Extract log information for yesteray unless 
#   overriding with date range   
##################################################################
if [[ $# -eq 2 ]];then
	echo " " >> ${LOGNAME}
	echo "Using override dates " >> ${LOGNAME}

	FIND_START_DT=$1
	FIND_END_DT=$2
else
	echo " " >> ${LOGNAME}
	echo "Using script calculated dates " >> ${LOGNAME}
	
	# get yesterday's date
	FIND_START_DT=`date -d "-1 day" +%Y%m%d` 
	FIND_END_DT=${FIND_START_DT}
fi


#############################################################
# Display parameters passed to script 
#############################################################
echo " " >> ${LOGNAME}
echo "Parameters to script: " >> ${LOGNAME}
echo "   FIND_START_DT=${FIND_START_DT} " >> ${LOGNAME}
echo "   FIND_END_DT=${FIND_END_DT} " >> ${LOGNAME}
	
#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh 

echo "" >> ${LOGNAME}
echo "CONFIG_BUCKET=${CONFIG_BUCKET}" >> ${LOGNAME}


#############################################################
# function definitions  
#############################################################
createJobInfoKeyValuePairs() {

	# $1 = Y/N (Job success/failure)
	echo "" >> ${LOGNAME}
	echo "In function createJobInfoKeyValuePairs" >> ${LOGNAME}

	############################################################	
	# Create Key/value pairs for Job Info
	############################################################
	#blbtn_clm_ext_20231020.134153.log, Fri Oct 20 13:41:53 EDT 2023,Fri Oct 20 13:42:05 EDT 2023
	#OFM_PDE_Extract_20231018.163447.log, Wed Oct 18 16:34:47 EDT 2023,Wed Oct 18 16:56:11 EDT 2023	

	echo "" >> ${LOGNAME}
	echo "Parse for Key Values" >> ${LOGNAME}
	Logfilename=`basename ${LogfileNPath} | sed "s/^[ ]*//g" `  >> ${LOGNAME}
	echo "Logfilename=${Logfilename}"  >> ${LOGNAME}

	# Parse for Extract name and Run Timestamp	
	FLD_POS_EXT=`echo ${Logfilename} | grep -bo "_" | wc -l `
	FLD_POS_TMSTMP=`expr ${FLD_POS_EXT} + 1 ` 2>> ${LOGNAME}
	
	ext_name=`echo ${Logfilename} | cut -d_ -f1-${FLD_POS_EXT} `
	
	# remove verbiage "Driver" and "Extract"
	ext_name=`echo ${ext_name} | sed "s/_Driver//g" `    >> ${LOGNAME}
	ext_name=`echo ${ext_name} | sed "s/_Extract//g" `   >> ${LOGNAME}
	ext_name=`echo ${ext_name} | sed "s/_EXTRACT//g" `   >> ${LOGNAME}
	echo "ext_name=${ext_name}" >> ${LOGNAME}
	
	runTmpstmp=`echo ${Logfilename} | cut -d_ -f${FLD_POS_TMSTMP}- | sed "s/.log//" `   >> ${LOGNAME}
	echo "runTmpstmp=${runTmpstmp}" >> ${LOGNAME}
	
	runDate=`echo ${runTmpstmp} | cut -d. -f1 `
	echo "runDate=${runDate}" >> ${LOGNAME}

	# Set script success/failure
	JobSuccess=$1
	echo "JobSuccess=${JobSuccess}" >> ${LOGNAME}

	################################	
	## Write Extract info record
	################################	
	echo "log=${Logfilename} ext=${ext_name} runDate=${runDate} runTmstmp=${runTmpstmp} success=${JobSuccess} "  >> ${DATADIR}DASHBOARD_JOB_INFO.txt

}


#################################################################################
# Get list of Log Files that are between START_DT and END_DT
#
# NOTE: Ignore logs for utility scripts, load finder file scripts, python database logs, and support processing logs.
#       Also, ignore certin application child logs. 
#
#################################################################################
echo "Change to logs directory "  >> ${LOGNAME}
cd ${LOGS}
echo `pwd` >> ${LOGNAME}

echo "" >> ${LOGNAME}
echo "Get list of log files to process "  >> ${LOGNAME}

# find .-name -mtime +2 -a -mtime -8 -ls    -->  older than 2 days but not older than 8 days.
# find . -name "*.log" -mtime -${DAYS_OLD}  -->  all files not older than ${DAYS_OLD}
# find . -name "*.log" -newermt 20240301 \! -newermt 20240309 ## Run every day even Sat/Sun, and run for prior day

# Extract initial list of log files to process into temp file - Ignore logs for utility scripts, load finder file scripts, python database logs
find . -name "*.log" -newermt ${FIND_START_DT} \! -newermt ${FIND_END_DT} | grep -v "CombineS3Files" | grep -v "CreateManifestFile" | grep -v "ProcessFiles2EFT" | grep -v "_SF." | grep -v "LOAD_*"  > ${DATADIR}${TMP_FILE1}

# Remove support scripts
cat ${DATADIR}${TMP_FILE1} | grep -v "DashboardInfo" | grep -v "BuildRunExtCalendar" | grep -v "KIA" | grep -v "ListXTRProcess" >  ${DATADIR}${TMP_FILE2}

# Remove reporting log files
cat ${DATADIR}${TMP_FILE2} | grep -v "ManifestFile" | grep -v "FinderFiles" | grep -v "CalendarExtReports" >  ${DATADIR}${TMP_FILE3}

# Remove child logs from list of log files to process
cat ${DATADIR}${TMP_FILE3} | grep -v "DemoFinderFilePrep" | grep -v "DEMOFNDR_PT"  >  ${DATADIR}${TMP_FILE4}

# Remove Driver logs from list of log files to process
# Need to NOT exclude OPMHI_*_Driver.sh
cat ${DATADIR}${TMP_FILE4} | grep -v "_Driver_" >  ${DATADIR}${TMP_FILE}

# Display list of log files that we will process
LOG_FILES_2_PROCESS=`cat ${DATADIR}${TMP_FILE} `
echo LOG_FILES_2_PROCESS="${LOG_FILES_2_PROCESS}"   >> ${LOGNAME}


#################################################################################
# Loop thru list.
#################################################################################
while read LogfileNPath
do

	echo "" >> ${LOGNAME}
	echo "*********************" >> ${LOGNAME}
	
	echo "LogfileNPath=${LogfileNPath}"  >> ${LOGNAME}
	
	################################
	## Get Start Time for script
	################################
	# Example "started at Mon Aug  7 15:22:12 EDT 2023"
	JobStartLine=`head -n 3 ${LogfileNPath} | grep "started at" ` 
	echo "JobStartLine=${JobStartLine} " >> ${LOGNAME}
	
	offset=`echo ${JobStartLine} | grep -bo " at" | cut -d: -f1 `
	echo "offset=${offset}"  >> ${LOGNAME}
	offset=`expr ${offset} + 4 `    2>> ${LOGNAME}

	JobStartTime=`echo ${JobStartLine} | cut -c${offset}- `
	echo "JobStartTime=${JobStartTime}" >> ${LOGNAME}
	
	################################	
	# Get End Time for script
	################################	
	# Example: "Ended at Mon Aug  7 15:23:01 EDT 2023"
	JobEndLine=`tail -n 5 ${LogfileNPath} | grep "Ended at" ` 2>> ${LOGNAME}
	echo "JobEndLine=${JobEndLine} " >> ${LOGNAME}

	################################
	# Did job complete?
	################################
	if [ "${JobEndLine:=""}" = "" ];then
		echo "WARNING: Could not find Job End Line. Job did not complete. " >> ${LOGNAME}
		createJobInfoKeyValuePairs "N"
		continue
	fi

	################################
	# Get Job End Time
	################################
	offset=`echo ${JobEndLine} | grep -bo "at " | cut -d: -f1 `
	echo "offset=${offset}"  >> ${LOGNAME}
	offset=`expr ${offset} + 4 `  2>> ${LOGNAME}

	JobEndTime=`echo ${JobEndLine} | cut -c${offset}- `
	echo "JobEndTime=${JobEndTime}" >> ${LOGNAME}	


	######################################################	
	# Create Job Info key/value pairs
	# NOTE: Job variables set in funtion are used 
	#       to create File Extract Key/value pair records
	######################################################	
	createJobInfoKeyValuePairs "Y"

	
	###########################################	
	# Get Extract filenames and record counts
	###########################################	
	#filenamesAndCounts: DEA_PECOS_20230821.134920.txt.gz     14,848,397
	echo "" >> ${LOGNAME}
	echo "Extract filenames and record counts" >> ${LOGNAME}
	
	START_POSITIONS=`grep -n "filenamesAndCounts:" ${LogfileNPath} | cut -d: -f1 `   2>> ${LOGNAME}
	echo "START_POSITIONS=${START_POSITIONS}"  >> ${LOGNAME}
	
	if [ -z "${START_POSITIONS}" ];then
		echo "ERROR: START_POSITIONS is blank. Cannot get extract filenames. Script needs to be modified." >> ${LOGNAME}
		continue
	fi

	#########################################################	
	# Loop thru start Positions to get sets of extract files
	#########################################################	
	for START_POS in ${START_POSITIONS}
	do

		echo ""  >> ${LOGNAME} 
		echo "START_POS=${START_POS} " >> ${LOGNAME} 

		# Look for 1st blank line after START_POS	
		END_OFFSET=`sed -n "${START_POS},999999p" ${LogfileNPath} | grep -n '^[[:space:]]*$' | head -n 1 | cut -d: -f1 `
		echo "END_OFFSET=${END_OFFSET} " >> ${LOGNAME} 

		# Start line + blank line (-2) --> END_POS = last extract filename
		END_POS=`expr ${START_POS} + ${END_OFFSET} - 2 `
		echo "END_POS=${END_POS} " >> ${LOGNAME} 

		ExtractFilesAndRecCounts=`sed -n "${START_POS},${END_POS}p" ${LogfileNPath} `   >> ${LOGNAME}
		echo "ExtractFilesAndRecCounts: ${ExtractFilesAndRecCounts} " >> ${LOGNAME}

		############################################################	
		# Create Key/value pairs for ExtractFiles and record counts
		############################################################	
		echo "" >> ${LOGNAME}
		echo "Parse for Key Values" >> ${LOGNAME}

		Logfilename=`basename ${LogfileNPath} | sed "s/^[ ]*//g" `  >> ${LOGNAME}
		echo "Logfilename=${Logfilename}" >> ${LOGNAME}

		####################################	
		## Extract filenames and rec counts
		####################################	
		IFS=$'\n'
		for ExtractFileAndRecCount in ${ExtractFilesAndRecCounts}
		do
			# Remove Literal
			RECORD=`echo ${ExtractFileAndRecCount} | sed 's/filenamesAndCounts: //' `  >> ${LOGNAME}
			echo "ExtractFileAndRecCountRec=${RECORD}" >> ${LOGNAME}
			
			ExtractFile=`echo ${RECORD} |  awk '{print $1}'  `  2>> ${LOGNAME}
			RecCount=`echo ${RECORD} |  awk '{print $2}' | sed "s/,//g" `   >> ${LOGNAME}
			
			echo "ExtractFile=${ExtractFile}" >> ${LOGNAME}
			echo "RecCount=${RecCount}" >> ${LOGNAME}			
			
			#########################################	
			## Write Extract files and record counts
			#########################################	
			echo "log=${Logfilename} runDate=${runDate} runTmstmp=${runTmpstmp} ext=${ext_name} ExtractFile=${ExtractFile} RecCount=${RecCount} "  >> ${DATADIR}DASHBOARD_JOB_FILE_EXTRACTS_DTLS.txt

		done
		
	done

	
done  <  ${DATADIR}${TMP_FILE}


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME} 
echo "Remove temporary text files from data directory" >> ${LOGNAME} 

rm ${DATADIR}${TMP_FILE}  >> ${LOGNAME} 2>&1
rm ${DATADIR}${TMP_FILE1} >> ${LOGNAME} 2>&1
rm ${DATADIR}${TMP_FILE2} >> ${LOGNAME} 2>&1
rm ${DATADIR}${TMP_FILE3} >> ${LOGNAME} 2>&1
rm ${DATADIR}${TMP_FILE4} >> ${LOGNAME} 2>&1

#############################################################
# end script
#############################################################
echo "" >> ${LOGNAME}
echo "DashboardInfo.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS