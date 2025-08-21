#!/usr/bin/bash
############################################################################################################
# Script Name: DashboardInfo_MS.sh
#
# Description: This script will extract dashboard info from extract scripts.
#
#  ./DashboardInfo_MS.sh $1 $2  or ./DashboardInfo_MS.sh 
#  $1 --> RUN_FROM_DT (YYYYMMDD format) (Optional)
#  $2 --> RUN_TO_DT   (YYYYMMDD format) (Optional)
# 
#
# Author     : Paul Baranoski	
# Created    : 04/03/2024
#
# Paul Baranoski 2024-04-03 Created script.
# Paul Baranoski 2024-07-12 Fix bugs. 
#                           1) json files were not correctly moved in S3 to an archive folder.
#                           2) Second -newermt for find command is non-inclusive 
#                           3) DSH Extract name was parsed as DSHs. Added sed command to remove 'EXTRACTS'.  
# Paul Baranoski 2024-11-04 Add exception logic (legacy logic) for specific DEMO Finder.
# Paul Baranoski 2024-11-05 Modified to look for ending line to be "Ended at.." or "ended at" to know if extract ended successfully for VAPTD and VARTN scripts.  
# Paul Baranoski 2024-11-05 Modify date parameter logic so that the date to find log files is different than thru date for database deletes.  
# Paul Baranoski 2024-11-06 Clean-up of DASHBOARD_JOBINFO_FILE and DASHBOARD_JOBDTLS_FILE at end of script.
#                           Add exception logic for PSPS_NPI,PSPS_Split_files, and PTD_DUALS (Monthly). 
# Paul Baranoski 2024-11-07 Add case statements to ExtractFilenamesAndCountsLegacy to handle various label versions that identified filenames/record counts. 
# Paul Baranoski 2024-11-08 Renamed function getExtractFilenamesAndCountsDemo to getExtractFilenamesAndCountsDashboardInfo.
# Paul Baranoski 2024-11-08 Modified logic for PSPS_NPI, PSPS_Split_files, and PTD DUALS (Monthly) to get counts for awk created split files using DASHBOARD_INFO log 
#                           messages, and route to legacy code if those log messages are not found.
# Paul Baranoski 2025-03-28 Modify code to look for extract files and record counts even when an extract does not end successfully. This was done to capture
#                           extract filenames and record counts for extracts that fail to complete, and manual effort is done for create manifest files for deliver.
#                           Prior to this change, if a script failed, it was assumed that no files had been created. And, without this change, we will not 
#                           be accounting for extract files that were created, and thus under reporting the information in regards to extract files.
# Paul Baranoski 2025-04-09 Correct S3 move to archive folder log/email messages to be more accurate.
# Paul Baranoski 2025-06-18 Exclude GitHub log files from Dashboard processing.
# Paul Baranoski 2025-07-22 Exclude "PSPS_SF_Table_Load" logs from processing.
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/DashboardInfo_MS_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

DASHBOARD_JOBINFO_FILE=DASHBOARD_JOB_INFO_${TMSTMP}.json
DASHBOARD_JOBDTLS_FILE=DASHBOARD_JOB_DTLS_EXTRACT_FILES_${TMSTMP}.json

# Global variables for functions
g_HumanFileSize=""
g_ByteSize=""

TOT_WARNINGS=0
NL=$'\n'
		
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
# Ensure that DASHBOARD files exist
##################################################################
touch ${DATADIR}${DASHBOARD_JOBINFO_FILE}
touch ${DATADIR}${DASHBOARD_JOBDTLS_FILE}

chmod 666 ${DATADIR}${DASHBOARD_JOBINFO_FILE} 2>> ${LOGNAME} 
chmod 666 ${DATADIR}${DASHBOARD_JOBDTLS_FILE} 2>> ${LOGNAME} 


##################################################################
# Extract log information for yesteray  
#  --> unless overriding with date range   
##################################################################
if [[ $# -eq 2 ]];then
	echo " " >> ${LOGNAME}
	echo "Using override dates " >> ${LOGNAME}

	RUN_FROM_DT=$1
	RUN_TO_DT=$2
	RUN_THRU_DT_NOT_INCLUSIVE=`date -d "${2} +1 days" +%Y-%m-%d`
else
	echo " " >> ${LOGNAME}
	echo "Using script calculated dates " >> ${LOGNAME}
	
	# get yesterday's date
	RUN_FROM_DT=`date -d "-1 day" +%Y%m%d`
	RUN_TO_DT=`date -d "-1 day" +%Y%m%d`
	RUN_THRU_DT_NOT_INCLUSIVE=`date +%Y%m%d`
fi


#############################################################
# Display parameters passed to script 
#############################################################
echo " " >> ${LOGNAME}
echo "Parameters to script: " >> ${LOGNAME}
echo "   RUN_FROM_DT=${RUN_FROM_DT} " >> ${LOGNAME}
echo "   RUN_TO_DT=${RUN_TO_DT} " >> ${LOGNAME}
echo "   RUN_THRU_DT_NOT_INCLUSIVE=${RUN_THRU_DT_NOT_INCLUSIVE} " >> ${LOGNAME}
	
#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh 

echo "" >> ${LOGNAME}
echo "DASHBOARD_BUCKET=${DASHBOARD_BUCKET}" >> ${LOGNAME}


#############################################################
# function definitions  
#############################################################
getExtractFilenamesAndCounts() {

	echo "" >> ${LOGNAME}
	echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" >> ${LOGNAME}
	echo "In function getExtractFilenamesAndCounts" >> ${LOGNAME}
	
	# function paremeters
	P_LOGNAME=$1
	echo "function logname: ${P_LOGNAME}"  >> ${LOGNAME}
	
	# Get extract filenames
	COPY_INTO_FILENAMES=`egrep '^Executing: COPY INTO [@]{1}[a-zA-Z0-9_\.]+[/]+' ${P_LOGNAME} | cut -d/ -f2 `  2>> ${LOGNAME}

	# Filenames were not found	
	if [ -z "${COPY_INTO_FILENAMES}" ];then
		echo "" >> ${LOGNAME}
		echo "COPY_INTO_FILENAMES is empty/blank. " >> ${LOGNAME}
		echo "Exiting function getExtractFilenamesAndCounts" >> ${LOGNAME}
		
		# return code from function	
		return 4
	fi
	
	################################################
	# Ex. rows_unloaded,input_bytes,output_bytes
	#     2605154,601790572,107807527
	#
	# extract record-count,unzipped-bytes,zipped-bytes | convert commas to spaces
	ROW_COUNTS=`awk '/rows_unloaded/{getline;print $0}' ${P_LOGNAME} | sed 's/,/ /g' `  2>> ${LOGNAME}

	echo "" >> ${LOGNAME}
	echo "COPY_INTO_FILENAMES: ${COPY_INTO_FILENAMES} "  >> ${LOGNAME}
	echo "ROW_COUNTS: ${ROW_COUNTS}"  >> ${LOGNAME}

	# Ex. filenamesAndCounts = "TRICARE_EXTRACT_20241016.103059.txt.gz 2605154 601790572 107807527" 
	filenamesAndCounts=`paste <(printf %s "${COPY_INTO_FILENAMES}") <(printf %s "${ROW_COUNTS}") `  2>> ${LOGNAME}

	echo ""   >> ${LOGNAME}
	echo "filenamesAndCounts: ${filenamesAndCounts}"  >> ${LOGNAME}

	echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" >> ${LOGNAME}
	
}

getExtractFilenamesAndCountsDashboardInfo() {

	echo "" >> ${LOGNAME}
	echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" >> ${LOGNAME}
	echo "In function getExtractFilenamesAndCountsDashboardInfo" >> ${LOGNAME}
	
	# function paremeters
	P_LOGNAME=$1
	P_EXT=$2
	
	echo "logname: ${P_LOGNAME}"  >> ${LOGNAME}
	echo "Extract: ${P_EXT}"  >> ${LOGNAME}	
	
	#####################################################################################
	#     filename, record-count, unzipped-bytes, zipped-bytes | convert commas to spaces
	# Ex. DASHBOARD_INFO:DEMOFNDR_PTA_H0137_202305_20241022.151515.txt.gz 376,239136,8335 
	# --> DEMOFNDR_PTA_H0137_202305_20241022.151515.txt.gz 376 239136 8335
	#####################################################################################
	COPY_INTO_FILENAMES=`grep 'DASHBOARD_INFO:' ${P_LOGNAME} | cut -d: -f2 | cut -d' ' -f1 `  2>> ${LOGNAME}

	echo "" >> ${LOGNAME}
	echo "COPY_INTO_FILENAMES: ${COPY_INTO_FILENAMES} " >> ${LOGNAME}
	
	# if DASHBOARD_INFO: is not in log file--> use alternate search for ROW_COUNTS for older log files.	
	if [ -z "${COPY_INTO_FILENAMES}" ];then
		echo "" >> ${LOGNAME}
		echo "DASHBOARD_INFO:COPY_INTO_FILENAMES is empty/blank. Use older method for record counts" >> ${LOGNAME}
		
		ExtractFilenamesAndCountsLegacy ${P_LOGNAME} ${P_EXT}
		# capture RC from function ExtractFilenamesAndCountsLegacy
		RC=$?

		return ${RC}
	fi

	filenamesAndCounts=`grep 'DASHBOARD_INFO:' ${P_LOGNAME} | cut -d: -f2 | sed 's/,/ /g'`  2>> ${LOGNAME}


	echo ""   >> ${LOGNAME} >> ${LOGNAME}
	echo "filenamesAndCounts: ${filenamesAndCounts}"  >> ${LOGNAME}
	
	echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" >> ${LOGNAME}
	
}

ExtractFilenamesAndCountsLegacy(){

	echo "" >> ${LOGNAME}
	echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" >> ${LOGNAME}
	echo "In function ExtractFilenamesAndCountsLegacy" >> ${LOGNAME}
	
	# function paremeters
	P_LOGNAME=$1
	P_EXT=$2
	
	echo "logname: ${P_LOGNAME}"  >> ${LOGNAME}
	echo "Extract: ${P_EXT}"  >> ${LOGNAME}
	
	F_LogFilename=`basename ${P_LOGNAME} | sed "s/^[ ]*//g" ` 2>> ${LOGNAME}
	
	# Create variable to be used by caller
	filenamesAndCounts=""

	
	###########################################	
	# Get Extract filenames and record counts
	###########################################	
	#The following files were created: 
	#     DEMOFNDR_PTA_H0137_202305_20240820.150353.txt.gz            376
	#     DEMOFNDR_PTB_H0137_202305_20240820.150353.txt.gz            525
	
	echo "" >> ${LOGNAME}
	echo "Get START_POSITIONS" >> ${LOGNAME}

	case ${P_EXT} in
		DEMO)
			START_POSITIONS=`egrep -n "filenamesAndCounts: " ${P_LOGNAME} | cut -d: -f1`   2>> ${LOGNAME}
                ;; 
		PTD_DUALS)
			START_POSITIONS=`egrep -n "REC_CNTS=" ${P_LOGNAME} | cut -d: -f1`   2>> ${LOGNAME}
				;;
		PSPS_NPI)
	 		START_POSITIONS=`egrep -n "filenamesAndCounts: " ${P_LOGNAME} | cut -d: -f1`   2>> ${LOGNAME}
				;;
		PSPS_SPLIT)
	 		START_POSITIONS=`egrep -n "filenamesAndCounts: " ${P_LOGNAME} | cut -d: -f1`   2>> ${LOGNAME}
				;;
		*)
				;;
	esac
	
	echo "START_POSITIONS=${START_POSITIONS}"  >> ${LOGNAME}
	
	if [ -z "${START_POSITIONS}" ];then
		echo "WARNING: START_POSITIONS is blank. Cannot get extract filenames. Script associated with ${F_LogFilename} may need to be modified." >> ${LOGNAME}
		echo "Exiting function ExtractFilenamesAndCountsLegacy" >> ${LOGNAME}

		TOT_WARNINGS=$((TOT_WARNINGS+1))  2>> ${LOGNAME}

		return 4
	fi

	#########################################################	
	# Loop thru start Positions to get sets of extract files
	#########################################################	
	for START_POS in ${START_POSITIONS}
	do

		echo ""  >> ${LOGNAME} 
		echo "START_POS=${START_POS} " >> ${LOGNAME} 

		# Look for 1st blank line after START_POS	
		END_OFFSET=`sed -n "${START_POS},999999p" ${P_LOGNAME} | grep -n '^[[:space:]]*$' | head -n 1 | cut -d: -f1 `
		echo "END_OFFSET=${END_OFFSET} " >> ${LOGNAME} 

		# Start line + blank line (-2) --> END_POS = last extract filename
		END_POS=`expr ${START_POS} + ${END_OFFSET} - 2 `
		echo "END_POS=${END_POS} " >> ${LOGNAME} 

		ExtractFilesAndRecCounts=`sed -n "${START_POS},${END_POS}p" ${P_LOGNAME} `   >> ${LOGNAME}
		echo "ExtractFilesAndRecCounts: ${ExtractFilesAndRecCounts} " >> ${LOGNAME}

		############################################################	
		# Create Key/value pairs for ExtractFiles and record counts
		############################################################	
		echo "" >> ${LOGNAME}
		echo "Parse for Key Values" >> ${LOGNAME}


		####################################	
		## Extract filenames and rec counts
		####################################	
		IFS=$'\n'
		for ExtractFileAndRecCount in ${ExtractFilesAndRecCounts}
		do
			echo "ExtractFileAndRecCountRec=${ExtractFileAndRecCount}" >> ${LOGNAME}

			# Remove Literal
			case ${P_EXT} in
				DEMO)
					RECORD=`echo ${ExtractFileAndRecCount} | sed 's/filenamesAndCounts: //' ` >> ${LOGNAME}
						;; 
				PTD_DUALS)
					RECORD=`echo ${ExtractFileAndRecCount} | sed 's/REC_CNTS=//' ` >> ${LOGNAME}
						;;
				PSPS_NPI)
					RECORD=`echo ${ExtractFileAndRecCount} | sed 's/filenamesAndCounts: //' ` >> ${LOGNAME}
						;;
				PSPS_SPLIT)
					RECORD=`echo ${ExtractFileAndRecCount} | sed 's/filenamesAndCounts: //' ` >> ${LOGNAME}
						;;
				*)
						;;
			esac	
			
			echo "RECORD=${RECORD}" >> ${LOGNAME}
			
			####################################################
			# Does line contain a file (.gz or .txt or .csv file ext)? 
			#   1) A blank record didn't exist after filenames/record counts --> we got other log messages instead of filenames/record counts
			####################################################
			FILE_FND=`echo ${RECORD} | egrep -c "(.gz |.txt |.csv )" `
			if [ ${FILE_FND} -eq 0 ];then
				echo "WARNING: ${RECORD} does not appear to contain a filename. Script associated with ${F_LogFilename} may need to be modified." >> ${LOGNAME}
				echo "Exiting function ExtractFilenamesAndCountsLegacy" >> ${LOGNAME}
				
				TOT_WARNINGS=$((TOT_WARNINGS+1))  2>> ${LOGNAME}
				
				# skip this record and remaing records
				return 0
			fi
			
			ExtractFile=`echo ${RECORD} |  awk '{print $1}'  `  2>> ${LOGNAME}
			RecCount=`echo ${RECORD} |  awk '{print $2}' | sed "s/,//g" `   >> ${LOGNAME}
			
			echo "ExtractFile=${ExtractFile}" >> ${LOGNAME}
			echo "RecCount=${RecCount}" >> ${LOGNAME}			

			
			#########################################	
			# Get human readable file size
			#########################################	
			if [ -n "${ExtractFile}" ];then
				calcByteCount ${ExtractFile} ${RecCount} ${P_EXT}

				# Build Extract file info with byte size	
				NextFileInfo="${ExtractFile} ${RecCount} ${g_ByteSize}"
				
				# Append NextFileInfo to variable
				filenamesAndCounts="${filenamesAndCounts}${NextFileInfo}${NL}"
				echo "filenamesAndCounts: ${filenamesAndCounts}"  >> ${LOGNAME}

			else
				echo "WARNING: Extract Filename and record count are blank. Script associated with ${Logfilename} may need to be modified." >> ${LOGNAME}
				echo "Exiting function ExtractFilenamesAndCountsLegacy" >> ${LOGNAME}
				
				TOT_WARNINGS=$((TOT_WARNINGS+1))  2>> ${LOGNAME}
						
				# skip this record and remaing records
				return 4
			fi
		done
		
	done

	echo ""   >> ${LOGNAME} >> ${LOGNAME}
	echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" >> ${LOGNAME}
		
}

calcByteCount() {


	echo "" >> ${LOGNAME}
	echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" >> ${LOGNAME}
	echo "In function calcByteCount" >> ${LOGNAME}

	P_ExtractFile=$1
	P_RecCount=$2
	P_EXT=$3

	echo "P_ExtractFile: ${P_ExtractFile}"  >> ${LOGNAME}
	echo "P_RecCount:    ${P_RecCount}"     >> ${LOGNAME}
	echo "P_EXT:         ${P_EXT}"          >> ${LOGNAME}

	# Get LRECLs for exception logs
	case ${P_EXT} in
		DEMO)
			if [ `echo ${P_ExtractFile} | grep '_PTA_' ` ];then
				LRECL=635
		    elif [ `echo ${P_ExtractFile} | grep '_PTB_' ` ];then
				LRECL=625
			else
				LRECL=253
			fi
                ;; 
		PTD_DUALS)
			LRECL=185
				;;
         
		PSPS_NPI)
	 		LRECL=126
				;;
        
		PSPS_SPLIT)
			LRECL=129			
				;;
		
		*)
				;;

	esac

	echo "LRECL: ${LRECL}"  >> ${LOGNAME}
	
	# Calculate total NOF bytes
	g_ByteSize=$(( ${LRECL} * ${P_RecCount} ))  2>> ${LOGNAME}
	echo "g_ByteSize: ${g_ByteSize}"  >> ${LOGNAME}

	echo ""   >> ${LOGNAME} >> ${LOGNAME}
	echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" >> ${LOGNAME}	

}


convertBytes2ReadableSize() {

	echo "" >> ${LOGNAME}
	echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" >> ${LOGNAME}
	
	echo "In function convertBytes2ReadableSize" >> ${LOGNAME}
	
	# total NOF bytes for file
	p_Bytes=$1
	g_HumanFileSize=""

	echo "p_Bytes=${p_Bytes}" >> ${LOGNAME}

	# NOF Bytes per size type
	KB=1024
	MB=1048576
	GB=1073741824
	TB=1099511627776
	
	# convert total bytes to human readable file size
	echo "Convert total bytes to human readable file size" >> ${LOGNAME}
	
	if   [ ${p_Bytes} -ge ${TB} ];then
	    echo "Calculate file size in TBs" >> ${LOGNAME}
		g_HumanFileSize=`awk -v totBytes="${p_Bytes}" -v totSizeBytes=${TB}  'BEGIN{printf "%.2f TB\n", totBytes / totSizeBytes}' ` 2>> ${LOGNAME}
		
	elif [ ${p_Bytes} -ge ${GB} ];then
	    echo "Calculate file size in GBs" >> ${LOGNAME}
		g_HumanFileSize=`awk -v totBytes="${p_Bytes}" -v totSizeBytes=${GB}  'BEGIN{printf "%.2f GB\n", totBytes / totSizeBytes}' ` 2>> ${LOGNAME}
		
	elif [ ${p_Bytes} -ge ${MB} ];then
	    echo "Calculate file size in MBs" >> ${LOGNAME}
		g_HumanFileSize=`awk -v totBytes="${p_Bytes}" -v totSizeBytes=${MB}  'BEGIN{printf "%.2f MB\n", totBytes / totSizeBytes}' ` 2>> ${LOGNAME}
		
	elif [ ${p_Bytes} -ge ${KB} ];then
	    echo "Calculate file size in KBs" >> ${LOGNAME}
		g_HumanFileSize=`awk -v totBytes="${p_Bytes}" -v totSizeBytes=${KB}  'BEGIN{printf "%.2f KB\n", totBytes / totSizeBytes}' ` 2>> ${LOGNAME}
		
	elif [ ${p_Bytes} -ge 0 ];then
	    echo "Calculate file size in Bytes" >> ${LOGNAME}
		g_HumanFileSize="${p_Bytes} B" 
	fi

	echo "g_HumanFileSize=${g_HumanFileSize} " >> ${LOGNAME}

	echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" >> ${LOGNAME}
	
}	

createJobInfoKeyValuePairs() {

	echo "" >> ${LOGNAME}
	echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" >> ${LOGNAME}
	
	echo "In function createJobInfoKeyValuePairs" >> ${LOGNAME}

	# Set script success/failure
	JobSuccess=$1
	echo "JobSuccess=${JobSuccess}" >> ${LOGNAME}
	
	############################################################	
	# Create Key/value pairs for Job Info
	############################################################
	#blbtn_clm_ext_20231020.134153.log, Fri Oct 20 13:41:53 EDT 2023,Fri Oct 20 13:42:05 EDT 2023
	#OFM_PDE_Extract_20231018.163447.log, Wed Oct 18 16:34:47 EDT 2023,Wed Oct 18 16:56:11 EDT 2023	

	echo "" >> ${LOGNAME}
	echo "Parse for Key Values" >> ${LOGNAME}

	echo "Logfilename=${Logfilename}"  >> ${LOGNAME}

	# Parse for Extract name and Run Timestamp	
	FLD_POS_EXT=`echo ${Logfilename} | grep -bo "_" | wc -l `
	FLD_POS_TMSTMP=`expr ${FLD_POS_EXT} + 1 ` 2>> ${LOGNAME}
	
	ext_name=`echo ${Logfilename} | cut -d_ -f1-${FLD_POS_EXT} `
	
	# remove verbiage "Driver" and "Extract"
	ext_name=`echo ${ext_name} | sed "s/_Driver//g" `    >> ${LOGNAME}
	ext_name=`echo ${ext_name} | sed "s/_Extracts//g" `  >> ${LOGNAME}
	ext_name=`echo ${ext_name} | sed "s/_Extract//g" `   >> ${LOGNAME}
	ext_name=`echo ${ext_name} | sed "s/_EXTRACTS//g" `  >> ${LOGNAME}
	ext_name=`echo ${ext_name} | sed "s/_EXTRACT//g" `   >> ${LOGNAME}
	echo "ext_name=${ext_name}" >> ${LOGNAME}
	
	runTmpstmp=`echo ${Logfilename} | cut -d_ -f${FLD_POS_TMSTMP}- | sed "s/.log//" `   >> ${LOGNAME}
	echo "runTmpstmp=${runTmpstmp}" >> ${LOGNAME}
	
	runDate=`echo ${runTmpstmp} | cut -d. -f1 `
	echo "runDate=${runDate}" >> ${LOGNAME}


	##########################################
	## Write Extract info record as json file
	##########################################	
	echo "{\"log\": \"${Logfilename}\",\"ext\": \"${ext_name}\",\"runTmstmp\": \"${runTmpstmp}\",\"success\": \"${JobSuccess}\"} " >> ${DATADIR}${DASHBOARD_JOBINFO_FILE}

	echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" >> ${LOGNAME}

}


#################################################################################
# Get list of Log Files that are between START_DT and END_DT
#
# NOTE: Ignore logs for utility scripts, load finder file scripts, python database logs, and support processing logs.
#       Also, ignore certin application child logs. 
#
# NOTE-2:!!!!PSPS_Split_files  - No record counts - The following file(s) were created:
#  Should I ignore or use old logic - for awk scripts that split files.
#################################################################################
echo "Change to logs directory "  >> ${LOGNAME}
cd ${LOGS}
echo `pwd` >> ${LOGNAME}

echo "" >> ${LOGNAME}
echo "Get list of log files to process "  >> ${LOGNAME}

############################################################################
# !!!! WARNING: Second -newermt date is non-inclusive (\! = NOT)
# find . -name "*.log" -newermt 20240301 \! -newermt 20240309  --> all log files between the 20240301 thru 20240308
############################################################################

# Extract initial list of log files to process into temp file - Ignore logs for utility scripts, load finder file scripts, python database logs
find . -name "*.log" -newermt ${RUN_FROM_DT} \! -newermt ${RUN_THRU_DT_NOT_INCLUSIVE} | grep -v "CombineS3Files" | grep -v "CreateManifestFile" | grep -v "ProcessFiles2EFT" | grep -v "_SF." | grep -v "LOAD_*"  > ${DATADIR}${TMP_FILE1}

# Remove support scripts
cat ${DATADIR}${TMP_FILE1} | grep -v "DashboardInfo" | grep -v "BuildRunExtCalendar" | grep -v "KIA" | grep -v "ListXTRProcess" | grep -v "GitHub" >  ${DATADIR}${TMP_FILE2}

# Remove reporting log files
cat ${DATADIR}${TMP_FILE2} | grep -v "Manifest" | grep -v "FinderFiles" | grep -v "CalendarExtReports" | grep -v "PSPS_SF_Table_Load" >  ${DATADIR}${TMP_FILE3}

# Remove child logs from list of log files to process
cat ${DATADIR}${TMP_FILE3} | grep -v "DemoFinderFilePrep" | grep -v "DEMOFNDR_PT"  >  ${DATADIR}${TMP_FILE4}

# Remove specific Driver logs from list of log files to process. Ones to keep are VAPTD_Driver, VARTN_Driver, OPMHI_Driver
cat ${DATADIR}${TMP_FILE4} | grep -v "NYSPAP_Extract_Driver"  | grep -v "PTD_DUAL_Daily_Driver"  | grep -v "PTD_DUAL_Monthly_Driver" | egrep -v "SAF_ENC_(INP|SNF)_Driver"  >  ${DATADIR}${TMP_FILE}


# Display list of log files that we will process
LOG_FILES_2_PROCESS=`cat ${DATADIR}${TMP_FILE} `
echo LOG_FILES_2_PROCESS="${LOG_FILES_2_PROCESS}"   >> ${LOGNAME}


#################################################################################
# Loop thru list.
#################################################################################
IFS=$'\n'
		
while read LogfileNPath
do

	echo "" >> ${LOGNAME}
	echo "" >> ${LOGNAME}
	echo "********************************************" >> ${LOGNAME}
	
	echo "LogfileNPath=${LogfileNPath}"  >> ${LOGNAME}
	
	Logfilename=`basename ${LogfileNPath} | sed "s/^[ ]*//g" `  >> ${LOGNAME}
	echo "Logfilename=${Logfilename}"  >> ${LOGNAME}	
	
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
	# Example: "Ended at Mon Aug  7 15:23:01 EDT 2023" or "VAPTD_Driver.sh ended at: 20240902.170605"
	#       or "Script PSPS_Split_files.sh completed successfully"
	JobEndLine=`tail -n 5 ${LogfileNPath} | egrep "(E|e)nded at" ` 2>> ${LOGNAME}
	echo "JobEndLine=${JobEndLine} " >> ${LOGNAME}


	################################
	# Did job complete? No.
	################################
	if [ "${JobEndLine:=""}" = "" ];then
		createJobInfoKeyValuePairs "N"

		echo "INFO: Could not find Job End Line. Job associated with ${Logfilename} did not complete. " >> ${LOGNAME}

	else
		################################
		# Get Job End Time
		################################
		offset=`echo ${JobEndLine} | grep -bo "at " | cut -d: -f1 ` 2>> ${LOGNAME}
		echo "offset=${offset}"        >> ${LOGNAME}
		offset=`expr ${offset} + 4 `  2>> ${LOGNAME}

		JobEndTime=`echo ${JobEndLine} | cut -c${offset}- `  2>> ${LOGNAME}
		echo "JobEndTime=${JobEndTime}" >> ${LOGNAME}	

		######################################################	
		# Create Job Info key/value pairs
		# NOTE: Job variables set in function are used 
		#       to create File Extract Key/value pair records
		######################################################	
		createJobInfoKeyValuePairs "Y"
	
	fi


	###########################################	
	# Get Extract filenames and record counts
	###########################################	
	echo "" >> ${LOGNAME}
	echo "Get Extract filenames and record counts from log file" >> ${LOGNAME}

    ###################################################################################################################
	# 1) For all extracts that extract files using a SF SELECT statment (except DEMO Finder), we replicate the FilenameCounts.bash 
	#    logic to extract and combine the SF Filenames, record counts, and byte counts which were always displayed in the logs.
	#
	# 2) For Demo Finder going forward: modified the FilenameCounts.bash to display filenames, record counts, and byte counts with label 
    #    "DASHBOARD_INFO:" for each extract file. The logic from #1 would not work since that information was in separate DEMO PTA, PTB, and PTD log files. 
	#    And, the Dashboard script is only parsing the main Demo Finder log file for simplicity. (Demo is one extract not 3 separate extracts).
	#
	#    For older Demo Finder log files, find label "filenamesAndCounts:", extract filenames and record counts, then for each file, calculate byte count
    #    by multiplying record count * hard-coded LRECL. Add byte count to end of each "filename record count" to add to table.	
	#
	# 3) For extracts that use awk to split the extract file, we will find label "filenamesAndCounts:", extract filenames and record counts, then for each file
	#    calculate byte count by multiplying record count * hard-coded LRECL. Add byte count to end of each "filename record count" to add to table.
    #    While we can use the logic for #1 to get the all-in-one-extract counts, we would miss out on the split files created by the awk script.	
	#    Includes PSPS_NPI_Extract.sh, PSPS_Split_files.bash 
	#
    # PTD Duals Monthly     - 1) Normal one file extract where we get counts; 2) split extract file into smaller files - will ignore this. (Too complex)
    #
	#
 	if [ `echo ${Logfilename} | egrep '^DemoFinderFileExtracts_' ` ];then
		getExtractFilenamesAndCountsDashboardInfo ${LogfileNPath} "DEMO"
		RC=$?
	elif [ `echo ${Logfilename} | egrep '^PSPS_NPI_Extract_' ` ];then	
		getExtractFilenamesAndCountsDashboardInfo ${LogfileNPath} "PSPS_NPI"
		RC=$?
	elif [ `echo ${Logfilename} | egrep '^PSPS_Split_files_' ` ];then	
		getExtractFilenamesAndCountsDashboardInfo ${LogfileNPath} "PSPS_SPLIT"
		RC=$?		
	elif [ `echo ${Logfilename} | egrep '^PTD_Duals_Extract_' ` ];then	
		getExtractFilenamesAndCountsDashboardInfo ${LogfileNPath} "PTD_DUALS"
		RC=$?
	else
		getExtractFilenamesAndCounts ${LogfileNPath}
		RC=$?
	fi


	echo "RC from getExtractFilenameAndCounts=${RC}" >> ${LOGNAME}
	
	# Were extract filenames found?	
	if [ $RC -ne 0 ];then
		echo "WARNING: COPY_INTO_FILENAMES is blank. Cannot get extract filenames. Script associated with ${Logfilename} may need to be modified." >> ${LOGNAME}

		TOT_WARNINGS=$((TOT_WARNINGS+1))  2>> ${LOGNAME}

		continue	
	fi
	
	## Were extract filenames found?
	#if [ -z "${COPY_INTO_FILENAMES}" ];then
	#	echo "WARNING: COPY_INTO_FILENAMES is blank. Cannot get extract filenames. Script associated with ${Logfilename} may need to be modified." >> ${LOGNAME}
    # 
	#	TOT_WARNINGS=$((TOT_WARNINGS+1))  2>> ${LOGNAME}
    #
	#	continue
	#fi

	#########################################################	
	# Loop thru start Positions to get sets of extract files
	#########################################################
	echo "" >> ${LOGNAME}
	echo "Loop thru Extract File information found" >> ${LOGNAME}
		
	for FILE_INFO in ${filenamesAndCounts}
	do

		############################################################	
		# Extract Filename, rec count, and byte count from FILE_INFO
		# Ex. Extract_file_20240102.151515.txt.gz 376 239136 8335
		############################################################	
		echo ""  >> ${LOGNAME} 
		echo "FILE_INFO=${FILE_INFO} " >> ${LOGNAME} 

		ExtractFile=`echo ${FILE_INFO} | awk '{print $1}' ` 2>> ${LOGNAME}
		RecCount=`echo ${FILE_INFO} | awk '{print $2}' `    2>> ${LOGNAME}
		g_ByteSize=`echo ${FILE_INFO} | awk '{print $3}' `    2>> ${LOGNAME}
		##g_ByteZipSize=`echo ${FILE_INFO} | awk '{print $4}' `    2>> ${LOGNAME}	

		# RecCount cannot be empty str/Null
		if [ "${RecCount}" = "" ];then
			echo "RecCount is empty str. Assign RecCount and g_ByteSize 0 default value." >> ${LOGNAME}
			RecCount=0
			g_ByteSize=0
		fi
		
		echo "ExtractFile=${ExtractFile}" >> ${LOGNAME}
		echo "RecCount=${RecCount}" >> ${LOGNAME}	
		echo "g_ByteSize=${g_ByteSize}" >> ${LOGNAME}	
			
		############################################################	
		# Convert bytes to Human Readable value
		############################################################	
		convertBytes2ReadableSize ${g_ByteSize}

		
		############################################################	
		# Create Key/value pairs for ExtractFiles and record counts
		############################################################
		echo "{\"log\": \"${Logfilename}\",\"ext\": \"${ext_name}\",\"runTmstmp\": \"${runTmpstmp}\",\"ExtractFile\": \"${ExtractFile}\",\"RecCount\": \"${RecCount}\" , \"FileByteSize\": \"${g_ByteSize}\", \"HumanFileSize\": \"${g_HumanFileSize}\" } " >> ${DATADIR}${DASHBOARD_JOBDTLS_FILE}
		
	done

	
done  <  ${DATADIR}${TMP_FILE}


############################################################
# Move Dashboard JOBINFO json file to S3.
############################################################
echo "" >> ${LOGNAME}
echo "Move ${DASHBOARD_JOBINFO_FILE} file from linux data directory to S3 bucket ${DASHBOARD_BUCKET}" >> ${LOGNAME}

aws s3 mv ${DATADIR}${DASHBOARD_JOBINFO_FILE} s3://${DASHBOARD_BUCKET}${DASHBOARD_JOBINFO_FILE}  1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Move ${DASHBOARD_JOBINFO_FILE} file from linux data directory to S3 bucket ${DASHBOARD_BUCKET} - failed." >> ${LOGNAME}
	
	# Send Failure email
	SUBJECT="DashboardInfo_MS.sh - Failed (${ENVNAME})"
	MSG="Move ${DASHBOARD_JOBINFO_FILE} file from linux data directory to S3 bucket ${DASHBOARD_BUCKET} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


############################################################
# Move Dashboard JOBDTLS json file to S3 
############################################################
echo "" >> ${LOGNAME}
echo "Move ${DASHBOARD_JOBDTLS_FILE} file from linux data directory to S3 bucket ${DASHBOARD_BUCKET}" >> ${LOGNAME}

aws s3 mv ${DATADIR}${DASHBOARD_JOBDTLS_FILE} s3://${DASHBOARD_BUCKET}${DASHBOARD_JOBDTLS_FILE}  1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Move ${DASHBOARD_JOBDTLS_FILE} file from linux data directory to S3 bucket ${DASHBOARD_BUCKET} - failed." >> ${LOGNAME}
	
	# Send Failure email
	SUBJECT="DashboardInfo_MS.sh - Failed (${ENVNAME})"
	MSG="Move ${DASHBOARD_JOBDTLS_FILE} file from linux data directory to S3 bucket ${DASHBOARD_BUCKET} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


#################################################################################
# Execute Python code to load Extract data to SF
#################################################################################
echo "" >> ${LOGNAME}
echo "Start execution of Dashboard_MS.py program"  >> ${LOGNAME}

# Export environment variables for Python code
export RUN_FROM_DT
export RUN_TO_DT
export DASHBOARD_JOBINFO_FILE
export DASHBOARD_JOBDTLS_FILE

${PYTHON_COMMAND} ${RUNDIR}DashboardInfo_MS.py >> ${LOGNAME} 2>&1


#################################################################################
# Check the status of python script
#################################################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Python prgoram DashboardInfo_MS.py failed" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Python program DashboardInfo_MS.py - Failed (${ENVNAME})"
	MSG="Python program DashboardInfo_MS.py failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


############################################################
# Move Dashboard JOBINFO json file to S3 archive folder.
############################################################
echo "" >> ${LOGNAME}
echo "Move S3 ${DASHBOARD_JOBINFO_FILE} file to S3 ${DASHBOARD_BUCKET}archive folder" >> ${LOGNAME}

aws s3 mv s3://${DASHBOARD_BUCKET}${DASHBOARD_JOBINFO_FILE} s3://${DASHBOARD_BUCKET}archive/${DASHBOARD_JOBINFO_FILE}  1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Move S3 ${DASHBOARD_JOBINFO_FILE} file to S3 ${DASHBOARD_BUCKET}archive folder - failed." >> ${LOGNAME}
	
	# Send Failure email
	SUBJECT="DashboardInfo_MS.sh - Failed (${ENVNAME})"
	MSG="Move ${DASHBOARD_JOBINFO_FILE} file to S3 bucket ${DASHBOARD_BUCKET} archive folder failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


############################################################
# Move Dashboard JOBDTLS json file to S3 archive folder.
############################################################
echo "" >> ${LOGNAME}
echo "Move S3 ${DASHBOARD_JOBDTLS_FILE} file to S3 ${DASHBOARD_BUCKET}archive folder" >> ${LOGNAME}

aws s3 mv s3://${DASHBOARD_BUCKET}${DASHBOARD_JOBDTLS_FILE} s3://${DASHBOARD_BUCKET}archive/${DASHBOARD_JOBDTLS_FILE}  1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Move S3 ${DASHBOARD_JOBDTLS_FILE} file to S3 ${DASHBOARD_BUCKET}archive folder - failed." >> ${LOGNAME}
	
	# Send Failure email
	SUBJECT="DashboardInfo_MS.sh - Failed (${ENVNAME})"
	MSG="Move ${DASHBOARD_JOBDTLS_FILE} file to S3 bucket ${DASHBOARD_BUCKET} archive folder failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


############################################################
# Success email. 
############################################################
echo "" >> ${LOGNAME}
echo "Send success email for load of Dashboard tables for period ${RUN_FROM_DT} to ${RUN_TO_DT}." >> ${LOGNAME}

SUBJECT="DashboardInfo_MS (${ENVNAME})" 
MSG="The loading of the Dashboard tables with extract log information from ${RUN_FROM_DT} to ${RUN_TO_DT} has completed successfully.\n\nThere are ${TOT_WARNINGS} warnings in script log."

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Error in calling sendEmail.py" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="Sending Success email in DashboardInfo_MS.sh - Failed (${ENVNAME})"
	MSG="Sending Success email in DashboardInfo_MS.sh  has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


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

rm ${DATADIR}${DASHBOARD_JOBINFO_FILE} >> ${LOGNAME} 2>&1
rm ${DATADIR}${DASHBOARD_JOBDTLS_FILE} >> ${LOGNAME} 2>&1

#############################################################
# end script
#############################################################
echo "" >> ${LOGNAME}
echo "DashboardInfo.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS