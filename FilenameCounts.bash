#!/usr/bin/bash
############################################################################################################
# Name:  FilenameCounts.bash
#
# NOTE: Script must be /usr/bin/bash instead of /usr/bin/sh because the included FilenameCounts.bash file requires some bash specific syntax for 
#       a paste command which uses "bash Process Substitution". The paste command expects files and not variables as input. However, 
#       "bash Process Substitution" makes variables appear to the command as files.  The purpose of doing it this way instead of using temp files 
#       is because we have multiple scripts using the same data directory that would use the same temp file, possibly interferring with each other. 
#
# Modified: 
#
# Paul Baranoski 2023-04-24 Modified parameter name LOGNAME in function to be P_LOGNAME. Function logfilename parameter was
#                           overlaying main function LOGNAME variable causing log messages to be written to wrong log file.
# Paul Baranoski 2024-09-17 Modify grep command to get COPY_INTO_FILENAMES to exclude Finder File loads in log file. 
#                           Ex. - to exclude "Executing: COPY INTO BIA_DEV.CMS_TARGET_XTR_DEV.SEER_FF"
#                           Ex. - to find "Executing: COPY INTO @BIA_DEV.CMS_STAGE_XTR_DEV.BIA_DEV_XTR_SEER_STG/SEER_EXT_UT-Cancer-Registry_20240917.160500.txt.gz"
# Paul Baranoski 2024-10-18 Modify to get ROW_INFO which is entire rows-unloaded information for DashboardInfo_MS.sh script which includes extract byte count.
# Paul Baranoski 2024-11-07 Add comment to better explain DASHBOARD_INFO contents.
#############################################################################################################


getExtractFilenamesAndCounts() {

	# function paremeters
	P_LOGNAME=$1
	echo "function logname: ${P_LOGNAME}"
	
	#COPY_INTO_FILENAMES=`grep 'Executing: COPY INTO' ${P_LOGNAME} | cut -d/ -f2 `
	COPY_INTO_FILENAMES=`egrep '^Executing: COPY INTO [@]{1}[a-zA-Z0-9_\.]+[/]+' ${P_LOGNAME} | cut -d/ -f2 `
	
	# Ex. rows_unloaded,input_bytes,output_bytes 
	#     12345,456546,3453454  --> ROW_INFO
	ROW_COUNTS=`awk -F "," '/rows_unloaded/{getline;print $1}' ${P_LOGNAME} `
	ROW_INFO=`awk '/rows_unloaded/{getline;print $0}' ${P_LOGNAME} `
	
	echo ""
	echo "COPY_INTO_FILENAMES: ${COPY_INTO_FILENAMES} "
	echo "ROW_COUNTS: ${ROW_COUNTS}"
	echo "ROW_INFO: ${ROW_INFO}"
	
	#filenamesAndCounts=`paste <(printf %s "${COPY_INTO_FILENAMES}") <(printf %s "${ROW_COUNTS}") | xargs printf "%-60s %'14d\n" `
	filenamesAndCounts=`paste <(printf %s "${COPY_INTO_FILENAMES}") <(printf %s "${ROW_COUNTS}") | xargs printf "%s %'14d\n" `

	echo ""
	echo "filenamesAndCounts: ${filenamesAndCounts}"

	# Print eye-catcher DASHBOARD_INFO: for DashboardInfo_MS.sh; 
	# DASHBOARD_INFO:DEMOFNDR_PTA_H0137_202305_20241022.151515.txt.gz 376,239136,8335 contains label:filename rec-count,byte-count,zip-byte-count
	DASHBOARD_INFO=`paste <(printf %s "${COPY_INTO_FILENAMES}") <(printf %s "${ROW_INFO}") | xargs printf "DASHBOARD_INFO:%s %s \n" `

	# print 	
	echo ""
	echo "${DASHBOARD_INFO}"
	

}



