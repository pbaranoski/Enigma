#!/usr/bin/bash
######################################################################################
# Name:  blbtn_clm_ext.sh
#
# Desc: Extract Blue Button claim data (IDR#BLB1). 
#
# Execute as ./blbtn_clm_ext.sh       (processing without override dates) 
# Execute as ./blbtn_clm_ext.sh $1 $2 (processing with override dates) 
#
# 			$1 = From_dt (YYYY-MM-DD)
# 			$2 = To_dt   (YYYY-MM-DD)
#
# Created: Paul Baranoski  06/09/2022
# Modified:
#
# Paul Baranoski 2022-09-29 Added code to call CombineS3Files.sh to concatenate/combine 
#                           S3 "parts" files 
# Paul Baranoski 2022-11-02 Added call to CreateManifestFile.sh
# Paul Baranoski 2022-11-03 Added code to send Success emails with filenames from script
#                           instead of python code. 
# Paul Baranoski 2023-07-14 Modify logic in getting extract filenames that include record counts. 
# Paul Baranoski 2023-07-19 Comment out Box functionality (we may use it in the future),
#                           and add EFT functionality.
# Paul Baranoski 2023-07-19 Add code to accept override extract dates from a config file.
# Paul Baranoski 2023-08-14 In performing a parallel run, noticed that the calculated parameter
#                           dates are a week ahead of the current mainframe run. So there is a 
#                           week lag. 
#                           Ex. on 8/14/2023, MF parm dates are 2023-07-31 AND 2023-08-06
#                           instead of 2023-08-07 AND 2023-08-13.
# Paul Baranoski 2023-12-11 Add $ENVNAME to SUBJECT line of all emails.
######################################################################################
set +x

###############################################################################
# Write Week date parameters (starting with Monday) 
# NOTE: If run on Tues --> week date range starts from prior Mon (yesterday) 
# NOTE: If run on Mon --> week date range starts from that Mon (today)  
###############################################################################

build_week_dt_parms() 
{

	# Parm Dates to be in YYYYMMDD format

	# get (current date - 14 days) and day-of-week
	dow=`date -d "-14 day" +%A`
	wkly_strt_dt=`date -d "-14 day" +%Y-%m-%d` 


	# set default NOF days decrement value = 14 above + 7 days prior
	# (d="a week before 14 days ago"
	d=`expr 14 + 7`
	dHold=${d}

	# if current date is Monday --> skip loop
	# if not Monday --> find Monday prior to today )
	until [ $dow = 'Monday' ]
	do
		d=`expr $d - 1` 	
		#echo $d
		dow=`date -d "-$d day" +%A`
		#echo $dow 

		wkly_strt_dt=`date -d "-$d day" +%Y-%m-%d` 
		#echo "wkly_strt_dt=$wkly_strt_dt"
	   
	done

	#echo "wkly_strt_dt=$wkly_strt_dt"


	# find end of week from selected Monday (a week before the decrement value
	if [ $d -eq ${dHold} ]; then
	    # if today is Monday --> end date is a week from yesterday
		wkly_end_dt=`date -d "-8 day" +%Y-%m-%d`
		#echo $wkly_end_dt
	else
	    # 6 days after the Monday that was found.
		d=`expr -$d + 6`
		wkly_end_dt=`date -d "$d day" +%Y-%m-%d`
		#echo $wkly_end_dt
	fi

	#echo "wkly_end_dt=$wkly_end_dt"
	
}


#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
LOGNAME=/app/IDRC/XTR/CMS/logs/blbtn_clm_ext_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "blbtn_clm_ext.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}


##################################################################
# Extract can run stand-alone or as a called script.
##################################################################
if ! [[ $# -eq 0 || $# -eq 2  ]]
then
	echo "Incorrect # of parameters sent to script. NOF parameters: $#" >> ${LOGNAME}
	exit 12
fi


#############################################################
# Display parameters passed to script 
#############################################################
ParmOverrideFromDt=$1
ParmOverrideToDt=$2

echo " " >> ${LOGNAME}
echo "Parameters to script: " >> ${LOGNAME}
echo "NOF parameters for script: " $# >> ${LOGNAME}
echo "   ParmOverrideFromDt=${ParmOverrideFromDt} " >> ${LOGNAME}
echo "   ParmOverrideToDt=${ParmOverrideToDt} " >> ${LOGNAME}


#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh >> ${LOGNAME}

source ${RUNDIR}FilenameCounts.bash

S3BUCKET=${BLBTN_BUCKET} 


#############################################################
# Build date parameters for blbtn_clm_ext script
#############################################################
echo "" >> ${LOGNAME}
echo "Calculate weekly extract dates" >> ${LOGNAME}

build_week_dt_parms

wkly_strt_dt=${ParmOverrideFromDt:-${wkly_strt_dt}} 
wkly_end_dt=${ParmOverrideToDt:-${wkly_end_dt}}

echo "wkly_strt_dt=${wkly_strt_dt}" >> ${LOGNAME} 
echo "wkly_end_dt=${wkly_end_dt}" >> ${LOGNAME}


#############################################################
# Make variables available for substitution in Python code
#############################################################
export TMSTMP
export wkly_strt_dt
export wkly_end_dt


#############################################################
# Execute Python code to Extract claims data.
#############################################################
echo "" >> ${LOGNAME}
echo "Start execution of blbtn_clm_ext.py program" >> ${LOGNAME}

${PYTHON_COMMAND} ${RUNDIR}blbtn_clm_ext.py  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
        echo "" >> ${LOGNAME}
        echo "Python script blbtn_clm_ext.py failed" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Weekly Blue Button Extract - Failed (${ENVNAME})"
		MSG="The weekly Blue Button extract has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${BLBTN_EMAIL_SENDER}" "${BLBTN_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

        exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script blbtn_clm_ext.py completed successfully." >> ${LOGNAME}


####################################################################
# Concatenate S3 files
# NOTE: Multiple files with suffix "n_n_n.txt.gz" are created. 
#       Will concatenate them into single file.
#
# Example --> blbtn_clm_ex_20220922.084321.txt.gz_0_0_0.txt.gz 
#         --> blbtn_clm_ex_20220922.084321.txt.gz
####################################################################
echo "" >> ${LOGNAME}
echo "Concatenate S3 files using CombineS3Files.sh   " >> ${LOGNAME}

echo "S3BUCKET=${S3BUCKET} " >> ${LOGNAME} 

concatFilename=blbtn_clm_ext_${TMSTMP}.txt.gz
echo "concatFilename=${concatFilename}" >> ${LOGNAME} 

${RUNDIR}CombineS3Files.sh ${S3BUCKET} ${concatFilename} 


#############################################################
# Check the status of script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Shell script CombineS3Files.sh failed." >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Combining S3 files in blbtn_clm_ext.sh - Failed (${ENVNAME})"
		MSG="Combining S3 files in blbtn_clm_ext.sh has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${BLBTN_EMAIL_SENDER}" "${BLBTN_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
fi	


#############################################################
# Get list of S3 files and record counts for success email.
#############################################################
echo "" >> ${LOGNAME}
echo "Get S3 Extract file list and record counts" >> ${LOGNAME}

getExtractFilenamesAndCounts ${LOGNAME}  >> ${LOGNAME} 2>&1
S3Files="${filenamesAndCounts}" 


#############################################################
# Send Success email.
#############################################################
echo "" >> ${LOGNAME}
echo "Send success email with S3 Extract filename." >> ${LOGNAME}
echo "S3Files=${S3Files} "   >> ${LOGNAME}

SUBJECT="Weekly Blue Button claim extract (${ENVNAME})" 
MSG="The Weekly Blue Button claim extract has completed.\n\nThe following file(s) were created:\n\n${S3Files}"

${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${BLBTN_EMAIL_SENDER}" "${BLBTN_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Error in calling sendEmail.py" >> ${LOGNAME}
		
		# Send Failure email	
		SUBJECT="Sending Success email in blbtn_clm_ext.sh - Failed (${ENVNAME})"
		MSG="Sending Success email in blbtn_clm_ext.sh has failed."
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${BLBTN_EMAIL_SENDER}" "${BLBTN_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12
fi	


##\/ Keep BOX/manifest file logic for the future
#############################################################
# Create Manifest file
#############################################################
#echo "" >> ${LOGNAME}
#echo "Create Manifest file for Blbtn Claim Extract.  " >> ${LOGNAME}
#
#${RUNDIR}CreateManifestFile.sh ${S3BUCKET} ${TMSTMP} ${BLBTN_EMAIL_SUCCESS_RECIPIENT} 


#############################################################
# Check the status of script
#############################################################
#RET_STATUS=$?

#if [[ $RET_STATUS != 0 ]]; then
#		echo "" >> ${LOGNAME}
#		echo "Shell script CreateManifestFile.sh failed." >> ${LOGNAME}
#		
#		# Send Failure email	
#		SUBJECT="Create Manifest file in blbtn_clm_ext.sh - Failed"
#		MSG="Create Manifest file in blbtn_clm_ext.sh has failed."
#		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${BLBTN_EMAIL_SENDER}" "${BLBTN_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
#
#		exit 12
#fi	
##/\ Keep manifest logic for the future


#############################################################
# EFT Extract files
#############################################################
echo " " >> ${LOGNAME}
echo "EFT Blue Button Claim Extract File " >> ${LOGNAME}
${RUNDIR}ProcessFiles2EFT.sh ${S3BUCKET}  >> ${LOGNAME} 2>&1


#############################################################
# Check the status of extract script
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script ProcessFiles2EFT.sh failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT=" Blue Button Claim Extract EFT process  - Failed (${ENVNAME})"
	MSG=" Blue Button Claim Extract EFT process has failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${BLBTN_EMAIL_SENDER}" "${BLBTN_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi


#############################################################
# clean-up linux data directory
#############################################################
echo "" >> ${LOGNAME} 
echo "Remove ${EXT_DT_CONFIG_FILE} from data directory" >> ${LOGNAME} 

rm ${DATADIR}${EXT_DT_CONFIG_FILE} 2>> ${LOGNAME} 


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "blbtn_clm_ext.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS
