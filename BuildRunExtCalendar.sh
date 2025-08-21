#!/usr/bin/bash

######################################################################################
# Name:  BuildRunExtCalendar.sh
#
# Desc: Create a Run Extract calendar based on the values in a configuration file and 
#       Year parameter.
#
# DOW_DOM: For weekly extracts, valid values are: 1) M-F 2) individual days 3) series of days delimited by semi-colon
# !! What about Tue, Thu
#          For non-weekly extracts, valid values are: 
#              LW = Last working day of month    LD = Last day of month 
#              FW = First working day of month   FD = First day of month 
#
# Created: Paul Baranoski  02/05/2024
# Modified:
#
# Paul Baranoski 2024-02-05 Created script.
# Paul Baranoski 2024-02-20 Change config file delimiter logic to use pipe instead of comma.
#                           Change field delimiter from semi-colon to comma.
# Paul Baranoski 2024-02-21 Modify for new field FF_Pre_Processing.
# Paul Baranoski 2024-06-05 Modify success email to use ENIGMA_EMAIL_SUCCESS_RECIPIENT.
# Paul Baranoski 2024-09-03 Add getMatchingDOWDate function to find dates like 2nd Fri of month
#                           Re-work logic to use buildQtrCal4Yr function for month processing.
# Paul Baranoski 2024-10-21 Renamed constant DAYS4MM_NON_LEAR_YR to DAYS4MM_NON_LEAP_YR. Incorrect variable name caused 2025 calendar code not
#                           to work. Re-worked if statment which set DAYS4MM.
# Paul Baranoski 2025-06-06 Modify error message to include CONFIG Bucket value.
######################################################################################
set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
LOGNAME=/app/IDRC/XTR/CMS/logs/BuildRunExtCalendar_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

#############################################################
# Constants
#############################################################
# M-F
WORKING_DAYS_MASK="12345"
FLD_DELIM="|"

MON_ABREVS="JANFEBMARAPRMAYJUNJULAUGSEPOCTNOVDEC"
MON_ABREVS_DELIM="JAN,FEB,MAR,APR,MAY,JUN,JUL,AUG,SEP,OCT,NOV,DEC"

DAYS4MM_NON_LEAP_YR="31|28|31|30|31|30|31|31|30|31|30|31"
DAYS4MM_LEAP_YR="31|29|31|30|31|30|31|31|30|31|30|31"
DAYS4MM=""
VALID_DOM_LF_VALUES="LWFWLDFD"

RUN_CONFIG_FILE=CalendarConfigFile.csv

#############################################################
# Start process
#############################################################
touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "BuildRunExtCalendar.sh started at `date` " >> ${LOGNAME}


#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh >> ${LOGNAME}

echo "" >> ${LOGNAME}
echo "CALENDAR_BUCKET=${CALENDAR_BUCKET}" >> ${LOGNAME}


##################################################################
# Verify that parameter year has been passed.
##################################################################
if ! [[ $# -eq 1 ]]
then
	echo "Incorrect # of parameters sent to script. NOF parameters: $#" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="BuildRunExtCalendar.sh  - Failed (${ENVNAME})"
	MSG="Incorrect # of parameters sent to script. NOF parameters: $#. Process failed. "
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12		
			
fi


#############################################################
# Display parameters passed to script 
#############################################################
ProcessingYYYY=$1

echo " " >> ${LOGNAME}
echo "Parameters to script: " >> ${LOGNAME}
echo "NOF parameters for script: " $# >> ${LOGNAME}
echo "   ProcessingYYYY=${ProcessingYYYY} " >> ${LOGNAME}

# Set output filename
RUN_CALENDAR_OUTPUT_FILE=RunCalendar_${ProcessingYYYY}.txt


########################################################
# Perform clean-up of linux data directory 
########################################################
echo " " >> ${LOGNAME}
echo "Remove residual work files" >> ${LOGNAME}
rm 	${DATADIR}${RUN_CALENDAR_OUTPUT_FILE}  2>> ${LOGNAME}
rm 	${DATADIR}${RUN_CONFIG_FILE}           2>> ${LOGNAME}


#############################################################
# Download configuration file 
#############################################################
echo "" >> ${LOGNAME}
echo "Copy ${RUN_CONFIG_FILE} configuration file from S3 to Linux data directory" >> ${LOGNAME}

aws s3 cp s3://${CONFIG_BUCKET}${RUN_CONFIG_FILE} ${DATADIR}${RUN_CONFIG_FILE}  1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Copying S3 s3://${CONFIG_BUCKET}${RUN_CONFIG_FILE} configuration file to Linux failed." >> ${LOGNAME}
	
	# Send Failure email
	SUBJECT="BuildRunExtCalendar.sh  - Failed (${ENVNAME})"
	MSG="Copying S3 ${RUN_CONFIG_FILE} configuration file to Linux failed. "
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	


########################################################
# Is processing year a leap year? 
########################################################
date -d "${ProcessingYYYY}-02-29" >> ${LOGNAME}  2>&1

if [ $? = 0 ];then
	Y_DAYS=366
	DAYS4MM=${DAYS4MM_LEAP_YR}
else
	Y_DAYS=365
	DAYS4MM=${DAYS4MM_NON_LEAP_YR}
fi

echo "" >> ${LOGNAME}
echo "Y_DAYS=${Y_DAYS}" >> ${LOGNAME}
echo "DAYS4MM=${DAYS4MM}"  >> ${LOGNAME}


#####################################################################################
# Function definitions 
#####################################################################################
function monthnumber() {

	echo ""  >> ${LOGNAME}
	echo "In function monthnumber"  >> ${LOGNAME}  
	
    p_searchMon=$1
	
	# Upper case month abbrev parm
	searchMon=`echo ${p_searchMon} | tr "[a-z]" "[A-Z]" `  2>> ${LOGNAME} 

	# Find offset in Lookup String
    offset=`echo ${MON_ABREVS} | grep -bo "${searchMon}" | cut -d: -f1 ` 2>> ${LOGNAME} 
	
	# convert offset to MM number
    monthNbr=$((offset/3+1))
    MMFormatted=`printf "%02d\n" $monthNbr `
	
	echo "MMFormatted=${MMFormatted}"  >> ${LOGNAME} 
	echo "Leaving function monthnumber"  >> ${LOGNAME} 

}

function buildDaysMatchMask() { 

	echo ""  >> ${LOGNAME}
	echo "In function buildDaysMatchMask" >> ${LOGNAME}
	
	DOW_parm=$1
	
	#echo "DOW_parm=${DOW_parm}"

	# initialize match Mask
	REQ_DAYS_MASK=""
	
	if [ "${DOW_parm}" = "M-F" ];
	then
		REQ_DAYS_MASK=${WORKING_DAYS_MASK}	
	else
		# parse days by delimiter (;)
		# MON;TUE;WED;THU;FRI;SAT;SUT
		
		DaysString=$(echo ${DOW_parm} | tr ',' ' ')
		echo "DaysString=${DaysString}"  >> ${LOGNAME} 
		
		DAYS_ARRAY=(${DaysString})
		#echo "Number of elements in the array: ${#DAYS_ARRAY[@]}"

		
		# Build Days Mask
		for (( idx=0 ; idx < ${#DAYS_ARRAY[@]}; idx++ ))
		do
		    DAY=${DAYS_ARRAY[${idx}]}
			echo "DAY=${DAY}"  >> ${LOGNAME} 
			
			# convert Days array to use 3-char day names
			
			case ${DAY} in
				MON)
					REQ_DAYS_MASK="${REQ_DAYS_MASK}1"
					echo "M REQ_DAYS_MASK"  >> ${LOGNAME} 
					;;
				TUE)
					REQ_DAYS_MASK="${REQ_DAYS_MASK}2"				
					;;
					
				WED)
					REQ_DAYS_MASK="${REQ_DAYS_MASK}3"
					;;
					
				THU)
					REQ_DAYS_MASK="${REQ_DAYS_MASK}4"
					;;
				
				FRI)
					REQ_DAYS_MASK="${REQ_DAYS_MASK}5"
					;;	
				SAT)
					REQ_DAYS_MASK="${REQ_DAYS_MASK}6"
					;;	
				SUN)
					REQ_DAYS_MASK="${REQ_DAYS_MASK}0"
					;;	
				*)
					;;	
			
			esac	
			
		done
	fi
	
	echo "REQ_DAYS_MASK=${REQ_DAYS_MASK}"  >> ${LOGNAME} 
	
}	

function getMatchingDOWDate() 
{
	#################################################################################################################	
	# This function will find the 1st, 2nd, 3rd, last Day-of-week (FRI, MON, etc) and set that as the return date.
	# parmStartDt =  (YYYY-MM-01 format) or (YYYY-MM-31 - last day of month) 
	# parmDOWMask = 0-6 reprsenting the day(s) of the week like 5=Fri. "12345" -> every work day; "15" --> Mon and Fri' 
	#           "5" --> Friday
	# parmOcc = 1,2,3 (1st, 2nd, 3rd); 'FW' or 'FD' --> parmOCC = '1' (sign=+); 'LW' or 'LD' --> parmOCC = '1' (sign=-)
	# parmSign = '+' or '-'
	#
	#################################################################################################################	
		
	echo ""  >> ${LOGNAME}
	echo "In function getMatchingDOWDate"  >> ${LOGNAME}

	parmStartDt=$1
	parmDOWMask=$2
	parmOcc=$3
	parmSign=$4
	
	echo "parmStartDt=${parmStartDt}"  >> ${LOGNAME}
	echo "parmDOWMask=${parmDOWMask}"  >> ${LOGNAME}
	echo "parmOcc=${parmOcc}"  >> ${LOGNAME}
	echo "parmSign=${parmSign}"  >> ${LOGNAME}
	
	###########################################################
	# if parmStartDt the search dow? (like Fri) --> skip loop
	# if not dow looking for --> find 
	###########################################################
	NOF_Occ=0

	echo "" >> ${LOGNAME}
	echo "starting for loop" >> ${LOGNAME}

	for days_sub in {0..31}
	do
		# get parm date's day of week
		# dowNbr=date -d 2024-09-04 +%w
		CALC_DATE=`date -d "${parmStartDt} ${parmSign}${days_sub} days" +%Y-%m-%d`

		dow_nbr=`date -d "${parmStartDt} ${parmSign}${days_sub} day" +%w`

	    echo "" >> ${LOGNAME}
		echo "CALC_DATE=${CALC_DATE}" >> ${LOGNAME}
		echo "dow_nbr=${dow_nbr}"  >> ${LOGNAME}

		# is the date dow = requested DOW?
		NOFDOWMatches=`echo "${parmDOWMask}" | grep -c "${dow_nbr}"`
		echo "NOFDOWMatches=${NOFDOWMatches}"  >> ${LOGNAME}

		# date dow = requested DOW
		if  [ ${NOFDOWMatches} -gt 0 ];then
			NOF_Occ=`expr ${NOF_Occ} + 1`
			echo "NOF_Occ=${NOF_Occ}" >> ${LOGNAME}

			if [ ${NOF_Occ} -eq ${parmOcc} ];then
				echo "NOF OCC criteria met" >> ${LOGNAME}
				# criteria satisfied
				break
			fi
		fi

	done

	# caller will use CALC_DATE
	echo "CALC_DATE=${CALC_DATE}"  >> ${LOGNAME}
	
}

function buildWkCal4Yr() {

	echo ""  >> ${LOGNAME}
	echo "In function buildWkCal4Yr" >> ${LOGNAME}
	
	# output record = input config record
	p_out_rec=$1
	echo "p_out_rec=${p_out_rec}"  >> ${LOGNAME} 

	# Set date to first day of year
	StartDate=${ProcessingYYYY}-01-01
	echo "StartDate=${StartDate}"  >> ${LOGNAME} 
	
	# loop thru days of year to create appropriate calendar records
	for iDays in {0..365..1}
	do

		# calculate date
		nextDt=`date -d "${StartDate} ${iDays} days" +%Y-%m-%d`
		
		# skip if year is not for current processing year
		nextDtYYYY=`echo ${nextDt} | cut -c1-4 `
		# if next year --> exit loop
		if  [ ${nextDtYYYY} -ne ${ProcessingYYYY} ];
		then
			break
		fi
		
		# get day of week
		dow_nbr=`date -d ${nextDt} +%w ` 2>> ${LOGNAME} 
		#echo "dow_nbr=${dow_nbr}"  >> ${LOGNAME} 
		
		# if day of week we need --> create date
		bCrRec=`echo "${REQ_DAYS_MASK}" | grep ${dow_nbr} ` 2>> ${LOGNAME} 	
		#echo "bCrRec=${bCrRec}"  >> ${LOGNAME} 

		########################################################
		# Build Calendar record --> append calendar info to 
		#                           config record
		########################################################	
		if  [ "${bCrRec}" != "" ];then
			echo "extDt=${nextDt}"  >> ${LOGNAME} 
			dowAbbrev=`date -d ${nextDt} +%a `
			
			# output record and add Extract day and NOD
			echo "${nextDt}${FLD_DELIM}${dowAbbrev}${FLD_DELIM}${p_out_rec}"  >> ${DATADIR}${RUN_CALENDAR_OUTPUT_FILE}
		fi

	done
	
}


function buildQtrCal4Yr() {

	###################################################
	# p_Months like: "JAN,APR,JUL,OCT" or "JAN,JUL"
	# p_Month_Day: 2-digit day number 
	#             LW,FW,LD,FD,
	#             "FRI-2" (2nd FRI) "FRI-L" (last FRI)
	#             "FRI-F" (first FRI)
	###################################################
	echo ""  >> ${LOGNAME}
	echo "In function buildQtrCal4Yr"  >> ${LOGNAME} 
	
	p_Months=$1
	p_Month_Day=$2
	p_out_rec=$3
	
	echo "p_Months=${p_Months}"   >> ${LOGNAME} 
	echo "p_Month_Day=${p_Month_Day}"   >> ${LOGNAME} 

	###################################################	
	# Convert Months comma-delimited string to space-delimited
	# Ex. "JAN,APR,JUL,OCT" --> "JAN APR JUL OCT"
	###################################################
	MonthsString=$(echo ${p_Months} | tr ',' ' ')
	echo "MonthsString=${MonthsString}"   >> ${LOGNAME} 	

	###################################################	
	# Validate Month_Day
	# valid values --> month number (DD)
	#                 (LW|FW|FD|LD) 
	#                 (FRI-2|FRI-L) etc.
	###################################################
	if [ `echo ${p_Month_Day} | egrep '^[0-9]+$'` ];then
		echo "Valid month number" >> ${LOGNAME}
		
	elif [ `echo ${p_Month_Day} | egrep '^(SUN|MON|TUE|WED|THU|FRI|SAT)-(1|2|3|4|L|F)$' ` ];then
		# is format like "FRI=2"?
		echo "Valid Day and Occurrence - Ex. FRI-2" >> ${LOGNAME}
		
	elif [ `echo ${VALID_DOM_LF_VALUES} | grep ${p_Month_Day} ` ];then
		# is it a valid DOM value? (LW|FW|FD|LD)
		echo "Valid value (LW|FW|FD|LD)" >> ${LOGNAME}

	else		
		echo "Invalid DOM value ${p_Month_Day} in config file record ${p_out_rec}" >> ${LOGNAME} 	
	
		# Send Failure email	
		SUBJECT="BuildRunExtCalendar.sh  - Failed (${ENVNAME})"
		MSG="Invalid DOM value ${p_Month_Day} in config file record ${p_out_rec}. Process failed. "
		${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

		exit 12

	fi
	
	###################################################	
	# Loop thru Months array
	###################################################
	MON_ARRAY=(${MonthsString})
	echo "Number of elements in the array: ${#MON_ARRAY[@]}"   >> ${LOGNAME} 
	

	for (( idxMon=0 ; idxMon < ${#MON_ARRAY[@]}; idxMon++ ))
	do
		echo "" >> ${LOGNAME} 
		echo "*------------------------------*" >> ${LOGNAME} 
		echo "idxMon=${idxMon}" >> ${LOGNAME} 
		
		MON=${MON_ARRAY[${idxMon}]}
		echo "MON=${MON}"  >> ${LOGNAME} 

		# Convert Month abbrev to month number
		monthnumber "${MON}"
		echo "MMFormatted=${MMFormatted}" >> ${LOGNAME}   

		#################################################
		# Build date using Month number and month day
		# or Get appropriate date
		# --> Build date in YYYY-MM-DD format
		#################################################
		if [ "${p_Month_Day}" = "LW" ];then
			# last working day for month

			dd=`echo ${DAYS4MM} | cut -d'|' -f${MMFormatted} `  2>> ${LOGNAME}  
			echo "dd=${dd}"  >> ${LOGNAME} 
			
			parmStartDt="${ProcessingYYYY}-${MMFormatted}-${dd}"
			parmDOWMask=${WORKING_DAYS_MASK}
			parmOcc=1
			parmSign=-
				
			getMatchingDOWDate ${parmStartDt} ${parmDOWMask} ${parmOcc} ${parmSign}

			QtrDate=${CALC_DATE}				

		elif [ "${p_Month_Day}" = "FW" ];then			
			# first working day for month

			parmStartDt="${ProcessingYYYY}-${MMFormatted}-01"
			parmDOWMask=${WORKING_DAYS_MASK}
			parmOcc=1
			parmSign=+
				
			getMatchingDOWDate ${parmStartDt} ${parmDOWMask} ${parmOcc} ${parmSign}

			QtrDate=${CALC_DATE}

		elif [ "${p_Month_Day}" = "LD" ];then	
			# find last day for month

			dd=`echo ${DAYS4MM} | cut -d'|' -f${MMFormatted} ` 2>> ${LOGNAME}  
			echo "dd=${dd}"  >> ${LOGNAME} 
			
			# Set Extract date variable	
			QtrDate="${ProcessingYYYY}-${MMFormatted}-${dd}"

		elif [ "${p_Month_Day}" = "FD" ];then				
			# find first day for month
	
			QtrDate="${ProcessingYYYY}-${MMFormatted}-${01}"

		elif [ `echo ${p_Month_Day} | egrep '^(SUN|MON|TUE|WED|THU|FRI|SAT)-(1|2|3|4|L|F)$' ` ];then
			# Ex. (FRI-2) --> 2nd FRI of month
			
			# separate DOW_DAY from modifier
			DOW_DAY=`echo ${p_Month_Day} | cut -d- -f1 ` 2>> ${LOGNAME} 
			DOW_MODIFIER=`echo ${p_Month_Day} | cut -d- -f2 ` 2>> ${LOGNAME}
			
			echo "DOW_DAY=${DOW_DAY}"  >> ${LOGNAME}
			echo "DOW_MODIFIER=${DOW_MODIFIER}"  >> ${LOGNAME}

			# Build DOW mask
			buildDaysMatchMask ${DOW_DAY}
			parmDOWMask=${REQ_DAYS_MASK}
			
			# Set parms for function
			if [ "${DOW_MODIFIER}" = "L" ];then
				dd=`echo ${DAYS4MM} | cut -d'|' -f${MMFormatted} `  2>> ${LOGNAME}  
				echo "dd=${dd}"  >> ${LOGNAME} 
			
				parmStartDt="${ProcessingYYYY}-${MMFormatted}-${dd}"
				parmOcc=1			
				parmSign=-

			elif [ "${DOW_MODIFIER}" = "F" ];then
				parmStartDt="${ProcessingYYYY}-${MMFormatted}-01"
				parmOcc=1	
				parmSign=+				
			
			else
				parmStartDt="${ProcessingYYYY}-${MMFormatted}-01"
				parmOcc=${DOW_MODIFIER} 
				parmSign=+
			fi

			# Get matching DOW	
			getMatchingDOWDate ${parmStartDt} ${parmDOWMask} ${parmOcc} ${parmSign}

			QtrDate=${CALC_DATE}
			
		else
			# p_Month_Day is number 

			# !!!!If p_Month_Day is > NOF days per month --> substitute correct NOF days per month
			dd=`echo ${DAYS4MM} | cut -d'|' -f${MMFormatted} `  2>> ${LOGNAME}  
			echo "dd=${dd}"  >> ${LOGNAME} 

			if [ ${p_Month_Day} -gt ${dd} ];then
				parmStartDt="${ProcessingYYYY}-${MMFormatted}-${dd}"
			else
				parmStartDt="${ProcessingYYYY}-${MMFormatted}-${p_Month_Day}"
			fi
			
			# find nearest prior working day for config day
			parmDOWMask=${WORKING_DAYS_MASK}
			parmOcc=1
			parmSign=-
				
			getMatchingDOWDate ${parmStartDt} ${parmDOWMask} ${parmOcc} ${parmSign}

			QtrDate=${CALC_DATE}
			
		fi
		
		echo "QtrDate=${QtrDate}"  >> ${LOGNAME} 

		#################################################
		# Build output record	
		#################################################		
		dowAbbrev=`date -d ${QtrDate} +%a `		
		# output record and add Extract day and NOD
		echo "${QtrDate}${FLD_DELIM}${dowAbbrev}${FLD_DELIM}${p_out_rec}"  >> ${DATADIR}${RUN_CALENDAR_OUTPUT_FILE}
		
	done		

	echo "Leaving function buildQtrCal4Y"  >> ${LOGNAME} 

}


#####################################################################################
# Start processing of config file
#####################################################################################

########################################################
# Loop thru config file records   
########################################################
while read config_rec 
do
	echo ""  >> ${LOGNAME} 
	echo "*********************************************"  >> ${LOGNAME} 
	echo "config_rec=${config_rec}"  >> ${LOGNAME} 
	
	####################################################
	# parse input record
	# Example: Blbtn,Blue Button,W,M;F,,,N,EFT 
	####################################################
	ExtractID=`echo ${config_rec} | awk -F'|', '{print $1}' ` 
	Ext_Desc=`echo ${config_rec} | awk -F'|' '{print $2}' `
	TimeFrame=`echo ${config_rec} | awk -F'|' '{print $3}' `
	DOW_DOM=`echo ${config_rec} | awk -F'|' '{print $4}' `
	Months=`echo ${config_rec} | awk -F'|' '{print $5}' `
	Month_Day=`echo ${config_rec} | awk -F'|' '{print $6}' `
	FinderFileReq=`echo ${config_rec} | awk -F'|' '{print $7}' `
	FF_Pre_Processing=`echo ${config_rec} | awk -F'|' '{print $8}' `
	DeliveryMethod=`echo ${config_rec} | awk -F'|' '{print $9}' `
	
	####################################################
	# Create year calendar records for extract
	####################################################
	echo "" >> ${LOGNAME}
	echo "TimeFrame=${TimeFrame}"  >> ${LOGNAME} 
	
	case ${TimeFrame} in 
	
		W)
			echo "DOW_DOM=${DOW_DOM}"  >> ${LOGNAME} 
			
			buildDaysMatchMask "${DOW_DOM}"
			buildWkCal4Yr "${config_rec}"
			;;
		M)
			buildQtrCal4Yr "${MON_ABREVS_DELIM}" "${DOW_DOM}" "${config_rec}"
			;;
			
		Q|S|A) 
			buildQtrCal4Yr "${Months}" "${Month_Day}" "${config_rec}"
			;;
			
		*)
 			echo "Invalid extract time frame: ${TimeFrame} in config file record ${config_rec}" >> ${LOGNAME} 	
		
			# Send Failure email	
			SUBJECT="BuildRunExtCalendar.sh  - Failed (${ENVNAME})"
			MSG="Invalid extract time frame: ${TimeFrame} in config file record ${config_rec}. Process failed. "
			${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

			exit 12			
			;;
		
	esac

done < ${DATADIR}${RUN_CONFIG_FILE}


#################################################################################
# Move Calendar output file to S3 
#################################################################################
echo "" >> ${LOGNAME}
echo "Move Calendar output file ${RUN_CALENDAR_OUTPUT_FILE} to s3" >> ${LOGNAME}

aws s3 mv ${DATADIR}${RUN_CALENDAR_OUTPUT_FILE} s3://${CALENDAR_BUCKET}${RUN_CALENDAR_OUTPUT_FILE}   1>> ${LOGNAME} 2>&1

RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Moving Calendar output file ${S3FinderFilename} to S3 folder ${CALENDAR_BUCKET} failed." >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="BuildRunExtCalendar.sh  - Failed (${ENVNAME})"
	MSG="Moving Calendar output file ${S3FinderFilename} to S3 folder ${CALENDAR_BUCKET} failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi	

#############################################################
# export variables for python code  
#############################################################
export RUN_CALENDAR_OUTPUT_FILE
export ProcessingYYYY

#############################################################
# Execute python script  
#############################################################
echo "Start execution of BuildRunExtCalendar.py program"  >> ${LOGNAME}
${PYTHON_COMMAND} ${RUNDIR}BuildRunExtCalendar.py >> ${LOGNAME} 2>&1


#############################################################
# Check the status of python script  
#############################################################
RET_STATUS=$?

if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Python script BuildRunExtCalendar.py failed" >> ${LOGNAME}
	
	# Send Failure email	
	SUBJECT="BuildRunExtCalendar.py - Failed ($ENVNAME)"
	MSG="Python script BuildRunExtCalendar.py failed."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1

	exit 12
fi

echo "" >> ${LOGNAME}
echo "Python script BuildRunExtCalendar.py completed successfully. " >> ${LOGNAME}


########################################################
# Success email.
########################################################
# Send Failure email	
SUBJECT="BuildRunExtCalendar.sh completed successfully. ($ENVNAME)"
MSG="BuildRunExtCalendar.sh completed successfully."
${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${CMS_EMAIL_SENDER}" "${ENIGMA_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1


########################################################
# clean-up of linux data directory 
########################################################
echo " " >> ${LOGNAME}
echo "Script clean-up of files" >> ${LOGNAME}
rm 	${DATADIR}${RUN_CALENDAR_OUTPUT_FILE}  2>> ${LOGNAME}
rm 	${DATADIR}${RUN_CONFIG_FILE}           2>> ${LOGNAME}

	
########################################################
# script completed.
########################################################
echo " " >> ${LOGNAME}
echo "Script BuildRunExtCalendar.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit 0
