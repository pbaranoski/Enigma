#!/usr/bin/bash
#
######################################################################################
# Name:  CompCreateCSVFile.bash
#
# Desc: Script to compile C program(s)
#
# Created: Paul Baranoski  09/20/2023
#
# Paul Baranoski 2024-11-25 Add code to accept one paramter which is name of C pgm. 
######################################################################################

TMSTMP=`date +%Y%m%d.%H%M%S`
LOGNAME=/app/IDRC/XTR/CMS/logs/CompCreateCSVFile_${TMSTMP}.log
DIR=/app/IDRC/XTR/CMS/scripts/run/

echo "################################### " >> ${LOGNAME}
echo "CompCreateCSVFile.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

if ! [[ $# -eq 0 || $# -eq 1  ]]
then
	echo "Incorrect # of parameters sent to script. NOF parameters: $#" >> ${LOGNAME}
	exit 12
fi

#############################################################
# Display parameters passed to script 
#############################################################
CPgmName=$1

echo " " >> ${LOGNAME}
echo "Parameters to script: " >> ${LOGNAME}
echo "NOF parameters for script: " $# >> ${LOGNAME}
echo "   CPgmName=${CPgmName} " >> ${LOGNAME}


cd $DIR
pwd  >> ${LOGNAME}

cc -std=c99 createCSVFile.c -o createCSVFile.exe  >> ${LOGNAME}  

if [[ $? != 0 ]]; then
	echo "Compiling createCSVFile.c failed with bad RC. " >> ${LOGNAME}
	exit 12
fi

#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "CompCreateCSVFile.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $?