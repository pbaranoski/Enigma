#!/usr/bin/sh
#
######################################################################################
# Name: CleanUpDirs.sh
# Desc: Clean-up/remove obsolete files from data and log directories. Do NOT drill down to sub-directories.
#       Remove files .txt, .csv. and .log files that are over 30 days old.
#
# Execute as ./CleanUpDirs.sh 
#
# Created: Paul Baranoski  07/11/2023
#
# Modified: 
#
# Paul Baranoski 2023-07-11 Created script.
######################################################################################

######################################################################################
set +x


#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP:=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/CleanUpDirs_${TMSTMP}.log

RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/
LOGDIR=/app/IDRC/XTR/CMS/logs/

touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "" >> ${LOGNAME}
echo "################################### " >> ${LOGNAME}
echo "CleanUpDirs.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh >> ${LOGNAME}

echo "DATADIR=${DATADIR} " >> ${LOGNAME}
echo "LOGDIR=${LOGDIR} " >> ${LOGNAME}


#############################################################
# Display Disk Usage
#############################################################
echo "" >> ${LOGNAME}
df -h ${DATADIR} >> ${LOGNAME}

echo "" >> ${LOGNAME}
df -h ${LOGDIR} >> ${LOGNAME}


#############################################################
# Display contents of $DATADIR -before
#############################################################
echo "" >> ${LOGNAME}
echo "Display contents of dir ${DATADIR} - BEFORE " >> ${LOGNAME}

ls -l ${DATADIR}  >> ${LOGNAME}

#############################################################
# Remove .txt files from data dir that are more than 30 days old
#############################################################
echo "" >> ${LOGNAME}
echo "Remove .txt files that are older than 30 days from ${DATADIR}  " >> ${LOGNAME}

NOF_FILES=`find ${DATADIR} -maxdepth 1 -type f -mtime +30 -name "*.txt" | wc -l `  2>> ${LOGNAME}
echo "${NOF_FILES} .txt files found to remove from ${DATADIR}  " >> ${LOGNAME}

if [ ${NOF_FILES} -gt 0 ]; then
	find ${DATADIR} -maxdepth 1 -type f -mtime +30 -name "*.txt" | xargs rm   >> ${LOGNAME} 2>&1
fi


#############################################################
# Remove .csv files from data dir that are more than 30 days old
#############################################################
echo "" >> ${LOGNAME}
echo "Remove .csv files that are older than 30 days from ${DATADIR}  " >> ${LOGNAME}

NOF_FILES=`find ${DATADIR} -maxdepth 1 -type f -mtime +30 -name "*.csv" | wc -l `  2>> ${LOGNAME}
echo "${NOF_FILES} .csv files found to remove from ${DATADIR}  " >> ${LOGNAME}

if [ ${NOF_FILES} -gt 0 ]; then
	find ${DATADIR} -maxdepth 1 -type f -mtime +30 -name "*.csv" | xargs rm   >> ${LOGNAME} 2>&1
fi

#############################################################
# Display contents of $DATADIR -after
#############################################################
echo "" >> ${LOGNAME}
echo "Display contents of dir ${DATADIR} - AFTER" >> ${LOGNAME}
ls -l ${DATADIR}  >> ${LOGNAME}


#############################################################
# Display contents of $DATADIR -before
#############################################################
echo "" >> ${LOGNAME}
echo "" >> ${LOGNAME}
echo "Display contents of dir ${LOGDIR} - BEFORE " >> ${LOGNAME}

ls -l ${LOGDIR}  >> ${LOGNAME}

#############################################################
# Remove .txt files from data dir that are more than 30 days old
#############################################################
echo "" >> ${LOGNAME}
echo "Remove files older than 30 days from ${LOGDIR} " >> ${LOGNAME}

NOF_FILES=`find ${LOGDIR} -maxdepth 1 -type f -mtime +30 -name "*.log" | wc -l `  2>> ${LOGNAME}
echo "${NOF_FILES} .log files found to remove from ${LOGDIR}  " >> ${LOGNAME}

if [ ${NOF_FILES} -gt 0 ]; then
	find ${LOGDIR} -maxdepth 1 -type f -mtime +30 -name "*.log" | xargs rm  >> ${LOGNAME} 2>&1
fi

#############################################################
# Display contents of $DATADIR -after
#############################################################
echo "" >> ${LOGNAME}
echo "Display contents of dir ${LOGDIR} - AFTER" >> ${LOGNAME}
ls -l ${LOGDIR}  >> ${LOGNAME}


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "CleanUpDirs.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS
