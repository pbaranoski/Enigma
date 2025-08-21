#!/usr/bin/bash

############################################################################################################
# Name:  CleanUpLogsDir.sh
#
# Desc: Clean-up obsolete log files. Takes a long time to refresh logs directory in WinSCP. 
#
# Author     : Paul Baranoski	
# Created    : 01/27/2025
#
# Modified:
#
# Paul Baranoski 2025-01-27 Create script.
############################################################################################################

TMSTMP=`date +%Y%m%d.%H%M%S`
LOGDIR=/app/IDRC/XTR/CMS/logs/
LOGNAME=/app/IDRC/XTR/CMS/logs/CleanUpLogsDir_${TMSTMP}.log


filename_wildcard=$1
DaysOld=$2

#filename_wildcard="*_SF.2025*.log"

echo "filename_wildcard=${filename_wildcard}" >> ${LOGNAME}
echo "DaysOld=${DaysOld}"  >> ${LOGNAME}


cd ${LOGDIR}
echo "pwd=`pwd`"  >> ${LOGNAME}

echo ""  >> ${LOGNAME}
echo "Log files we are deleting..."  >> ${LOGNAME}

find . -type f -mtime +"${DaysOld}" -name "${filename_wildcard}" >> ${LOGNAME}

echo ""  >> ${LOGNAME}
echo "Issuing find Log files delete Command"  >> ${LOGNAME}

find . -type f -mtime +"${DaysOld}" -name "${filename_wildcard}" -delete >> ${LOGNAME} 2>&1

