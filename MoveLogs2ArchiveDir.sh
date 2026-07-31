#!/usr/bin/bash

############################################################################################################
# Name:  MoveLogs2ArchiveDir.sh
#
# Desc: Move obsolete log files to logs/archive directory. Takes a long time to refresh logs directory in WinSCP. 
#
# Author     : Paul Baranoski	
# Created    : 04/09/2026
#
# Modified:
#
# Paul Baranoski 2026-04-09 Create script.
############################################################################################################

TMSTMP=`date +%Y%m%d.%H%M%S`
LOGDIR=/app/IDRC/XTR/CMS/logs/
LOGNAME=/app/IDRC/XTR/CMS/logs/CleanUpLogsMove2Archive_${TMSTMP}.log

DaysOld=$1

echo "DaysOld=${DaysOld}"  >> ${LOGNAME}


cd ${LOGDIR}
echo "pwd=`pwd`"  >> ${LOGNAME}

# Create archive folder is it does exist
mkdir -m 775 -p archive

echo ""  >> ${LOGNAME}
echo "Log files we are moving to the archive folder ..."  >> ${LOGNAME}

find . -maxdepth 1 -type f -mtime +"${DaysOld}"  >> ${LOGNAME}

echo ""  >> ${LOGNAME}
echo "Issuing find Log files for mv to archive folder Command"  >> ${LOGNAME}

# preserve original log file meta-data on cp
#echo "find . -maxdepth 1 -type f -mtime +"${DaysOld}" -exec cp -p -t ${LOGDIR}/archive {} \;"  >> ${LOGNAME}

# migrate old log files in logs directory to archive folder. 
find . -maxdepth 1 -type f -mtime +"${DaysOld}" -exec mv -t ${LOGDIR}/archive {} \;  >> ${LOGNAME} 2>&1

echo "Files have been moved to archive folder"  >> ${LOGNAME}

