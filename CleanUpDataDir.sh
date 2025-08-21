#!/usr/bin/bash


filename=$1

TMSTMP=`date +%Y%m%d.%H%M%S`
DATADIR=/app/IDRC/XTR/CMS/data/
LOGNAME=/app/IDRC/XTR/CMS/logs/CleanUpDataDir_${TMSTMP}.log

echo "rm ${DATADIR}${filename}*"  >> ${LOGNAME}
rm ${DATADIR}${filename}*  2>> ${LOGNAME}