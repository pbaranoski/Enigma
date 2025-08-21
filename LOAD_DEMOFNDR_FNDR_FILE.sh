#!/usr/bin/sh
######################################################################################################
# Script Name: LOAD_DEMOFNDR_FNDR_FILE.sh
# Description: This script executes a python script that loads the FMR table DEMOFNDR_HICN_PLAN.
# Author     : Sumathi Gayam	
# Created    : 09/06/2022
######################################################################################################
####################################################################
# SET ENVIRONMENT VARIABLES
####################################################################
set +x
. /app/IDRC/XTR/CMS/scripts/run/SET_XTR_ENV.sh

####################################################################
#Log file creation
####################################################################

########################################################################
# TMSTMP variable is exported from DemoFinderFileExtracts.sh script
# This will allow for creation of a single PTA log file, 
# and all extract files will have the same timestamp, making
# it easier to find them in S3.
########################################################################

log_file="/app/IDRC/XTR/CMS/logs/LOAD_DEMOFNDR_FILE_${TMSTMP}.log"

RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/
LANDING=${DATADIR}

touch ${log_file}
chmod 764 ${log_file}

exec 1> $log_file 2>&1

####################################################################
# PRINT TIME TO LOG 
####################################################################
JobBeginTime=$(date +"%Y-%m-%d_%T")
echo ${JobBeginTime}

${PYTHON_COMMAND} ${RUNDIR}LOAD_DEMOFNDR_FNDR_FILE.py
rc=$?
if [[ $rc -ne 0 ]]; then
	JobEndTime=$(date +"%Y-%m-%d_%T")
	echo ${JobEndTime}: LOAD_DEMOFNDR_FNDR_FILE.py script failed with return code ${rc}
	exit $rc
fi
exit $rc
