#!/usr/bin/bash
######################################################################################
# Name:  KIAProcess.sh
#
# Desc: Kill a process. Run ListXTRProcess.sh to get list of idrcxtr processes. 
#       Use that to identify process to kill.
#
# Execute as ./KIAProcess.sh $1 
#
# $1 = script name to kill  (e.g., ReleaseHeldManifestFiles.sh)
#
# Created: Paul Baranoski  10/11/2023
# Modified:
#
# Paul Baranoski 2023-10-11 Created script. 
#
######################################################################################
set +x


#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
LOGNAME=/app/IDRC/XTR/CMS/logs/KIAProcess_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/

	
	
touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "KIAProcess.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}


##################################################################
# Verify that required NOF parameters have been sent from RunDeck
##################################################################
if ! [[ $# -eq 1 ]]
then
	echo "Incorrect # of parameters sent to script. NOF parameters: $#" >> ${LOGNAME}
	exit 12
fi


#############################################################
# Display parameters passed to script 
#############################################################
scriptName2Kill=$1

echo " " >> ${LOGNAME}
echo "Parameters to script: " >> ${LOGNAME}
echo "        scriptName2Kill=${scriptName2Kill} " >> ${LOGNAME}

#############################################################
# get ProcessID of script 2 kill 
#############################################################
echo " " >> ${LOGNAME}
echo "Get process ID to kill " >> ${LOGNAME}

ProcessID2Kill=`ps -ux | grep "bash" | grep "${RUNDIR}${scriptName2Kill}" | awk '{print $2}' ` >> ${LOGNAME}  2>&1
echo "ProcessID2Kill=${ProcessID2Kill}" >> ${LOGNAME}

 
#############################################################
# Kill process 
#############################################################
echo " " >> ${LOGNAME}
echo "Killing processID ${ProcessID2Kill}" >> ${LOGNAME}
kill ${ProcessID2Kill}  >> ${LOGNAME}


#############################################################
# wait for kill to happen 
#############################################################
echo " " >> ${LOGNAME}
echo "sleeping 10 seconds to let kill take effect " >> ${LOGNAME}

sleep 10


#############################################################
# Display parameters passed to script 
#############################################################
echo " " >> ${LOGNAME}
echo "show idrxctr currently running processes: " >> ${LOGNAME}
echo "USER       PID %CPU %MEM    VSZ   RSS TTY      STAT START   TIME COMMAND" >> ${LOGNAME}
ps -ux  >> ${LOGNAME}

  
#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "KIAProcess.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS
