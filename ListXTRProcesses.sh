#!/usr/bin/bash
############################################################################################################
# Name:  ListXTRProcesses.sh
#
# Desc: Create Manifest file required for transfers of Extract files to Outside Consumers using BOX 
#
# Execute as ./ListXTRProcesses.sh 
#
#
# 01/02/2024 Paul Baranoski   Created script.	
############################################################################################################

set +x

#############################################################
# Establish log file  
#############################################################
#TMSTMP = If TMSTMP value set by caller via export --> use that value. 
#         Else use the timestamp created in this script
TMSTMP=${TMSTMP:=`date +%Y%m%d.%H%M%S`}

LOGNAME=/app/IDRC/XTR/CMS/logs/ListXTRProcesses_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "ListXTRProcesses.sh started at `date` " >> ${LOGNAME}
echo " " >> ${LOGNAME}
echo "TMSTMP=${TMSTMP} " >> ${LOGNAME}


#############################################################
# Display parameters passed to script 
#############################################################
echo " " >> ${LOGNAME}
echo "show idrxctr currently running processes: " >> ${LOGNAME}
echo "USER       PID %CPU %MEM    VSZ   RSS TTY      STAT START   TIME COMMAND" >> ${LOGNAME}
ps -ux   >> ${LOGNAME}


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "ListXTRProcesses.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS