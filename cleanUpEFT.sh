#!/usr/bin/bash
#
######################################################################################
# Name:  cleanUpEFT.sh
#
# Desc: Clean-up EFT directory
#
# Created: Paul Baranoski  09/20/2023
######################################################################################

TMSTMP=`date +%Y%m%d.%H%M%S`
LOGNAME=/home/BZH3/cleanUpEFT_${TMSTMP}.log

echo "################################### " >> ${LOGNAME}
echo "cleanUpEFT.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

cd /home/BZH3/EFT

DIR=`pwd` 
DAYS_OLD=+3

echo "DIR=${DIR}" >> ${LOGNAME}
echo "DAYS_OLD=${DAYS_OLD}" >> ${LOGNAME}


#############################################################
# Disk usage 
#############################################################
echo "" >> ${LOGNAME}
echo "Disk usage before clean-up" >> ${LOGNAME}

df ${DIR} -hl >> ${LOGNAME}


#############################################################
# Before removing all files with node
#############################################################
echo "" >> ${LOGNAME}
echo "Before removing all old files" >> ${LOGNAME}

ls -l  >> ${LOGNAME}

#############################################################
# Removing all files with node
#############################################################
echo "" >> ${LOGNAME}
echo "Removing all old files " >> ${LOGNAME}

find . -mtime ${DAYS_OLD} | xargs rm  2>> ${LOGNAME}  

#############################################################
# After removing all files with node
#############################################################
echo "" >> ${LOGNAME}
echo "After removing all old files" >> ${LOGNAME}

ls -l  >> ${LOGNAME}

	
#############################################################
# clean-up files with non-EFT HLQ 
#############################################################
echo "" >> ${LOGNAME}
echo "Before non-EFT file processing" >> ${LOGNAME}

	
NODES=`ls | cut -d. -f1 | sort | uniq `  >> ${LOGNAME}
echo "NODES=${NODES}" >> ${LOGNAME}

for NODE in ${NODES}
do

	#############################################################
	# Skip Nodes
	#############################################################
	if [ "${NODE}" = "P#EFT" -o "${NODE}" = "T#EFT" ]; then
		continue
	fi
	
	#############################################################
	# Before removing all files with node
	#############################################################
	echo "" >> ${LOGNAME}
	echo "Before removing all files that start with node ${NODE}" >> ${LOGNAME}
	
	ls -l  >> ${LOGNAME}

	#############################################################
	# Removing all files with node
	#############################################################
	echo "" >> ${LOGNAME}
	echo "Removing all files that start with node ${NODE}" >> ${LOGNAME}

	rm ${NODE}.*   2>> ${LOGNAME}

	#############################################################
	# After removing all files with node
	#############################################################
	echo "" >> ${LOGNAME}
	echo "After removing all files that start with node ${NODE}" >> ${LOGNAME}
	
	ls -l  >> ${LOGNAME}
	

done


#############################################################
# Disk usage 
#############################################################
echo "" >> ${LOGNAME}
echo "" >> ${LOGNAME}
echo "Disk usage after clean-up" >> ${LOGNAME}

df ${DIR} -hl >> ${LOGNAME}


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "cleanUpEFT.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $?