#!/usr/bin/sh
#
######################################################################################
# Name: CombineS3Files.sh 
# Desc: Combine/concatenate S3 Files into single file.
#
# Execute as ./CombineS3Files.sh $1 $2 
#
# $1 = S3 bucket/folder_name  Ex1: bucket=aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod/xtr/DEV/Blbtn/  
#                             Ex2: bucket=aws-hhs-cms-eadg-bia-ddom-extracts/xtr/Blbtn/
# $2 = S3 Combined filename   Ex:  PartB_Carrier_FINAL_2021_QTR1_20220922.084321.csv.gz
#
# Created: Paul Baranoski  09/22/2022
# Modified: 
# 
# Paul Baranoski 2023-06-05 Increase max filesize from 50GB to 60GB. This was done as
#                           quick fix since Josh's FFS extract generated 2 combine groups with file suffixes,
#                           and we don't yet have a solution on how to create EFT files from these files.
# Paul Baranoski 2023-06-07 Set the max filesize back to 50GB.
# Paul Baranoski 2023-06-16 Added timestamp to temp file so that each caller will have unique temp file, 
#                           and prevent overlay of file by another caller.
# Paul Baranoski 2023-09-14 Change logic to delete part files rather than move them to an archive folder. 
# Paul Baranoski 2025-03-28 Modify logic to bypass call to python combineS3Files.py module when NOF_PARTS_FILES = 0.
#                           This was done to avoid critical error in combineS3Files.py which causes parent shell script to fail.
######################################################################################

######################################################################################
set +x

#############################################################
# Establish log file  
#############################################################
TMSTMP=${TMSTMP:=`date +%Y%m%d.%H%M%S`}
LOGNAME=/app/IDRC/XTR/CMS/logs/CombineS3Files_${TMSTMP}.log
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
DATADIR=/app/IDRC/XTR/CMS/data/


touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "" >> ${LOGNAME}
echo "################################### " >> ${LOGNAME}
echo "CombineS3Files.sh started at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source ${RUNDIR}SET_XTR_ENV.sh >> ${LOGNAME}


#############################################################
# Display parameters passed to script 
#############################################################
S3BucketAndFldr=$1
combinedFilename=$2

echo "Parameters to script: " >> ${LOGNAME}
echo "   S3BucketAndFldr=${S3BucketAndFldr} " >> ${LOGNAME}
echo "   S3CombinedFilename=${combinedFilename} " >> ${LOGNAME}

#############################################################
# Extract the S3Bucket from S3BucketAndFldr
# aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod/xtr/DEV/Blbtn/ --> aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod
#############################################################
S3Bucket=`echo ${S3BucketAndFldr} | cut -d/ -f1`  2>> ${LOGNAME}

echo "" >> ${LOGNAME}
echo "S3Bucket=${S3Bucket}" >> ${LOGNAME}

#############################################################
# Extract the S3 FolderName from the S3BucketAndFldr
# aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod/xtr/DEV/Blbtn/ -->  xtr/DEV/Blbtn/
#############################################################
S3Folder=`echo ${S3BucketAndFldr} | cut -d/ -f2-` 2>> ${LOGNAME}
echo "S3Folder=${S3Folder}" >> ${LOGNAME}

##################################################################
# FilePrefix --> "I want to concatenate files that match this prefix/"wildcard "
#
# !!!! NOTE: The below "cut" command assumes that the first '.' appears in the timestamp
#            and there are no periods in the filename prior to what appears in the timestamp.
#            Filename should be like this: "PartB_Carrier_FINAL_2021_QTR1_20220922.084321.csv.gz"
#
# NOTE: I am assuming that all filenames will contain a timestamp at the end
#       of the filename AND we are not interested in anything after the time 
#       component in identifying the prefix/search criteria for files to concatenate.
#
# --> PartB_Carrier_FINAL_2021_QTR1_20220922.084321
#################################################################
S3FilePrefix=`echo ${combinedFilename} | cut -d. -f1-2` 2>> ${LOGNAME}
echo "S3FilePrefix=${S3FilePrefix}" >> ${LOGNAME}

#####################################################
# Set filesize of each concatenation
# 50 GB
#####################################################
#filesize=5368709120  -- 5GB
#filesize=10737418240 -- 10GB
#filesize=21474836480 -- 20GB
#filesize=26843545600 -- 25GB
filesize=53687091200  
#filesize=64424509440 -- 60GB

##################################################################
# Delete any residual combined/concatenated file before starting 
#    concatenation.
#
# NOTE: When Extract allows multiple files, there can be one S3 file
#       that will have suffix entension "_0_0_0" ). So, still want to 
#       create concatenated file without suffix.          
#
# NOTE: Get count of S3 files w/timestamp that  
#       have suffix (e.g. "_0_0_0." or "_31_0_7.") in filename.
#       There should be at least one.
##################################################################
echo "" >> ${LOGNAME}
echo "Get S3 Parts filenames for ${S3FilePrefix} " >> ${LOGNAME}

aws s3 ls s3://${S3Bucket}/${S3Folder}${S3FilePrefix} | awk '{print $4}' | egrep "_[0-9]{1,2}_[0-9]{1,2}_[0-9]{1,2}\." > ${DATADIR}S3PartsTemp_${TMSTMP}.txt   2>> ${LOGNAME} 

NOF_PARTS_FILES=`wc -l ${DATADIR}S3PartsTemp_${TMSTMP}.txt | awk '{print $1}' ` >> ${LOGNAME} 2>&1

echo "NOF S3 PARTS FILES=${NOF_PARTS_FILES}" >> ${LOGNAME}


##################################################################
# ONLY Run python concatenation program when there are parts files
##################################################################
if [ ${NOF_PARTS_FILES} -eq 0 ]; then
	echo "NOF S3 PARTS FILES=0. No combining of S3 files is needed. Call is bypassed." >> ${LOGNAME}

elif [ ${NOF_PARTS_FILES} -gt 0 ]; then
	echo "Deleting S3 file if exists=${S3Bucket}/${S3Folder}${combinedFilename}" >> ${LOGNAME}
	aws s3 rm s3://${S3Bucket}/${S3Folder}${combinedFilename} >> ${LOGNAME} 2>&1


	##################################################################
	# Run python concatenation program
	##################################################################
	echo "" >> ${LOGNAME}
	echo "Start Combining S3 files " >> ${LOGNAME}

	echo "" >> ${LOGNAME}
	echo "Run python program combineS3Files.py" >> ${LOGNAME}	

	${PYTHON_COMMAND} ${RUNDIR}combineS3Files.py --bucket ${S3Bucket} --folder ${S3Folder} --prefix ${S3FilePrefix} --output ${S3Folder}${combinedFilename} --filesize ${filesize}   >> ${LOGNAME} 2>&1

	RET_STATUS=$?

	if [[ $RET_STATUS != 0 ]]; then
		echo "" >> ${LOGNAME}
		echo "Executing python program combineS3Files.py failed." >> ${LOGNAME}

		exit 12
	fi	

	#############################################################
	# Move S3 Parts Files to Archive folder
	#############################################################
	echo "" >> ${LOGNAME}  
	#echo "Moving S3_PARTS_FILES to archive folder." >> ${LOGNAME}
	echo "Deleting S3_PARTS_FILES." >> ${LOGNAME}

	S3_PARTS_FILES=`cat ${DATADIR}S3PartsTemp_${TMSTMP}.txt`


	# combinedFilename     --> SAFENC_INP_FINAL_Y22QTR1_20230608.153723.txt.gz
	# parts filenames like --> SAFENC_INP_FINAL_Y22QTR1_20230608.153723.txt.gz_0_0_0.csv.gz

	# Mask includes "_" after combinedFilename
	# S3PartsFilenameMask=${combinedFilename}_

	#NOTE: s3 mv --recursive functionality is sometimes problematic with sub-folders   

	for S3PartsFilename in ${S3_PARTS_FILES}
	do

		#aws s3 mv s3://${S3Bucket}/${S3Folder}${S3PartsFilename} s3://${S3Bucket}/${S3Folder}archive/${S3PartsFilename}  1>> ${LOGNAME} 2>&1
		aws s3 rm s3://${S3Bucket}/${S3Folder}${S3PartsFilename}   1>> ${LOGNAME} 2>&1

		RET_STATUS=$?

		if [[ $RET_STATUS != 0 ]]; then
			echo "" >> ${LOGNAME}
			echo "Deleting S3 Parts file ${S3PartsFilename} to S3 archive folder failed." >> ${LOGNAME}

			exit 12
		fi
		
	done
	
fi


#############################################################
# remove unused files from linux data directory
#############################################################
echo " " >> ${LOGNAME}
echo "Delete temp file S3PartsTemp_${TMSTMP}.txt from linux data directory " >> ${LOGNAME}
rm ${DATADIR}S3PartsTemp_${TMSTMP}.txt  2>> ${LOGNAME}


#############################################################
# script clean-up
#############################################################
echo "" >> ${LOGNAME}
echo "CombineS3Files.sh completed successfully." >> ${LOGNAME}

echo "Ended at `date` " >> ${LOGNAME}
echo "" >> ${LOGNAME}
exit $RET_STATUS
