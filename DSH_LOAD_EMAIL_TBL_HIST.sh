#!/usr/bin/bash
#

TEMP_DATA_DIR=/app/IDRC/XTR/CMS/data/DSHTemp/

# change directory to temp data directory
cd ${TEMP_DATA_DIR}

# print working directory
pwd

# clean temp directory of files
rm ${TEMP_DATA_DIR}*.*


# Retrieve DSH Request files to temp folder
aws s3 cp --recursive s3://aws-hhs-cms-eadg-bia-ddom-extracts/xtr/Finder_Files/archive/ ${TEMP_DATA_DIR}  --exclude="*" --include="DSH_REQUEST_*"

# Get list of .csv request files
DSH_REQ_FILES=`ls -1 *.csv `  

# loop thru files to capture RequestNode and corresponding email
for REQ_FILE in ${DSH_REQ_FILES}
do

	# if invalid DSH REQUEST File --> bypass file
	if ! [ `echo ${REQ_FILE} | egrep '^DSH_REQUEST_[a-zA-Z0-9-]+_[0-9]+\.(csv|CSV)$' ` ];then
		echo "By pass ${REQ_FILE} which is not a valid DSH request file"
		
		continue
	fi
	
	# Get Requestor which is the uniq-id node
	REQUESTER_NODE=`echo ${REQ_FILE} | cut -d_ -f3 `
	
	# Get the email address	- Assumes the request file has a header
	EMAIL=`sed -n '2,2p' ${REQ_FILE} | cut -d, -f4 `
	
	# if email address exists (not zero)	
	if [ -n "${EMAIL}" ];then
		echo "${REQUESTER_NODE},${EMAIL}" >> ${TEMP_DATA_DIR}RequestEmailLoad.csv
	fi
	

done 

sort -u ${TEMP_DATA_DIR}RequestEmailLoad.csv > ${TEMP_DATA_DIR}RequestEmailLoadUniq.csv


