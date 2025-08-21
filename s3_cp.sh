#!/usr/bin/sh

search=$1
bucket_type=$2
#search='PartB'

bucket_ptb=idrc-dev-datalake-pdl/pbar/landing/
bucket_xtr=aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod/xtr/DEV/

bucket=idrc-dev-datalake-pdl/pbar/landing/


if [ "${bucket_type}" = "xtr" ]; then
	bucket=${bucket_xtr}
	uid=idrcxtr

	# Get list of files in s3 bucket
	#echo "aws s3 ls s3://${bucket}"
	aws s3 ls s3://${bucket}  > temp.txt

else
	# point to config file
	AWS_CONFIG_FILE=/app/IDRC/PTB/CMS/tmp/aws.cfg

	bucket=${bucket_ptb}
	uid=idrcptb

	# Get list of files in s3 bucket
	aws s3 ls s3://${bucket} --profile ${uid}  > temp.txt

fi
	

# Extract the filenames
awk '{print $4 }' temp.txt > temp2.txt

# find files that match search criteria
matches=`grep ${search} temp2.txt` 


# iterate thru files and delete
for filename in ${matches}
do

	if [ "${bucket_type}" = "xtr" ]; then
		aws s3 cp s3://${bucket}${filename} ${filename} 
	else
		aws s3 cp s3://${bucket}${filename} ${filename} --profile ${uid}	

done
