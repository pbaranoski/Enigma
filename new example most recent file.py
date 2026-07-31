
import boto3
from boto3.s3.transfer import ProgressCallbackInvoker
from boto3.s3.transfer import TransferConfig

import subprocess


def s3GetMostRecentFileKeySubProcess(s3BUCKET, s3BktFldr, sFilenamePrefix):

    print("")
    print(f"{s3BUCKET=}")
    print(f"{s3BktFldr=}")
    print(f"{sFilenamePrefix=}")

    S3ExtFldrNPrefix = f"{s3BktFldr}{sFilenamePrefix}"
    print(f"{S3ExtFldrNPrefix=}")

    # [-1] --> get most recent item that matches prefix
    cmd = [
        "aws",
        "s3api",
        "list-objects-v2",
        "--bucket", s3BUCKET,
        "--prefix", S3ExtFldrNPrefix,
        "--query", 'sort_by(Contents,&LastModified)[-1].Key'
    ]

    # make the api call
    try:
        # use the s3 api to query for results 
        sp_info = subprocess.run(cmd, capture_output=True, text=True, check=True) 
        #write_sp_info_2_log(sp_info)         

    except Exception as ex:
        print(f"No file was found. {ex}")
        
        # re-raise exception
        raise   
    
    # get the most recent key
    sMostRecentS3Key = sp_info.stdout

    print(sp_info)
    print(sMostRecentS3Key)
    
    return sMostRecentS3Key





	
# Get Keys
#getS3FileKeysList(s3_resource, s3BUCKET, s3BktFldr, sFilenamePrefix)

XTR_BUCKET = "aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod" 

#############################################################
# Get S3 references
#############################################################
s3_resource = boto3.resource('s3')
s3_client = boto3.client("s3")

#PREFIX = "PSPS_Extract_Q4"

PREFIX = "PSPS_Extract_Q6"		

PSPS_BCKT_FLDR  = "xtr/DEV/PSPS/"
S3ExtFldrNPrefix  = f"{PSPS_BCKT_FLDR}archive/{PREFIX}"      

#obj = s3_resource.Bucket(XTR_BUCKET).objects.filter(Prefix=S3ExtFldrNPrefix)	

# aws s3 ls ; aws s3 mv ;  aws s3 cp ;   aws s3 rm    

#aws s3api list-objects-v2 \
#  --bucket my-bucket \
#  --prefix my/folder/fileprefix \
#  --query 'sort_by(Contents,&LastModified)[-1].Key' \
# --output text

"""
cmd = [
    "aws",
    "s3api",
    "list-objects-v2",
    "--bucket", XTR_BUCKET,
    "--prefix", S3ExtFldrNPrefix,
    "--query", 'sort_by(Contents,&LastModified)[-1].Key'
 
]



try:

    # use the s3 api to query for results 
    sp_info = subprocess.run(cmd, capture_output=True, text=True, check=True)   

except Exception as ex:
    print(f"No file was found. {ex}")
    
    # re-raise exception
    raise   
        
"""

sMostRecentS3Key = s3GetMostRecentFileKeySubProcess(XTR_BUCKET, f"{PSPS_BCKT_FLDR}archive/", PREFIX)

print(sMostRecentS3Key)

# Remove the filepath from the key --> filename only
filename = sMostRecentS3Key.replace(f"{PSPS_BCKT_FLDR}archive/","")
print(filename )


	