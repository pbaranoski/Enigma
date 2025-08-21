########################################################################################################
# Name:  UnzipS3File.py
#
# Desc: Python program to unzip S3 files and move to Files2EFT folder
#
# Created: Paul Baranoski
#
# Paul Baranoski 2023-04-11 Created script.
# Paul Baranoski 2023-04-21 Modified error message text for exception in function unzip_gzip_files, and  
#                           set RC=12. 
# Paul Baranoski 2025-03-19 Modified name of s3 client. Changed methods being used from Object to get_object and put_object.
# Paul Barnaoski 2025-04-01 Change log message to remove reference to CombinesFiles.py.
########################################################################################################

import boto3 
import gzip
import logging
import argparse
import sys


# 5GB limit
PUT_BKT_LIMIT = (1024 ** 3) * 5

# Setup logger to display timestamp
logging.basicConfig(format='$(levelname) %(asctime)s => %(message)s', level=logging.INFO)


def unzip_gzip_files(sourcebucketname, fileKey, destKey):
    try:

        logging.warning ("Start unzip_gzip_files function: " )

        ###############################################################
        # Read S3 gzip file
        ###############################################################
        logging.warning(f"Get object for {sourcebucketname=} {fileKey=}" )
        gzip_file =  s3_client.get_object(Bucket=sourcebucketname, Key=fileKey)


        logging.warning("Read s3 gzip file ")
        strmBytes = gzip_file["Body"].read()
        logging.warning(f"S3 gz file size: {len(strmBytes)}")

        ###############################################################
        # Decompress S3 gzip file
        ###############################################################
        logging.warning("Decompress S3 gzip file data: " )
        unzipped_content = gzip.decompress(strmBytes)
        unzipped_content_len = len(unzipped_content)
        
        logging.warning(f"Unzipped bytes: {unzipped_content_len}")

        ###############################################################
        # if unzipped content is greater than 5GB, it will need to be 
        # put in increments.
        # NOTE: THIS CODE HAS NOT BEEN TESTED.
        ###############################################################
        if unzipped_content_len > PUT_BKT_LIMIT:
            logging.warning("Unzipped content is greater than 5GB")

            # Determine NOF 5GB parts
            parts = unzipped_content_len // PUT_BKT_LIMIT
            logging.warning(f"parts: {parts}")

            # Is there a partial part? --> increment NOF parts 
            partial_part_bytes = unzipped_content_len % PUT_BKT_LIMIT
            if partial_part_bytes > 0:
                logging.warning(f"partial_part_bytes: {partial_part_bytes}")
                parts += 1

            # Write S3 file parts 
            iBegByte = 0
            
            for i in range(1, parts + 1):
                destKeyPart = destKey + f"_0_0_{i}.txt"
                logging.warning(f"Create s3 resource for {destKeyPart}" )
                unzipped_S3file = s3_client.Object(bucket_name=sourcebucketname, key=destKeyPart)

                # Calculate ending 
                iEndByte = iBegByte + PUT_BKT_LIMIT 
                if iEndByte > unzipped_content_len:
                    iEndByte = unzipped_content_len

                # Extract bytes for next file part to write    
                unzipped_part_data = unzipped_content[iBegByte : iEndByte]

                # write next data part
                unzipped_S3file.put(Body=unzipped_part_data)

                # point to next set of bytes
                iBegByte = iEndByte

                # You need to use the boto3 copy method instead of copy_object. 
                # It will perform the multi-part upload that is required when 
                # copying objects greater than 5GB. It will also handle the threading for you.
                # You should consider using Multipart uploads.In a single operation, 
                # maximum allowed size is 5GB
                #
                #s3_client.meta.client.copy(unzipped_content,Bucket=sourcebucketname,Key=destKey)
                
                #############################################################################
                # Create Multi-part upload
                #############################################################################    
                #resp = s3_client.create_multipart_upload(Bucket=sourcebucketname, Key=destKey)
                #upload_id = resp['UploadId']

                #part_num = 1
                ## part_num is integer 1-10000
                ##for part_num, source_part in enumerate(s3_parts, 1): # part numbers are 1 indexed
                #resp = s3_client.upload_part_copy(Bucket=sourcebucketname,
                #                        Key=destKey,
                #                        PartNumber=part_num,
                #                        UploadId=upload_id,
                #                        CopySource=source_part)
            
        else:
            logging.warning("Unzipped content is less than or equal to 5GB")
            
            s3_client.put_object(Bucket=sourcebucketname, Key=destKey, Body=unzipped_content)
            logging.warning (f"Unzipped file created as {destKey}. ")
        
            s3_client.delete_object(Bucket=sourcebucketname, Key=fileKey)
            logging.warning (f"Zipped file {fileKey} was deleted. ")


    except Exception as e:  
        logging.error(f'Error: Unable to unzip or move file: {e}')
        sys.exit(12) 
 
  
####################################################################
# main
####################################################################
if __name__ == "__main__":

    ##########################################################################
    # Warning-level will print out to stdout (so it will appear in log files)
    ##########################################################################
    logging.warning("Starting UnzipS3File python program")

    ##########################################################################
    # INFO: Get S3 resource and client
    ##########################################################################
    # boto3.resource is a high-level services class wrap around boto3.client.
    # boto3.Session.client is low-level service
    #
    # boto3.resource is meant to attach connected resources under where you can  
    # later use other resources without specifying the original resource-id.
    #
    ##########################################################################
    logging.warning ("Get S3 resource and client tokens")
    
    global s3_client
    #s3_client = boto3.resource('s3')
    s3_client = boto3.client("s3")

    ##########################################################################
    # Get parameters
    ##########################################################################
    try:    
        parser = argparse.ArgumentParser(description="S3 file combiner")
        parser.add_argument("--bucket", help="base bucket to use")
        parser.add_argument("--zipFile", help="S3 folder whose contents should be combined")
        parser.add_argument("--unzipFile", help="prefix of files to combine into single file")
        
        args = parser.parse_args()

        #######################################################
        # Example parameters.
        #######################################################
        #args.bucket = 'aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod'
        #args.zipFile = 'xtr/DEV/filename.txt.gz'
        #args.unzipFile = 'xtr/DEV/filename.txt'


    except Exception as e:
        logging.error("Exception occured in UnzipS3File.py.")
        print(e)

        sys.exit(12)  

    sourcebucketname = args.bucket
    key = args.zipFile
    destKey = args.unzipFile    

    #sourcebucketname='aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod'
    #key = "xtr/DEV/Blbtn/blbtn_prvdr_ext_20230308.120725.csv.gz"
    #key = "xtr/DEV/Files2EFT/T#EFT.ON.BIAIDRC.SRCAR16.D230331.T0945051.txt.gz"
    #destKey = "xtr/DEV/ATest/blbtn_prvdr_ext_20230308.120725.txt"
    #destKey = "xtr/DEV/ATest/T#EFT.ON.BIAIDRC.SRCAR16.D230331.T0945051.txt"
    
    logging.warning ("Call unzip_gzip_files function")
    unzip_gzip_files(sourcebucketname, key, destKey)
