'''
###################################################################################
This script performs efficient concatenation of files stored in S3. Given a
folder, output location, and optional prefix, all files with the given prefix
will be concatenated into one file stored in the output location.
Concatenation is performed within S3 when possible, falling back to local
operations when necessary.
Run `python combineS3Files.py -h` for more info.
###################################################################################
Author: Jason Dsouza  (https://gist.github.com/jasonrdsouza/f2c77dedb8d80faebcf9)
                       http://whitfin.io/quickly-concatenating-files-in-amazon-s3/

Modifications: 
==============
2022-08-12 Paul Baranoski Modified suffix logic to instead use prefix logic which fits better with 
                          how multiple files are created using the COPYINTO functionality of snowflake.
2022-08-12 Paul Baranoski Fixed bug in function chunk_by_size which was not adding chunks less than the 
                          max_filesize to the group list.
2022-08-12 Paul Baranoski Added logic to not write a "-n" suffix for combined file when there is only 
                          one group in grouped_parts_list. 
2022-08-12 Paul Baranoski Added some clarification comments in regards to the logic. Add required logging 
                          messages. Properly set sys.exit for exceptions.                         
2022-08-15 Paul Baranoski Added code to run_concatenation to exit function if no files found to concatenate
                          or only 1 file found.
2022-08-15 Paul Baranoski Modified code in function assemble_parts_to_concatenate to properly handle 
                          small file size concatenation. Original code appears to have worked with 
                          txt files, but not binary compressed file.
2022-09-13 Paul Baranoski Added natural_sortKey function and sorting of returned parts_list from 
                          collect_parts function call so that files will be concatenated in proper order.
                          (Ex. blbtn_10_0_0.csv.gz will sort after blbtn_9_0_0.csv.gz.) 
2022-11-02 Paul Baranoski Modified code to run_concatenation to exit function if no files found to concatenate.
                          Removed logic where I do same when only 1 file found.
                          Ex. blbtn_clm_ext_20221102.093134.csv.gz_0_0_0.csv.gz created so still want to create  
                          file blbtn_clm_ext_20221102.093134.csv.gz. This allows the logic in callers to continue
                          to work as expected.       
2023-04-18 Paul Baranoski Modified function call _list_all_objects_with_size to include additional parameter "file_prefix".
                          This was done to include the S3 folder AND the "file prefix" when filtering objects returned from 
                          s3.list_objects_v2 call.
                          The reason for this modification was that items in the archive sub-folder were also included in the 
                          "parts list", and were concatenated into the single file, which was not the intention. 
2025-01-02 Paul Baranoski Updated comments of examples of parameters to be more accurate.   
2025-06-27 Paul Baranoski Modify download small_parts files to /tmp/ folder on server so this module can be "server-less"                       
'''

import boto3
import os
import threading
import argparse
import logging
import sys
import re

#pip install natsort
#from natsort import natsorted

# Script expects everything to happen in one bucket
BUCKET = "" # set by command line args
# S3 multi-part upload parts must be larger than 5mb
MIN_S3_SIZE = 6000000

# Setup logger to display timestamp
logging.basicConfig(format='$(levelname) %(asctime)s => %(message)s', level=logging.INFO)

# Used to sort list of files in S3: 
# \d = Matches any decimal digit
# \D = Matches any character which is not a decimal digit 
tokenize = re.compile(r'(\d+)|(\D+)').findall


# returns a tuple of tokens
def natural_sortkey(lstTuple):   
    #print(lstTuple[0])       
    return tuple(int(num) if num.isnumeric()  else alpha for num, alpha in tokenize(lstTuple[0]))


def run_concatenation(folder_to_concatenate, result_filepath, file_prefix, max_filesize):

    s3 = new_s3_client()

    ######################################################
    # Get S3 files to concatenate
    ######################################################
    parts_list = collect_parts(s3, folder_to_concatenate, file_prefix)
    #parts_list_sorted = sorted(parts_list, key=lambda x: x[0],  reverse=True)
    parts_list_sorted = sorted(parts_list, key=natural_sortkey)
    parts_list = parts_list_sorted
    print(f"parts_list: {parts_list_sorted}")

    logging.warning("Found {} parts to concatenate in {}/{}".format(len(parts_list), BUCKET, folder_to_concatenate))

    # No files to concatenate found --> exit function
    # One file to concatenate found --> exit function
    #if len(parts_list) == 0 or len(parts_list) == 1:
    if len(parts_list) == 0:
        logging.warning(f"No files to concatenate found for {file_prefix}")
        return

    ######################################################
    # 
    ######################################################
    grouped_parts_list = chunk_by_size(parts_list, max_filesize)
    #print(f"grouped_parts_list:{grouped_parts_list}")
    logging.warning("Created {} concatenation groups".format(len(grouped_parts_list)))

    #################################################################
    # if there is a single group --> write a file w/o an ending "-n" 
    #################################################################
    if (len(grouped_parts_list) == 1):
        logging.warning("Concatenating single group")
        run_single_concatenation(s3, grouped_parts_list[0], result_filepath)
    else:
        for i, parts in enumerate(grouped_parts_list):
            logging.warning("Concatenating group {}/{}".format(i, len(grouped_parts_list)))
            # The 3rd parameter adds a "-i" to end of result_filepath
            run_single_concatenation(s3, parts, "{}-{}".format(result_filepath, i))


def run_single_concatenation(s3, parts_list, result_filepath):

    logging.warning("In function run_single_concatenation")
    logging.warning(f"len(parts_list):{len(parts_list)}")

    if len(parts_list) > 1:
        # perform multi-part upload
        upload_id = initiate_concatenation(s3, result_filepath)
        parts_mapping = assemble_parts_to_concatenate(s3, result_filepath, upload_id, parts_list)
        complete_concatenation(s3, result_filepath, upload_id, parts_mapping)

    elif len(parts_list) == 1:
        # can perform a simple S3 copy since there is just a single file
        resp = s3.copy_object(Bucket=BUCKET, CopySource="{}/{}".format(BUCKET, parts_list[0][0]), Key=result_filepath)
        logging.warning("Copied single file to {} and got response {}".format(result_filepath, resp))

    else:
        logging.warning("No files to concatenate for {}".format(result_filepath))
        pass


def chunk_by_size(parts_list, max_filesize):

    logging.warning("In function chunk_by_size")

    grouped_list = []
    current_list = []
    current_size = 0

    # Break parts_list into max_filesize chunks
    for p in parts_list:
        current_size += p[1]
        current_list.append(p)
        if current_size > max_filesize:
            grouped_list.append(current_list)
            current_list = []
            current_size = 0

    # Add to group list any left-over chunk that did not exceed max_filesize
    if current_size < max_filesize:
        grouped_list.append(current_list)    

    return grouped_list


def new_s3_client():
    # initialize an S3 client with a private session so that multithreading
    # doesn't cause issues with the client's internal state
    session = boto3.session.Session()
    return session.client('s3')

def collect_parts(s3, folder, file_prefix):

    logging.warning("In function collect_parts")
    #logging.warning(f"folder:{folder}")
    #logging.warning(f"file_prefix:{file_prefix}")

    #####################################################
    # Filter list by Files that look like "file_prefix".
    #####################################################
    #return filter(lambda x: x[0].endswith(suffix), _list_all_objects_with_size(s3, folder))
    #return filter(lambda x: x.find(file_prefix) != -1, _list_all_objects_with_size(s3, folder))

    s3FolderFileList = _list_all_objects_with_size(s3, folder, file_prefix)
    #print(f"s3FolderList:{s3FolderFileList}")
    s3FolderFileListFiltered = [S3FldFilename for S3FldFilename in s3FolderFileList if S3FldFilename[0].find(file_prefix) != -1 ]
 
    '''
    s3FolderFileListFiltered = []

    for folderObj in s3FolderFileList:
        S3FolderAndFilename = folderObj[0]
        #print(f"S3FolderAndFilename:{S3FolderAndFilename}")
        if S3FolderAndFilename.find(file_prefix) != -1:
            #print(f"folderObj:{folderObj}")
            s3FolderFileListFiltered.append(folderObj)
    '''        

    #print(f"s3FolderListFiltered:{s3FolderFileListFiltered}")

    return s3FolderFileListFiltered        


def _list_all_objects_with_size(s3, folder, file_prefix):

    ##########################################################
    # Response contains Dictionary with item "Contents"
    # Within "Contents is a list that contains Dictionary 
    # The "Contents" dictionary contains keys: Key, Size,
    #     LastModified, ETag, StorageClass
    ##########################################################
    def resp_to_filelist(resp):
        return [(x['Key'], x['Size']) for x in resp['Contents']]

    objects_list = []
    #resp = s3.list_objects(Bucket=BUCKET, Prefix=folder)
    folder_n_file_prefix=folder + file_prefix
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=folder_n_file_prefix)
    ##print (f"resp:{resp}")

    objects_list.extend(resp_to_filelist(resp))

    while resp['IsTruncated']:
        # if there are more entries than can be returned in one request, the key
        # of the last entry returned acts as a pagination value for the next request
        logging.warning("Found {} objects so far".format(len(objects_list)))
        last_key = objects_list[-1][0]
        resp = s3.list_objects(Bucket=BUCKET, Prefix=folder, Marker=last_key)
        objects_list.extend(resp_to_filelist(resp))

    #print(f"objects_list: {objects_list}")

    return objects_list


def initiate_concatenation(s3, result_filename):
    # performing the concatenation in S3 requires creating a multi-part upload
    # and then referencing the S3 files we wish to concatenate as "parts" of that upload
    logging.warning("In function initiate_concatenation")

    resp = s3.create_multipart_upload(Bucket=BUCKET, Key=result_filename)
    logging.warning("Initiated concatenation attempt for {}, and got response: {}".format(result_filename, resp))
    return resp['UploadId']


def assemble_parts_to_concatenate(s3, result_filename, upload_id, parts_list):

    logging.warning("assemble_parts_to_concatenate")

    parts_mapping = []
    part_num = 0

    s3_parts = ["{}/{}".format(BUCKET, p[0]) for p in parts_list if p[1] > MIN_S3_SIZE]
    local_parts = [p[0] for p in parts_list if p[1] <= MIN_S3_SIZE]

    # assemble parts large enough for direct S3 copy
    for part_num, source_part in enumerate(s3_parts, 1): # part numbers are 1 indexed
        resp = s3.upload_part_copy(Bucket=BUCKET,
                                   Key=result_filename,
                                   PartNumber=part_num,
                                   UploadId=upload_id,
                                   CopySource=source_part)
        logging.warning("Setup S3 part #{}, with path: {}, and got response: {}".format(part_num, source_part, resp))
        parts_mapping.append({'ETag': resp['CopyPartResult']['ETag'][1:-1], 'PartNumber': part_num})

    ###############################################################################
    # assemble parts too small for direct S3 copy by downloading them locally,
    # combining them, and then reuploading them as the last part of the
    # multi-part upload (which is not constrained to the 5mb limit)
    ###############################################################################

    ###########################################################
    # small_parts = [] is OK for txt file, but not binary file
    ###########################################################
    #small_parts = []
    small_parts = bytearray()

    for source_part in local_parts:
        logging.warning(f"Building small parts --> source_part:{source_part}")
        
        gzip_file =  s3.get_object(Bucket=BUCKET, Key=source_part)
        strmBytes = gzip_file["Body"].read()
        small_parts.extend(strmBytes)

        ###\/#####
        
        ######################################################
        # Remove writing to /tmp directory.  
        # Ex. Before s3.download_file to temp_filename: /tmp/xtr_DEV_PSPS_NPI_PBAR_PSPS_NPI_2024_JAN_20240122.152712.txt.gz_0_0_0.csv.gz
        # Ex. source_part = xtr/DEV/PSPS_NPI_PBAR_PSPS_NPI_2024_JAN_20240122.152712.txt.gz_0_0_0.csv.gz
        ######################################################
        #logging.warning(f"source_part:{source_part}")
        #temp_filename = "/tmp/{}".format(source_part.replace("/","_"))

        #logging.warning(f"Before s3.download_file to temp_filename: {temp_filename}")
        #s3.download_file(Bucket=BUCKET, Key=source_part, Filename=temp_filename)

        ###################################################################
        # Combine small files into bytearray variable for upload to S3.
        ###################################################################
        #with open(temp_filename, 'rb') as f:
            #####################################
            # Use extend (instead of append) bytearray if you have a bytes object with more than one byte
            # Received exception when using "append"
            #####################################
            #small_parts.extend(f.read())
        #os.remove(temp_filename)
        #logging.warning("Downloaded and copied small part with path: {}".format(source_part))

        ###/\#####




    #logging.warning("Before len(small_parts):{len(small_parts)}")

    if len(small_parts) > 0:
        logging.warning("Uploading last combined small parts")

        last_part_num = part_num + 1
        #####################################################################################
        # When using the ''.join(small_parts) code
        #   --> received an exception: "sequence item 0: expected str instance, bytes found"
        #   --> presume this works with text files, but not binary compressed files
        #####################################################################################
        ###last_part = ''.join(small_parts)  
        last_part = small_parts

        logging.warning("Before s3.upload_part")
        resp = s3.upload_part(Bucket=BUCKET, Key=result_filename, PartNumber=last_part_num, UploadId=upload_id, Body=last_part)
        logging.warning("Setup local part #{} from {} small files, and got response: {}".format(last_part_num, len(small_parts), resp))
        parts_mapping.append({'ETag': resp['ETag'][1:-1], 'PartNumber': last_part_num})

    return parts_mapping

def complete_concatenation(s3, result_filename, upload_id, parts_mapping):

    if len(parts_mapping) == 0:
        resp = s3.abort_multipart_upload(Bucket=BUCKET, Key=result_filename, UploadId=upload_id)
        logging.warning("Aborted concatenation for file {}, with upload id #{} due to empty parts mapping".format(result_filename, upload_id))
    else:
        resp = s3.complete_multipart_upload(Bucket=BUCKET, Key=result_filename, UploadId=upload_id, MultipartUpload={'Parts': parts_mapping})
        logging.warning("Finished concatenation for file {}, with upload id #{}, and parts mapping: {}".format(result_filename, upload_id, parts_mapping))


######################################################
# Files to combine have a format similar to this:
#    blbtn_clm_ext_20220812.091145.csv.gz_0_0_0.csv.gz
#    blbtn_clm_ext_20220812.091145.csv.gz_0_1_0.csv.gz
#    blbtn_clm_ext_20220812.091145.csv.gz_0_2_0.csv.gz
#######################################################
#######################################################
# How to run program:
#     python combineS3Files.py --bucket 'aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod' 
#                              --folder 'xtr/DEV/'  --prefix blbtn_clm_ext_20220812.091145 
#                              --output 'xtr/DEV/blbtn_clm_ext_20220812.091145.csv.gz' 
#                              --filesize 1000000000
#######################################################
def S3FileConcatenationDriver():

    try:    
        parser = argparse.ArgumentParser(description="S3 file combiner")
        parser.add_argument("--bucket", help="base bucket to use")
        parser.add_argument("--folder", help="S3 folder whose contents should be combined")
        parser.add_argument("--prefix", help="prefix of files to combine into single file")
        parser.add_argument("--output", help="output location for resulting merged files, relative to the specified base bucket")
        parser.add_argument("--filesize", type=int, help="max filesize of the concatenated files in bytes")

        args = parser.parse_args()

        #######################################################
        # Example parameters.
        #######################################################
        #args.bucket = 'aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod'
        #args.folder = 'xtr/DEV/Blbtn/'
        #args.output = 'xtr/DEV/Blbtn/blbtn_clm_ext_20220812.091145.csv.gz'
        #args.prefix = 'blbtn_clm_ext_20220812.091145'
        #args.prefix = 'blbtn_clm_ext_20220815.094659'
        #args.prefix = 'blbtn_clm_ext_20220815.095921'
        #args.filesize = 1000000000

        ###############################################
        # Set global variable BUCKET with bucket name
        ###############################################
        global BUCKET
        BUCKET = args.bucket

        #logging.warning("Combining files in {}/{} to {}/{}, with a max size of {} bytes".format(BUCKET, args.folder, BUCKET, args.output, args.filesize))
        logging.warning(f"Combining files like {args.bucket}/{args.folder}{args.prefix} to {args.output} with a max size of {args.filesize}")

        # setting up default profile for session
        #boto3.setup_default_session(profile_name='idrcxtr')
        ##s3 = boto3.client("s3", aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY)

        run_concatenation(args.folder, args.output, args.prefix, args.filesize)

        sys.exit(0)

    except Exception as e:
        logging.error("Exception occured in combinedS3Files.py.")
        print(e)

        sys.exit(12)        


if __name__ == "__main__":

    S3FileConcatenationDriver()
