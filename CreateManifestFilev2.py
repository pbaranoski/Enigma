##################################################################################################
#
# Run `python3 CreateManifestFilev2.py -h` for more info.
#
##################################################################################################
# 10/13/2022 Paul Baranoski  Created program. Found pretty_size function in post on this url.  
# https://stackoverflow.com/questions/5194057/better-way-to-convert-file-sizes-in-python#:~:text=Instead%20of%20a%20size%20divisor%20of%201024%20%2A,be%20used%20with%20bytes%2C%20i.e.%3A%20megas%20%3D%20size_in_bytes%2FMBFACTOR.
# 
# 10/27/2022 Paul Baranoski Changed duraction from 1 to 30. Added jiraTicket tag.
#
# 11/07/2022 Paul Baranoski Add code to ensure shareDuration and totalNumberOfFiles
#                           are strings (add quotes). Also correct two key names (case).
# 01/31/2022 Paul Baranoski Change "dataRecepientEmails" tag to "dataRecipientEmails".
# 03/08/2023 Paul Baranoski Add "method":"POST" and "deleteReason":"" to manifest file.
# 08/25/2023 Paul Baranoski Remove "totalExtractFileSize" : totFileSize, and 
#                           "totalExtractFileSizeUnit" : totFileSizeUnit
#                           because they changed their format once again.
# 10/13/2023 Paul Baranoski Add timestamp to dataRequestID so that previous extract files are not overlayed in BOX.
# 01/03/2025 Paul Baranoski Clone CreateManifestFile.py to add code to encapsulate handling  
#                           various DDSM manifest file processing short-comings within this module
#                           instead of manually handling in each parent script. New code will
#                           create multiple manifest files based on total file sizes not exceeding 40GB,
#                           and to limit NOF files so that the size of manifest file does not exceed 10 KB.   
# 01/13/2025 Paul Baranoski In _list_all_objects_with_size function, when there are more than 999 objects, 
#                           you have to make multiple calls to s3.list_objects(Bucket=BUCKET, Prefix=folder, Marker=last_key)
#                           getting the next "page" of data. The code to get the last_key was incorrect.
#                           Copied from combineS3Files.py I neglected to modify the code to get the last item in list 
#                           as Dictionary item instead of tuple. A Dictionary item was more self-documenting. 
#                           When tuple: last_key=objects_list[-1][0] 
#                           When dictionary: last_key=objects_list[-1]["Key"]
# 01/15/2025 Paul Baranoski Add logic to extract a file prefix from S3 folder name, and use that in the s3.list_objects calls to reduce 
#                           the NOF objects/filenames returned for more efficient processing.
# 03/31/2025 Paul Baranoski Add edit to throw exception is a single file is larger than max size of all files to be included in a manifest file.
# 05/29/2025 Paul Baranoski Add exception logic for file prefix for VA_PTD.
##################################################################################################
import boto3
import os
import sys
import json
import argparse
import logging
from datetime import datetime


# bytes pretty-printing
UNITS_MAPPING = [
    (1<<50, ' PB'),
    (1<<40, ' TB'),
    (1<<30, ' GB'),
    (1<<20, ' MB'),
    (1<<10, ' KB'),
    (1, (' byte', ' bytes')),
]

def pretty_size(bytes, units=UNITS_MAPPING):
    """Get human-readable file sizes.
    simplified version of https://pypi.python.org/pypi/hurry.filesize/
    """
    for factor, suffix in units:
        if bytes >= factor:
            break
    amount = int(bytes / factor)

    if isinstance(suffix, tuple):
        singular, multiple = suffix
        if amount == 1:
            suffix = singular
        else:
            suffix = multiple

    return str(amount) + suffix
    
    
def new_s3_client():
    # initialize an S3 client with a private session so that multithreading
    # doesn't cause issues with the client's internal state
    session = boto3.session.Session()
    return session.client('s3')


def _list_all_objects_with_size(s3, folder):

    ##########################################################
    # Response contains Dictionary with item "Contents"
    # Within "Contents is a list that contains keys: Key, Size,
    # commented out --> return tuple   
    #                   return Dictionary instead.
    ##########################################################
    def resp_to_filelist(resp):
        #return [(x['Key'], x['Size']) for x in resp['Contents']]
        return [{"Key": x['Key'], "Size": x['Size']} for x in resp['Contents']]

    ###################################################
    # 1) Get all objects/files in S3 bucket/folder
    # 2) Extract Key(Filename) and File Size as Dictonary
    ###################################################
    objects_list = []
    #resp = s3.list_objects(Bucket=BUCKET, Prefix=folder)
    
    print("before S3 list_objects_v2")
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=folder)
    print(f"{resp=}")

    
    print("before objects_list.extend")
    objects_list.extend(resp_to_filelist(resp))
    #print(f"{objects_list=}")
    print("after objects_list.extend")

    # Get Next "page" of data if exists
    while resp['IsTruncated']:
        print("In While IsTruncated")
        # if there are more entries than can be returned in one request, the key
        # of the last entry returned acts as a pagination value for the next request
        logging.warning("Found {} objects so far".format(len(objects_list)))

        ##########################################################
        # Change from list of tuples to list of dictionary items
        ##########################################################
        #last_key = objects_list[-1][0]
        last_key=objects_list[-1]["Key"]
        print (f"{last_key=}")

        print("before s3.list_objects")
        resp = s3.list_objects(Bucket=BUCKET, Prefix=folder, Marker=last_key)
        print("after s3.list_objects")
        objects_list.extend(resp_to_filelist(resp))

    return objects_list


def BuildManifestFile(idx_lit, lstFileNames, args):

    print("BuildManifestFile")
    
    try:    

        ###############################################
        # process parameters
        ###############################################
        nofFiles = len(lstFileNames)
        print(f"nofFiles: {nofFiles}")

        #sFileLocation = "xtr/DEV/Blbtn" 
        sFileLocation = folder

        #dataRecepientEmails = "pbaranoski@apprioinc.com,jturner@apprioinc.com,SGayam@apprioinc.com"
        dataRecepientEmails = args.REmails

        #sOutFilePathAndName="/app/IDRC/XTR/CMS/data/OFM_PDE_Manifest_20240102.150137-0.json"
        sOutFilePathAndName = args.outfile.replace(".json",f"{idx_lit}.json")
        print(f"sOutFilePathAndName={sOutFilePathAndName}")

        # IDRBI-99999-20210126-165003-idx
        #tmstmp = datetime.today().strftime('%Y%m%d-%H%M%S')

        jiraTicket = args.jiraURL
        URLParts = jiraTicket.split("/")
        dataRequestID = f'{URLParts[len(URLParts) - 1]}-{tmstmp}{idx_lit}'

        
        ###############################################
        # Get Environment variables DDOM Contact Info 
        ###############################################
        DDOM_CONTACT_NM=os.getenv('DDOM_CONTACT_NM')
        DDOM_CONTACT_PHNE_NUM=os.getenv('DDOM_CONTACT_PHNE_NUM')
        DDOM_CONTACT_EMAIL=os.getenv('DDOM_CONTACT_EMAIL')

        ###############################################
        # Build List of Filename Dictionary items
        ###############################################
        print("Build S3 Filename Dictionary items")
        lstDictS3Filenames = [{"fileName": sFileName, "fileLocation": sFileLocation} for sFileName in lstFileNames]    
        #print(lstDictS3Filenames)

        ###############################################
        # Build Share Detail Dictionary
        ###############################################
        dctShareDetails = { "dataRequestID" : dataRequestID,
                            "shareDuration" : "30",
                            "dataRecipientEmails" : dataRecepientEmails, 
                            "totalNumberOfFiles"  : str(nofFiles),
                            "jiraTicket" : jiraTicket
        }

        ###############################################
        # Build Contact Info Dictionary
        ###############################################
        dctContactInfo = {
            "fullName": DDOM_CONTACT_NM, 
            "phoneNumber": DDOM_CONTACT_PHNE_NUM,
            "email": DDOM_CONTACT_EMAIL
        }

        ###############################################
        # Build Manifest file Dictionary
        ###############################################
        dctManifest = {"method":"POST",
                "deleteReason":"",
                "fileInformation": lstDictS3Filenames,
                "shareDetails": dctShareDetails,
                "requestorContactInfo": dctContactInfo,
                "comments" : ""  
        }  

        ###############################################
        # Write out manifest json file
        ###############################################
        print("Convert dictionary to json format")

        json_obj = json.dumps(dctManifest, indent=4)
        print(json_obj)

        print(f"Write manifest file:{sOutFilePathAndName}")
        
        with open(sOutFilePathAndName, "w+") as manFile:
            manFile.writelines(json_obj)

        print("")

    except Exception as e:
        logging.error("Exception occured in CreateManifestFile.py.")
        print(e)

        sys.exit(12) 



if __name__ == "__main__":

    try:   

        #######################################################
        # Get parameters.
        #######################################################
        parser = argparse.ArgumentParser(description="manifestFile")
        parser.add_argument("--bucket", help="S3 bucket")
        parser.add_argument("--folder", help="S3 folder")
        parser.add_argument("--runToken", help="Run Token timestamp")
        parser.add_argument("--REmails", help="Recipient email addresses")
        parser.add_argument("--outfile", help="Manifest Path and filename")
        parser.add_argument("--jiraURL", help="Extract JIRA URL")

        args = parser.parse_args()

        print("args:")    
        print(f"{args.bucket=}")
        print(f"{args.folder=}")
        print(f"{args.runToken=}")
        print(f"{args.REmails=}")        
        print(f"{args.outfile=}") 
        print(f"{args.jiraURL=}")  
        print("")
        
        #######################################################
        # Example parameters.
        #######################################################
        #args.bucket = 'aws-hhs-cms-eadg-bia-ddom-extracts-nonrpod'
        #args.folder = 'xtr/DEV/DSH/'
        #args.output = 'xtr/DEV/blbtn_clm_ext_20220812.091145.csv.gz'
        #args.prefix = 'blbtn_clm_ext_20220812.091145'
        #args.filesize = 1000000000

        ###############################################
        # Set global variable BUCKET with bucket name
        ###############################################
        global BUCKET
        BUCKET = args.bucket
        #BUCKET = "aws-hhs-cms-eadg-bia-ddom-extracts"
        
        global folder
        folder = args.folder
        #folder = "xtr/OFM_PDE/"
        #folder = "xtr/DOJ/"
        #folder = "xtr/DEV/DSH/
        
        #############################################################
        # Get file prefix to limit NOF items returned from S3 bucket
        #############################################################
        # xtr/DEV/OFM_PDE/ --> xtr/DEV/OFM_PDE
        folder_sans_end_slash=folder[:-1]
        # array = xtr DEV OFM_PDE
        temp_array = folder_sans_end_slash.split(sep='/')
        # get last array element and split by '_'
        # array = OFM PDE
        temp_file_prefix=temp_array[-1].split("_")
        # Get first element of array --> OFM
        file_prefix=temp_file_prefix[0]
        # 
        if file_prefix == "OPMHI":
            file_prefix = "FEHB"
        if file_prefix == "VA":
            file_prefix = "MOA"
        
        print(f"{file_prefix=}")
        

        #runToken = "20240805.153143"  # OFM
        #runToken = "20240826.135600"  # DOJ
        runToken = args.runToken

        ############################################
        # Variables and constants
        ############################################
        # 32212254720-30GB  42949672960-40GB  53687091200-50GB
        iMaxSizeAllFiles = 42949672960  
        iTotSizeAllFiles = 0
        
        iMaxNOFFiles = 35
        iTotNOFFiles = 0
        
        iMaxManifestFileBytes = 8000
        iNOFManifestFileBytes = 0

        Files2IncludeInManifest = []

        # for dataRequestID
        global tmstmp
        tmstmp = datetime.today().strftime('%Y%m%d-%H%M%S')
        
        ######################################################
        # Get access to S3
        ######################################################
        s3 = new_s3_client()

        ######################################################
        # Get list of all files in S3 bucket/folder
        ######################################################
        print("before _list_all_objects_with_size function call ")
        folder_file_prefix=folder + file_prefix
        print(f"{folder_file_prefix=}")
        
        #s3FolderFileList = _list_all_objects_with_size(s3, folder)
        s3FolderFileList = _list_all_objects_with_size(s3, folder_file_prefix)

        print(s3FolderFileList)

        print("after _list_all_objects_with_size function call ")

        ######################################################
        # Filter list of files by run-token
        # 1) Tuple version  2) Dictionary version
        ######################################################
        #s3FolderFileListFiltered = [S3FldFilename for S3FldFilename in s3FolderFileList if S3FldFilename[0].find(run_token) != -1 ]
        Files2IncludeInManifest = [S3FilenameDict for S3FilenameDict in s3FolderFileList if str(S3FilenameDict['Key']).find(runToken) != -1 ]
        #print (Files2IncludeInManifest)

        ######################################################
        # Create Group Lists to ensure:
        #    1) Total NOF files not exceeded
        #    2) Total size of files in Manifest file is < iMaxSizeAllFiles (40 or 50 GB)
        ######################################################
        group_filename_lists = []
        cur_filename_list = []

        for File2Include in Files2IncludeInManifest:

            # if a single file is larger than max, we can end up with an empty list of files to include in a manifest file
            if (File2Include['Size']) > iMaxSizeAllFiles:
                 raise Exception(f"File {os.path.basename(File2Include['Key'])} is too big to place in manifest file. Create a smaller sized file.")
            
            if (  ((iTotNOFFiles + 1)  > iMaxNOFFiles) or 
                
                  ((iTotSizeAllFiles + File2Include['Size']) > iMaxSizeAllFiles) ):
                      
                print(f"{iTotNOFFiles=}")
                print(f"{iTotSizeAllFiles=}")
                print(f'{iNOFManifestFileBytes=}')

                print ("\nNew Group")

                group_filename_lists.append(cur_filename_list)
                cur_filename_list = []

                iTotNOFFiles = 0
                iTotSizeAllFiles = 0

                iNOFManifestFileBytes = 0

            # end if


            # Add filename without path to filename_list
            cur_filename_list.append(os.path.basename(File2Include['Key']))

            iTotNOFFiles += 1
            iTotSizeAllFiles +=  File2Include['Size']

            #print(f'{iTotSizeAllFiles=}')
            #print(f"{File2Include['Size']=}")

            iNOFManifestFileBytes += len(File2Include['Key']) + len(folder) + 70


        ##################################################
        # Add remainder to group
        ##################################################
        if len(cur_filename_list) > 0:
            print(f"remainder->{iTotNOFFiles=}")
            group_filename_lists.append(cur_filename_list)    

            print(f"{iTotNOFFiles=}")
            print(f"{iTotSizeAllFiles=}")
            print(f'{iNOFManifestFileBytes=}')


        ##################################################
        # Create a manifest file for each group
        ##################################################
        print("\nBefore group_filenames process")
        
        NOFGroups = len(group_filename_lists)
        print(f"{NOFGroups=}")
        
        for idx, filename_list in enumerate(group_filename_lists, start=1):
            #print ("\n")
            print(f"{idx=}")
            print (f"{len(filename_list)=}")
            print(filename_list)
            
            # If NOF groups = 1 --> no literal  else add literal
            if NOFGroups == 1:
                idx_lit = ""
            else:    
                idx_lit = f"-{idx}"

            BuildManifestFile(idx_lit, filename_list, args)


        ###############################################
        # write output manifest file
        ###############################################
        sys.exit(0)

    except Exception as e:
        logging.error("Exception occured in CreateManifestFiles.py.")
        print(e)

        sys.exit(12)      

    

