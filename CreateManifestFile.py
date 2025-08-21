##################################################################################################
#
# !!!!!! NOTE:  THIS MODULE IS OBSOLETE.  !!!!!
#
# Run `python3 CreateManifestFile.py -h` for more info.
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
##################################################################################################
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


def BuildManifestFile():

    ###############################################
    # Get parameters
    ###############################################
    try:    
        parser = argparse.ArgumentParser(description="manifestFile")
        parser.add_argument("--bucket", help="S3 bucket")
        parser.add_argument("--folder", help="S3 folder")
        parser.add_argument("--files", help="S3 files")
        parser.add_argument("--REmails", help="Recipient email addresses")
        parser.add_argument("--filesize", type=int, help="total filesize of S3 files")
        parser.add_argument("--outfile", help="Manifest Path and filename")
        parser.add_argument("--jiraURL", help="Extract JIRA URL")
        
        args = parser.parse_args()
        print(f"args: {args}")

        ###############################################
        # process parameters
        ###############################################
        sFileNames = args.files.replace(',',' ').strip()
        #sFileNames = "Mickey.txt,Donald.jpg,Rigby.txt,Bugs.csv.gz,".replace(',',' ').strip()
        lstFileNames = sFileNames.split(" ")
        nofFiles = len(lstFileNames)
        print(f"nofFiles: {nofFiles}")

        sFileLocation = args.folder
        #sFileLocation = "xtr/DEV/Blbtn"

        dataRecepientEmails = args.REmails
        #dataRecepientEmails = "pbaranoski@apprioinc.com,jturner@apprioinc.com,SGayam@apprioinc.com"

        iFileSize = args.filesize
        #iFileSize=5034556

        #print(pretty_size(iFileSize))
        lstFileSizeInfo = pretty_size(iFileSize).split(' ')
        print(f"lstFileSizeInfo: {lstFileSizeInfo}")

        totFileSize = lstFileSizeInfo[0]
        totFileSizeUnit = lstFileSizeInfo[1]
        #print( totFileSizeUnit)

        sOutfilePathAndName = args.outfile  
        #sOutfilePathAndName = os.path.join(os.getcwd(),"manifestFile.man")

        # IDRBI-99999-20210126-165003
        tmstmp = datetime.today().strftime('%Y%m%d-%H%M%S')

        jiraTicket = args.jiraURL
        URLParts = jiraTicket.split("/")
        dataRequestID = f'{URLParts[len(URLParts) - 1]}-{tmstmp}'

        
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

        print("Write manifest file")
        with open(sOutfilePathAndName, "w+") as manFile:
            manFile.writelines(json_obj)


    except Exception as e:
        logging.error("Exception occured in CreateManifestFile.py.")
        print(e)

        sys.exit(12) 

    ###############################################
    # write output manifest file
    ###############################################
    sys.exit(0)


if __name__ == "__main__":
    BuildManifestFile()

