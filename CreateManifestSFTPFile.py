##################################################################################################
#
# Run `python3 CreateManifestSFTPFile.py -h` for more info.
#
##################################################################################################
# 2023-03-25 Paul Baranoski  Created program.  
# 
##################################################################################################
import os
import sys
import json
import argparse
import logging
from datetime import datetime


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
        parser.add_argument("--outfile", help="Manifest Path and filename")
        parser.add_argument("--SFTPDestFldr", help="SFTP Dest Folder")
        
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

        sOutfilePathAndName = args.outfile  
        #sOutfilePathAndName = os.path.join(os.getcwd(),"manifestFile.man")

        # IDRBI-99999-20210126-165003
        tmstmp = datetime.today().strftime('%Y%m%d-%H%M%S')

        dataRequestID = args.SFTPDestFldr

        
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
                            "shareDuration" : "5",
                            "dataRecipientEmails" : dataRecepientEmails, 
                            "totalNumberOfFiles"  : str(nofFiles),
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

