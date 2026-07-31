########################################################################################################
# Name:  createCSVFile.py
#
# Desc: Create a CSV file from a fixed-width non-delimited file. 
#
# NOTE: Program requires a configuration file that describes the Input fixed-width file. Config file should contain a header record, however the column names are 
#       not enforced by this program. Config file should contain: 1) field-name, field start position, field length, and field type (C)har or (N)umeric
#       
#       Execute: python3 createCSVFile.py --ConfigFile {path/filename} --InputFile {path/filename} --CSVFile {path/filename}
#
# Paul Baranoski 2025-08-28 Create Module.
########################################################################################################
import csv
import sys
import os
import argparse
import logging

#import datetime
from datetime import datetime
from datetime import date,timedelta

#LOG_DIR = "/app/IDRC/XTR/CMS/logs/"
LOG_DIR = ""

def setLogging(LOGNAME):

    # Configure root logger
    #logging.config.fileConfig(os.path.join(config_dir,"loggerConfig.cfg"))
    
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(funcName)-22s %(message)s",
        #encoding='utf-8', 
        datefmt="%Y-%m-%d %H:%M:%S", 
        #filename=f"{LOG_DIR}BuildRunExtCalendar_{TMSTMP}.log"
        handlers=[
        #logging.FileHandler(f"{LOG_DIR}createCSVFile_{TMSTMP}.log"),
        logging.FileHandler(f"createCSVFile_{TMSTMP}.log"),
        logging.StreamHandler(sys.stdout)],    
        level=logging.INFO)
 
    global rootLogger
    loggerName = os.path.basename(f"LOGNAME").replace(f"_{TMSTMP}.log","")
    rootLogger = logging.getLogger(loggerName)
  
    #os.chmod(LOG_DIR, 0o777)  # for Python3
    
    #logger.setLevel(logging.INFO)
    
def load_config(config_file):
    """Load configuration CSV into a list of dicts."""
    config = []

    with open(config_file, newline="") as f:
        reader = csv.reader(f)
        bSkipHeader = False

        for row in reader:
            if bSkipHeader:
                bSkipHeader = False
            else:    
                fieldName, StartPos, Length, Type = row
                config.append({
                    "name": fieldName,
                    "start": int(StartPos),
                    "length": int(Length),
                    "type": Type.strip().upper()
                })

    return config


def convert_fixed_width_2_csv(config, inputFile, csvFile):
    """Parse fixed-width file using config into a DataFrame."""
    
    with open(inputFile, "r") as inf,open(csvFile, "w", newline="") as outf:

        writer = csv.writer(outf)

        # Create header record
        header = [ entry["name"] for entry in config]
        writer.writerow(header)

        # Process input file        
        for line in inf:

            # Skip blank lines
            if line.strip() == "":
                continue
            else:
                record = []
                for field in config:
                    # Python is zero-based offsets
                    startPos = field["start"] - 1
                    EndPos = startPos + field["length"]
                    raw_value = line[startPos:EndPos]
                    raw_value = raw_value.strip()
                    
                    # Convert types
                    if field["type"] == "N":
                        if raw_value == "":
                            record.append(None)
                        else:
                            try:
                                record.append(int(raw_value))
                            except ValueError:
                                record.append(float(raw_value))
                    else:
                        record.append(raw_value)

            writer.writerow(record)


def main():

    try:    

        # Set Timestamp for log file and extract filenames
        global TMSTMP
        TMSTMP = datetime.now().strftime('%Y%m%d.%H%M%S')
        #print(f"{TMSTMP=}")

        global LOGNAME
        LOGNAME = f"{LOG_DIR}createCSVFile_{TMSTMP}.log"
        
        ##########################################
        # Establish log file
        ##########################################
        setLogging(LOGNAME)
        rootLogger.info(f"CreateCSVFile.py started at {TMSTMP}")
        
        ##########################################
        # Get any parameters
        ##########################################
        parser = argparse.ArgumentParser(description="BuildCalDriver parms")
        parser.add_argument("--ConfigFile", help="Configuration File which contains field-name, start-position, field length, field type")
        parser.add_argument("--InputFile", help="Fixed-width non-delimited file")
        parser.add_argument("--CSVFile", help="Output CSV file")

        args = parser.parse_args()

        rootLogger.info(f"Getting parms")

        config_file = str(args.ConfigFile)
        data_file = str(args.InputFile)
        output_file = str(args.CSVFile)

        # Load config
        rootLogger.info(f"Loading Config file")
        config = load_config(config_file)
        rootLogger.info(config)

        # Parse fixed width file
        rootLogger.info(f"Convert fixed-width to csv file")
        convert_fixed_width_2_csv(config, data_file, output_file)

        #print(records)

        rootLogger.info(f"CSV file created: {output_file}")
        
        rootLogger.info(f"CreateCSVFile.py ended at {TMSTMP}")

        
    except Exception as e:
        print (f"Exception occured in createCSVFile.py\n {e}")

        rootLogger.error("Exception occured in createCSVFile.py.")
        rootLogger.error(e)

        sys.exit(12) 
        

if __name__ == "__main__":
    main()

