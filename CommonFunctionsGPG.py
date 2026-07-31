######################################################################################
# Name: CommonFunctionsGPG.py
#
# Desc: Common functions module.
#
# NOTE: To use any of the functions in the CommonFunctionsGPG.py module, 
#       include at the top of your module with the other python import statements: 
#
#       "import CommonFunctionsGPG as GPGFunctions".
#
#       In addition, after establishing the Logger in your python module, include the below code:
#
#        # Establish logger with CommonFunctions module.
#        setCommonFunctionLogger(rootLogger)  --> where "rootLogger" is the logger name in your python module.
#
# Modified: 
#
# Paul Baranoski 2026-05-12 Created Module.
# Paul Baranoski 2026-06-30 Modify removeGnupg_home(gnupg_home) to add parameter "ignore_errors=True" to 
#                           shutil.rmtree function call.
# Paul Baranoski 2026-07-14 Modify exception logic for encrypt to write e.stdout and e.stderr to log file.
######################################################################################
import boto3
import subprocess
import tempfile
import os
import shutil


#PRIVATE_KEY_4_SECRET_NAME = "np-opm-private-key"
#PUBLIC_KEY_4_SECRET_NAME = "np-opm-extract-public-key"

#SECRET_NAME = "np-opm-private-key"
#REGION = "us-east-1"


def setCommonFunctionLogger(pRootLogger):

    # Pass the logger once instead of for each function
    global rootLogger
    rootLogger = pRootLogger

def write_sp_info_2_log(sp_info):
        
    # Send stdout to log using "%\n%s" to ensure output is broken by newlines
    if sp_info.stdout != "":
        rootLogger.info("\n%s", sp_info.stdout) 
    # Send stdout to log using "%\n%s" to ensure output is broken by newlines
    if sp_info.stderr != "":
        rootLogger.info("\n%s", sp_info.stderr) 
    rootLogger.info(f"{sp_info.returncode=}")  

    
def get_secret(secret_name, region):
    
    client = boto3.client("secretsmanager", region_name=region)

    response = client.get_secret_value(SecretId=secret_name)

    # Secret can be string or binary
    if "SecretString" in response:
        return response["SecretString"]
    else:
        return response["SecretBinary"].decode("utf-8")


def import_gpg_key(key_data):

    import json

    try:
        
        data = json.loads(key_data)
        #print(data)
        
        key_data = data["private_key"]

        # Import GPG key into a temporary GNUPGHOME"""
        gnupg_home = tempfile.mkdtemp()
        rootLogger.info(f"{gnupg_home=}")


        env = os.environ.copy()
        env["GNUPGHOME"] = gnupg_home

        sp_info = subprocess.run(
            ["gpg", "--batch", "--import"],
            input=key_data.encode("utf-8"),
            env=env,
            capture_output=True, check=True
        )
        
        write_sp_info_2_log(sp_info)
        
    except Exception as ex:
        rootLogger.info(ex)
        
        raise

    return gnupg_home


def decrypt_file(gnupg_home, recipient, encrypted_file, output_file):

    rootLogger.info("decrypt_file")
    rootLogger.info(f"{encrypted_file=}")
    rootLogger.info(f"{output_file=}")    
    rootLogger.info(f"{recipient=}")    

    env = os.environ.copy()
    env["GNUPGHOME"] = gnupg_home
    

    #"--passphrase", PASSPHRASE,
    try:
        
        sp_info = subprocess.run(
            [
                "gpg",
                "--batch",
                "--yes",
                "--pinentry-mode", "loopback",
                "--passphrase", recipient,
                "--trust-model", "always",
                "--output", output_file,
                "--decrypt",
                encrypted_file
            ],
            env=env,
            capture_output=True, check=True

        )

        write_sp_info_2_log(sp_info)
        
    except subprocess.CalledProcessError as ex:
        rootLogger.info("Decrypt exception")
        #rootLogger.info(ex)

        rootLogger.info(ex.stdout)
        rootLogger.info(ex.stderr)  
        
        raise
        
    finally:
        shutil.rmtree(gnupg_home, ignore_errors=True)    
        

def encrypt_file(gnupg_home, input_file, output_file, recipient):
    
    env = os.environ.copy()
    env["GNUPGHOME"] = gnupg_home

    try:
        sp_info = subprocess.run(
            [
                "gpg",
                "--batch",
                "--yes",
                "--trust-model", "always",
                "--output", output_file,
                "--encrypt",
                "--recipient", recipient,
                input_file
            ],
            env=env,
            capture_output=True, check=True
        )

        write_sp_info_2_log(sp_info)
        
    except Exception as ex:
        rootLogger.info(ex.stdout)
        rootLogger.info(ex.stderr) 
        raise
        
    finally:
        shutil.rmtree(gnupg_home, ignore_errors=True)
        

def get_key_fingerprint(gnupg_home):
    
    """Extract recipient ID (fingerprint) for encryption"""
    env = os.environ.copy()
    env["GNUPGHOME"] = gnupg_home

    result = subprocess.run(
        ["gpg", "--list-keys", "--with-colons"],
        capture_output=True,
        text=True,
        env=env,
        check=True
    )

    # parse fingerprint (fpr line)
    for line in result.stdout.splitlines():
        if line.startswith("fpr"):
            return line.split(":")[9]

    raise Exception("No fingerprint found")


def list_keys(gnupg_home):
    
    # List keys for debugging
    env = os.environ.copy()
    env["GNUPGHOME"] = gnupg_home

    # list keys
    sp_info = subprocess.run(
        [
        "gpg",
        "--list-secret-keys",
        "--keyid-format",
        "LONG"
        ],
        env=env,
        capture_output=True,
        text=True
        )

    rootLogger.info(sp_info.stdout)
    rootLogger.info(sp_info.stderr)
        

def list_packets(gnupg_home, encrypted_file):

    env = os.environ.copy()
    env["GNUPGHOME"] = gnupg_home

    try:
        
        sp_info = subprocess.run(
            [
                "gpg",
                "--list-packets",
                encrypted_file
            ],
            env=env,
            capture_output=True, text=True, check=True
        )

        rootLogger.info(sp_info.stdout)
        rootLogger.info(sp_info.stderr)
        
    except subprocess.CalledProcessError as ex:

        rootLogger.info(ex.stdout)
        rootLogger.info(ex.stderr)  
            
        raise    


def removeGnupg_home(gnupg_home):

    import shutil

    try:
        rootLogger.info(f"Removing {gnupg_home}")
        shutil.rmtree(gnupg_home, ignore_errors=True)

    except Exception as ex:
        rootLogger.info(f"Directory did not exist or could not delete directory")
        rootLogger.info(f"{ex.stderr=}")

def main():
    pass
    # 1. Get private key from Secrets Manager
    #private_key = get_secret(SECRET_NAME, REGION)
    #rootLogger.info(private_key)

    # 2. Import key into isolated GPG home
    #gnupg_home = import_gpg_key(private_key)
    #rootLogger.info(gnupg_home)

    # 3. Decrypt file
    #decrypt_file(gnupg_home, ENCRYPTED_FILE, OUTPUT_FILE)

    # 4. Encrypt file
    #encrypt_file(gnupg_home, INPUT_FILE, OUTPUT_FILE, recipient)



if __name__ == "__main__":
    main()
