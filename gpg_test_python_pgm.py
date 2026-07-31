import os
import sys
import io
import re

import subprocess

#sKey=`gpg --list-keys --keyid-format LONG | grep '^sub' | awk '{print $2}' | cut -d/ -f2 `

# Get the public key info.
sp_info = subprocess.run(['gpg', '--list-keys', '--keyid-format', 'LONG' ], capture_output=True, text=True, check=True)
if sp_info.returncode == 0:
    sPublicKeyInfo = sp_info.stdout 
    print(f"{sPublicKeyInfo=}")

# Extract the key. Look for line that starts with 'sub ' with any other characaters after this.
reSubLine = re.compile('^(sub[ ]+)(rsa4096/)([A-Z0-9]+)[ ]+', re.MULTILINE)
objMatch = reSubLine.search(sPublicKeyInfo)
if objMatch is None:
    print("No match found. Error")
else:
    print(objMatch)
    print(objMatch.group(3))
    sKey = objMatch.group(3) 
    print(f"{sKey=}")
    
# Encrypt the file --> gpg --batch -- yes --output sOutputSplitFile_3.gpg --encrypt --recipient {sKey} filename.txt.gz
sp_info = subprocess.run(['gpg', '--batch', '--yes', '--output', 'sOutputSplitFile_3.gpg', '--encrypt', '--recipient', sKey, 'sOutputSplitFile_3' ], capture_output=True, text=True, check=True)

print(sp_info.stdout)

print(sp_info.stderr)

print(sp_info.returncode)
