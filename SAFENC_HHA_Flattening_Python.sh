#!/usr/bin/bash

DATADIR=/app/IDRC/XTR/CMS/data/

echo "Starting script: `date +"%T.%6N" `"

SECONDS=0

python3 SAFENC_HHA_Flattening.py  ${DATADIR}SAFENC_HHA_FINAL_Y22QTR1_20240911.121100.txt ${DATADIR}SAFENC_HHA_Output_Python.txt > SAFENC_HHA_Python.log



echo "Elapsed time: ${SECONDS}"
echo "Ending script: `date +"%T.%6N" `"