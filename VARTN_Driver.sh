#!/usr/bin/sh
############################################################################################################
# Name: VARTN_Driver.sh
#
# Desc: VA Return File Annual Extract
#
# Author     : Joshua Turner	
# Created    : 1/17/2023
#
# Modified:             Date:        Description:
# --------------------  -----------  ----------------------------------------------------------------------
# Joshua Turner 	    2023-01-17   New script.
# Joshua Turner         2023-11-08   Updated for Box delivery - added call to create manifest file 
# Paul Baranoski        2023-11-28   Add parameter (S3 manifest folder override) in call to CreateManifestFile.sh 
#                                    Add ENVNAME to email Subject line.
# Sean Whitelock        2024-09-24   Updated the parameter for S3 manifest folder override call.
# Paul Baranoski        2024-11-05   Modified ending line to be "Ended at.." because Dashboard script is looking for that to know if extract ended successfully.  
# Paul Baranoski        2024-12-23   Add this line to re-migrate code due to "SSM agent on Jenkins server" was down.
############################################################################################################
set +x

#############################################################
# THIS ONE SCRIPT SETS ALL DATABASE NAMES VARIABLES 
#############################################################
source /app/IDRC/XTR/CMS/scripts/run/SET_XTR_ENV.sh
RUNDIR=/app/IDRC/XTR/CMS/scripts/run/
#############################################################
# Establish log file  
#############################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
LOGNAME=/app/IDRC/XTR/CMS/logs/VARTN_Driver_${TMSTMP}.log
touch ${LOGNAME}
chmod 666 ${LOGNAME} 2>> ${LOGNAME} 

echo "################################### " >> ${LOGNAME}
echo "VARTN_Driver.sh started at ${TMSTMP} " >> ${LOGNAME}
echo "" >> ${LOGNAME}

#############################################################
# Establish Date Parameters  
#############################################################
YEAR=`date +%Y`

echo "VA Return File Extract Processing with the following dates:" >> ${LOGNAME}
echo "YEAR: ${YEAR}" >> ${LOGNAME}
echo "" >> ${LOGNAME}

###########################################################################################
# Execute python script to extract VA Return File data and load the extract to S3 
###########################################################################################
TMSTMP=`date +%Y%m%d.%H%M%S`
export YEAR
export TMSTMP

echo ""
echo "Start VARTN_Extract.py." >> ${LOGNAME}
${PYTHON_COMMAND} ${RUNDIR}VARTN_Extract.py >> ${LOGNAME} 2>&1

RET_STATUS=$?
if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Python script VARTN_Extract.py failed." >> ${LOGNAME}
	
	# Send failure email
	SUBJECT="VA Return File Extract FAILED ($ENVNAME)"
	MSG="VA Return File extract has failed in VARTN_Extract.py."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${VARTN_EMAIL_SENDER}" "${VARTN_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
	exit 12
fi

###########################################################################################
# Get the number of records written to the extract file. This will be included in the success
# email. Use awk to extract this number from the logfile
###########################################################################################
NO_OF_RECS=$(awk -F "," '/rows_unloaded/{getline;print $1}' ${LOGNAME})

###########################################################################################
# Concatenate VA Return File S3 files into a single file 
###########################################################################################
echo ""
echo "Concatenate S3 files using CombineS3Files.sh." >> ${LOGNAME}
VARTN_FILE=VARETURN_Y${YEAR}_FILE_${TMSTMP}.txt.gz
${RUNDIR}CombineS3Files.sh ${VARTN_BUCKET} ${VARTN_FILE} 

RET_STATUS=$?
if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script CombineS3Files.sh failed." >> ${LOGNAME}
	
	# Send failure email
	SUBJECT="VA Return File S3 file concatenation FAILED ($ENVNAME)"
	MSG="VA Return File extract has failed in the CombineS3Files step of VARTN_Driver.sh."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${VARTN_EMAIL_SENDER}" "${VARTN_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
	exit 12
fi

###########################################################################################
# Create manifest file for Box delivery (Supply ManifestFileFolder override parameter).
###########################################################################################
echo "" >> ${LOGNAME}
echo "" >> "Creating Manifest file for: ${VARTN_FILE}" >> ${LOGNAME}

${RUNDIR}CreateManifestFile.sh ${VARTN_BUCKET} ${TMSTMP} ${VAPTD_EMAIL_BOX_RECIPIENT} ${MANIFEST_VA_MAC_BUCKET}

RET_STATUS=$?
if [[ $RET_STATUS != 0 ]]; then
	echo "" >> ${LOGNAME}
	echo "Shell script CreateManifestFile.sh failed." >> ${LOGNAME}
	
	# Send failure email
	SUBJECT="Create Manifest file in VARTN_Driver.sh - Failed ($ENVNAME)"
	MSG="VA Return File extract has failed in the CreateManifestFile step of VARTN_Driver.sh."
	${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${VARTN_EMAIL_SENDER}" "${VARTN_EMAIL_FAILURE_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1
	exit 12
fi

###########################################################################################
# Send Success Email
###########################################################################################
echo ""
echo "Sending success email" >> ${LOGNAME}
SUBJECT="VA RETURN ANNUAL EXTRACT : ${YEAR} ($ENVNAME)"
MSG="THE ANNUAL VA RETURN EXTRACTS HAVE BEEN COMPLETED.\n\n======================================================================\n\nFile Name						No of Records\n=========================================	=======================\n${VARTN_FILE}	${NO_OF_RECS}"
${PYTHON_COMMAND} ${RUNDIR}sendEmail.py "${VARTN_EMAIL_SENDER}" "${VARTN_EMAIL_SUCCESS_RECIPIENT}" "${SUBJECT}" "${MSG}" >> ${LOGNAME} 2>&1


TMSTMP=`date +%Y%m%d.%H%M%S`
echo "" >> ${LOGNAME}
echo "VA Return File Annual Extract completed successfully." >> ${LOGNAME}
echo "Ended at: ${TMSTMP}" >> ${LOGNAME}
exit $RET_STATUS
