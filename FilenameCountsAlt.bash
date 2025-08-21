#!/usr/bin/bash


getExtractFilenamesAndCounts() {

	# function paremeters
	LOGNAME=$1
	echo "function logname: ${LOGNAME}"
	
	COPY_INTO_FILENAMES=`grep 'Executing: COPY INTO' ${LOGNAME} | cut -d/ -f2 `
	ROW_COUNTS=`awk -F "," '/rows_unloaded/{getline;print $1}' ${LOGNAME} `


	echo ""
	echo "COPY_INTO_FILENAMES: ${COPY_INTO_FILENAMES} "
	echo "ROW_COUNTS: ${ROW_COUNTS}"

	filenamesAndCounts=`paste <(printf %s "${COPY_INTO_FILENAMES}") <(printf %s "${ROW_COUNTS}") | xargs printf "%s %'14d\n" `
	
	echo "${filenamesAndCounts}"

##################################################################
	echo "test alternate way instead of paste command"

	FILENAMES_ARR=( ${COPY_INTO_FILENAMES} )
	#echo "${FILENAMES_ARR[0]}"
	#echo "${FILENAMES_ARR[1]}"

	ROW_COUNT_ARR=( ${ROW_COUNTS} )
	#echo "${ROW_COUNT_ARR[0]}"
	#echo "${ROW_COUNT_ARR[1]}"
	
	NOF_FILES=${#FILENAMES_ARR[@]}
	
	for (( i = 0; i <= $NOF_FILES; i++ ))
	do 

		FilenamesNCounts=`echo "${FilenamesNCounts}"; echo ${FILENAMES_ARR[$i]} ${ROW_COUNT_ARR[$i]}; `

	done

	emailInfo=`echo ${FilenamesNCounts} | xargs printf "%s %'14d\n" `
	echo "${emailInfo}"

##################################################################
# Need the DataDir variable
#
	echo "test alternate way #2 using paste command with temp files"
	
	TMSTMP_ID=`date +%Y%m%d.%H%M%S`
	PID=$$
	
	TMP_FILENAMES="emailFilenames.${PID}.${TMSTMP_ID}.txt"
	TMP_ROW_COUNTS="emailRowCounts.${PID}.${TMSTMP_ID}.txt"

	grep 'Executing: COPY INTO' ${LOGNAME} | cut -d/ -f2 > ${TMP_FILENAMES}
	NOF_ROWS_EYE_CATCHERS=`grep -n 'rows_unloaded,input_bytes,output_bytes' ${LOGNAME} | cut -d: -f1 `

	
	for NOF_ROWS_EYE_CATCHER_REC_NO in ${NOF_ROWS_EYE_CATCHERS}
	do

		# Get actual row number in log file that contains the extracted row count
		NOF_ROWS_INFO_REC_NO=`expr ${NOF_ROWS_EYE_CATCHER_REC_NO} + 1 ` 
		echo "NOF_ROWS_INFO_REC_NO=${NOF_ROWS_INFO_REC_NO}" 

		# Get extracted row count
		NOF_ROWS=`sed -n "${NOF_ROWS_INFO_REC_NO},${NOF_ROWS_INFO_REC_NO}p" ${LOGNAME} | cut -d, -f1 ` 
		echo "NOF_ROWS=${NOF_ROWS}" 
		
		echo "${NOF_ROWS}" | xargs printf "%d \n" >> ${TMP_ROW_COUNTS}

	done

	
	filenamesAndCounts2=`paste ${TMP_FILENAMES} ${TMP_ROW_COUNTS} | xargs printf "%s %'14d\n" `
	echo "${filenamesAndCounts2}"
	
	rm ${TMP_FILENAMES}  
	rm ${TMP_ROW_COUNTS}

}


getExtractFilenamesAndCounts SRTR_ENC_HHA_Extract_20230217.134030.log