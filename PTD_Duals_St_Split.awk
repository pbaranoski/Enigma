#!/usr/bin/awk -f
#
#
# NOTE: awk cannot process separate input files. Input files are listed
# in order to be processed on the command line, and are "concatenated" as input 
# into the script. You can tell the end of one input file and the beginning of 
# another using the awk reserved variables NR (NOF record read) and 
# FNR (NOF current file read). These variables are the same value when processing 
# the first file, but FNR is set to 1 when reading the 2nd input file. 
#
# This script will suppress (by blanks or zeroes) specific fields per state
# based on the PTDDuals[Monthly/Daily]StParms.txt file. It will also 
# write output records into separate state files.
#
# Example of how to execute awk script (XX represents the position of 
#   the state code in the output filename):
#
# ./PTD_Duals_state_split.awk -v outfile_model="dataDir/PTD_DUALS_MONTHLY_XX.${TMSTMP}.txt" ST_PARMFILE ExtractFilename
#
#
# Paul Baranoski 2024-12-17 Modified stateCdOffset from 201 to 221.
#                           Changed "print substr(outRec,1,185) > st_outputfile" length from 185 to 205  
#                                
##############################
# 
##############################

BEGIN {
	
	# 40 spaces/stars/zeroes
	spaces="                                        "
    stars="****************************************"	
    zeroes="0000000000000000000000000000000000000000"
	suppressValues = ""
	
	bProcessSTParmFile="Y"
	holdStateCd = "  "
	stateCdOffset = 221

}

{

	################################################	
	# Check for State Parm File EOF
	################################################
	if (bProcessSTParmFile == "Y") {
	
		# skip comment lines
		if (substr($0,1,1) == "#") {
			next;
		}
		
		# Does the NOF Total Recs read != NOF Total Recs read for file? If Yes --> EOF Parm file.
		if (NR != FNR) {
			print "StateParmFileEOF"
			bProcessSTParmFile = "N"
		}	
	}

	################################################
	# Load St Parm File values into Array
	################################################
	if (bProcessSTParmFile == "Y") {
	
		key = substr($0,1,2)
		value = substr($0,4)

		#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		# Process special information row
		#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

		# Load array of data suppression types
		if ( key == "ZX" ) {
			# remove spaces before creating array
			gsub(/[ ]*/,"",value)
			split(value, FldNumAlpha, "|")
			next;
		}
		
		# Load array of field positions
		if ( key == "ZY" ) {
			split(value, FldPos, "|")
			next;
		}

		# Load array of field lengths
		if ( key == "ZZ" ) {
			split(value, FldLen, "|")
			next;
		}

		##############################
		# process actual state parms	
		##############################
		# convert XX to Y; and blanks to N
		gsub(/[X]{2,2}[ ]?/,"Y",value)
		gsub("   ","N",value)
		
		##print "key:" key
		##print "value:" value
		
		# load array record for state	
		states[key] = value
		
		# process next record	
		next;	
	}
	
	
	#########################################
	# Process Extract file
	#########################################
	curStateCd = substr($0,stateCdOffset,2)


	# Has Extract state code changed?	
	if ( holdStateCd != curStateCd ) {
		
		print "state changed:" curStateCd

		# Save last State code value
		holdStateCd = curStateCd
		
		# 
		print "outfile_model:" outfile_model
	    print "curStateCd:" curStateCd	
        st_outputfile = outfile_model		
	    gsub("XX",curStateCd,st_outputfile)
		print "st_outputfile:" st_outputfile
	
		# Get state parm Key/value record 
		key = curStateCd
		stRec = states[key]

		# --> convert state parm record to an array of flds
		NOFflds = split(stRec, flds, "|")
		print "NOFflds:" NOFflds


		# If NOFflds == 0 --> state record does not exist (not defined) --> error!!!
		# Do not write output record!! --> skip processing
		if ( NOFflds == 0) {
			#next;
			exit 12;
		}
	
		
		#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		# --> loop thru state flds array searching for blank fields
		# --> Create delimited string of blank fld columns.
		#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		FldsNot2Send = ""
		
		for (i=1; i < NOFflds; i++) {
			
			# --> create comma-delimited string of blank fields		
			if ( flds[i] == "N" ) {
				#print "blank field:" i
				if ( FldsNot2Send == "") {
					FldsNot2Send = i
				} 
				else {
					FldsNot2Send = FldsNot2Send "," i
				}
				#print "FldsNot2Send: " FldsNot2Send

			}

		} # end-for
		
		# Is blank when sending every field
		##print "FldsNot2Send: " FldsNot2Send
		
		#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		# --> Create blank fields array from delimited string
		#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		NOFFlds2Blank = split(FldsNot2Send, arrFlds2Blank, ",")
		##print "NOFFlds2Blank:" NOFFlds2Blank
		
			
	} # end-if state changed


	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Blank out appropriate fields in extract record for state 	
	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	#print "NOFarrFlds2Blank: " NOFFlds2Blank 
	
	outRec = $0
	#print "outRec:" outRec
	
	#for (idx = 0; idx < NOFFlds2Blank; idx++) {  
	for (idx in arrFlds2Blank) {
		#print "idx" idx
		i = arrFlds2Blank[idx]
		pos = FldPos[i]
		len = FldLen[i]
		numAlpha = FldNumAlpha[i]
		
		#print "pos: " pos
		#print "len: " len
		#print "numAlpha: " numAlpha

		
		#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		# Set proper suppression value for field: zeroes/spaces
		#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		if ( numAlpha == "X" ) {
			suppressValues = spaces
		} else {
			suppressValues = zeroes
		}
		
		#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		# Build record with suppressed field
		#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		if ( i == 1 ) {
			outRec = substr(suppressValues,1,len) substr(outRec,pos+len)
		} else if ( i == NOFflds) {
			outRec = substr(outRec,1,pos - 1) substr(suppressValues,1,len) 
		} else {
			outRec = substr(outRec,1,pos - 1) substr(suppressValues,1,len) substr(outRec,pos+len) 
		}	

		
	} # end-for

	# print output record
	print substr(outRec,1,205) > st_outputfile		


}


END {

}
