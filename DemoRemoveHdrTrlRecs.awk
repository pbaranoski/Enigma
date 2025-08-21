#!/usr/bin/awk -f
#
# 
# Example of how to execute awk script:
# ./DemoRemoveHdrTrlRecs.awk -v outfile="/app/IDRC/XTR/CMS/data/DEMO_FINDER_PLNXXXXX_AUG" infile
# 
#  outfile=path and file mask to be used for all output files
#   infile=path and full filename of input file. (no quotes)
#       Ex:  /app/IDRC/XTR/CMS/data/PBAR_PSPSQ6.txt
#                                
##############################
# 
##############################

BEGIN {
 
	# Assign parameter to awk variable
	OutputFile=outfile

	#print "OutputFile: "OutputFile

}

{

	recType=substr($0,1,4)
	#print recType 

	if ( recType != "HDRH" && recType != "TRLH") {
		HICN=substr($0,1,11)
		print HICN > OutputFile
	}

}


END {

}
