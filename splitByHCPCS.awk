#!/usr/bin/awk -f
#
#
# Example of how to execute awk script:
# ./splitByHCPCS.awk -v outfile="/app/IDRC/XTR/CMS/data/PBAR_Q6_PSPS" infile
# 
#  outfile=path and file mask to be used for all output files
#   infile=path and full filename of input file. (no quotes)
#       Ex:  /app/IDRC/XTR/CMS/data/PBAR_PSPSQ6.txt
#                                
##############################
# 
##############################


BEGIN {

tmstmp=strftime("%Y%m%d.%H%M%S",systime())
 
# Output filenames
#outfile="/app/IDRC/XTR/CMS/data/PBAR_Q6_PSPS"

F01=outfile"01_"tmstmp".txt"
F02=outfile"02_"tmstmp".txt"
F03=outfile"03_"tmstmp".txt"
F04=outfile"04_"tmstmp".txt"
F05=outfile"05_"tmstmp".txt"
F06=outfile"06_"tmstmp".txt"
F07=outfile"07_"tmstmp".txt"
F08=outfile"08_"tmstmp".txt"
F09=outfile"09_"tmstmp".txt"
F10=outfile"10_"tmstmp".txt"
F11=outfile"11_"tmstmp".txt"
F12=outfile"12_"tmstmp".txt"
F13=outfile"13_"tmstmp".txt"
F14=outfile"14_"tmstmp".txt"
F15=outfile"15_"tmstmp".txt"
F16=outfile"16_"tmstmp".txt"
F17=outfile"17_"tmstmp".txt"
F18=outfile"18_"tmstmp".txt"
F19=outfile"19_"tmstmp".txt"
F20=outfile"20_"tmstmp".txt"
F21=outfile"21_"tmstmp".txt"
F22=outfile"22_"tmstmp".txt"
F23=outfile"23_"tmstmp".txt"
F24=outfile"24_"tmstmp".txt"
F25=outfile"25_"tmstmp".txt"
F26=outfile"26_"tmstmp".txt"

#print "F01: "F01
#print "F02: "F02 


}

{

	hcpcs=substr($0,1,5)
	#print hcpcs 

	if ( hcpcs >= "0000 " && hcpcs <= "09999" ) {
		print $0 > F01
	}
	else if ( hcpcs >= "1000 " && hcpcs <= "14999" ) {
		print $0 > F02
	}
	else if ( hcpcs >= "1500 " && hcpcs <= "19999" ) {
		print $0 > F03
	}
	else if ( hcpcs >= "2000 " && hcpcs <= "24999" ) {
		print $0 > F04
	}
	else if ( hcpcs >= "2500 " && hcpcs <= "29999" ) {
		print $0 > F05
	}
	else if ( hcpcs >= "3000 " && hcpcs <= "32999" ) {
		print $0 > F06
	}
	else if ( hcpcs >= "3300 " && hcpcs <= "37999" ) {
		print $0 > F07
	}
	else if ( hcpcs >= "3800 " && hcpcs <= "38999" ) {
		print $0 > F08
	}
	else if ( hcpcs >= "3900 " && hcpcs <= "39999" ) {
		print $0 > F09
	}
	else if ( hcpcs >= "4000 " && hcpcs <= "49999" ) {
		print $0 > F10
	}
	else if ( hcpcs >= "5000 " && hcpcs <= "53999" ) {
		print $0 > F11
	}
	else if ( hcpcs >= "5400 " && hcpcs <= "55999" ) {
		print $0 > F12
	}
	else if ( hcpcs >= "5600 " && hcpcs <= "58999" ) {
		print $0 > F13
	}
	else if ( hcpcs >= "5900 " && hcpcs <= "59999" ) {
		print $0 > F14
	}
	else if ( hcpcs >= "6000 " && hcpcs <= "64999" ) {
		print $0 > F15
	}
	else if ( hcpcs >= "6500 " && hcpcs <= "68999" ) {
		print $0 > F16
	}
	else if ( hcpcs >= "6900 " && hcpcs <= "69999" ) {
		print $0 > F17
	}
	else if ( hcpcs >= "7000 " && hcpcs <= "74999" ) {
		print $0 > F18
	}
	else if ( hcpcs >= "7500 " && hcpcs <= "79999" ) {
		print $0 > F19
	}
	else if ( hcpcs >= "8000 " && hcpcs <= "89999" ) {
		print $0 > F20
	}
	else if ( hcpcs >= "9000 " && hcpcs <= "99199" ) {
		print $0 > F21
	}
	else if ( hcpcs >= "9920 " && hcpcs <= "99999" ) {
		print $0 > F22
	}
	else if ( hcpcs >= "A000 " && hcpcs <= "H9999" ) {
		print $0 > F23
	}
	# AND NOT "UNK"
	else if ( hcpcs >= "J000 " && hcpcs <= "Z9999" ) {
		if ( hcpcs == "UNK  " ) {
			print $0 > F25
		} else {
			print $0 > F24
		}
	}
	else if ( hcpcs == "~    " ) {
		print $0 > F25
	}	
	else {
		print $0 > F26

	}




}


END {

}
