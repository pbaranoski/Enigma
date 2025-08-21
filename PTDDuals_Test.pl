#!/usr/bin/perl 
#
# NOTE: 
#   
# !!! Need to add ability to write output to separate State files.
# !!! Need sample dataset names with bytes to replace with state code. 
#
# This script will suppress (by blanks or zeroes) specific fields per state
# based on the PTDDuals[Monthly/Daily]StParms.txt file. It will also 
# write output records into separate state files.
#
# Example of how to execute awk script (XX represents the position of 
#   the state code in the output filename):
#
# ./PartDDuals.pl 
# 
#                                
##############################
# 
##############################



##########################################################
# Process state parm file
##########################################################
sub processParmFile
{

	my $parmFilename = '/home/BZH3/PTDDualsMonthlyStParms.txt';

	open(hParmFile, '<', $parmFilename) or die $!;

	#print("File $parmFilename opened successfully!\n");

	while(<hParmFile>) {
		
		# store input record
		my $parmRec = $_;
		
		#skip comments
		if ( substr($parmRec,0,1) eq "#" ) {
			next;
		}	

		# assign key/value variables
		$key = substr($parmRec,0,2);
		$value = substr($parmRec,3);
		#print("value" . $value);

		# Process special information rows 
		if ( $key eq "ZX" ) {
			@FldNumAlpha = split('\|', $value);
			#print ("0:" . $FldNumAlpha[0] . "\n");
			#print ("4:" . $FldNumAlpha[4] . "\n");
			next;
		}
			
		if ( $key eq "ZY" ) {
			@FldPos = split('\|', $value);
			next;
		}

		if ( $key eq "ZZ" ) {
			@FldLen = split('\|', $value);
			next;
		}

		# process individual state parm recs
		# convert State Parm values to Y/N values
		$value =~ s/[X]{2,2}[ ]?/Y/g;
		$value =~ s/   /N/g;
		
		#print("value:" . $value);
		
		# build array of state record key/value pairs
		push(@stateData, $key);
		push(@stateData, $value);

	   
	}  # end-while

	# change array to associative array (key/value pairs)
	%states = @stateData;
	print("states-1:" . $states{"OK"} . "\n");
		
	close(hParmFile);	
	
}

#########################################
#MAIN
#########################################
processParmFile;


##############################
# Process Extract file
##############################
my $parmFilename = '/home/BZH3/PTDExtract.txt';

my $spaces="                                        ";
my $stars="****************************************"	;
my $zeroes="0000000000000000000000000000000000000000";
my $suppressValues = "";

my $holdStateCd = "  ";
my $stateCdOffset = 0;  # zero-based


open(hExtractFile, '<', $parmFilename) or die $!;

#print("File $Extract file opened successfully!\n");

while(<hExtractFile>) {

	# store input record
	my $ExtractRec = $_;
		
	$curStateCd = substr($ExtractRec,$stateCdOffset,2);
	#print("curStateCd:" . $curStateCd . "\n");
	
	# Has Extract state code changed?	
	if ( $holdStateCd ne $curStateCd ) {

		#print ("state changed:" . $curStateCd . "\n");

		# Save last State code value
		$holdStateCd = $curStateCd;
	
		# Get state parm Key/value record 
		$key = $curStateCd;
		$stRec = $states{$key};
		print("state rec:" . $stRec . "\n");

		# --> convert state parm record to an array of flds
		@flds = split('\|', $stRec);
		$NOFflds  = @flds ;
		print ("NOFflds:". $NOFflds . "\n");		

		# If NOFflds == 0 --> state record does not exist (not defined) --> error!!!
		# Do not write output record!! --> skip processing
		if ( $NOFflds == 0) {
			next;
		}


		#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		# --> loop thru state flds array searching for blank fields
		# --> Create delimited string of blank fld columns.
		#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		$FldsNot2Send = "";
		
		for ($i=0; $i < $NOFflds; $i++) {
			# --> create comma-delimited string of blank fields				
			print("flds[$i]:" . $flds[$i] . "\n");
			
			if ( $flds[$i] eq "N" ) {
				if ( $FldsNot2Send eq "") {
					$FldsNot2Send = $i;
				} 
				else {
					$FldsNot2Send = $FldsNot2Send . "," . $i;
				}
				print ("$FldsNot2Send: " . $FldsNot2Send . "\n");

			}

		} # end-for
		
		print ("FldsNot2Send: " . $FldsNot2Send . "\n");

		#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		# --> Create blank fields array from delimited string
		#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		@arrFlds2Blank = split(',', $FldsNot2Send);
		$NOFFlds2Blank  = @arrFlds2Blank ;
		print ("NOFFlds2Blank:". $NOFFlds2Blank . "\n");
		
		
	}# end-if	


	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Blank out appropriate fields in extract record for state 	
	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	print ("NOFarrFlds2Blank: " . $NOFFlds2Blank); 
	
	#print ("outRec:" . $ExtractRec . "\n");
	
	for my $idx (0 .. $#arrFlds2Blank) {		
		print("idx" . $idx);
		$i = $arrFlds2Blank[$idx];
		$pos = $FldPos[$i];
		$len = $FldLen[$i];
		$numAlpha = $FldNumAlpha[$i];
		
		print ("pos: " . $pos . "\n");
		print ("len: " . $len . "\n");
		print ("numAlpha: " . $numAlpha . "\n");

		#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		# Set proper suppression value for field: zeroes/spaces
		#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		if ( $numAlpha eq "X" ) {
			$suppressValues = substr($spaces,1,$len);
		} else {
			$suppressValues = substr($zeroes,1,$len);
		}
		
		#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		# Build record with suppressed field
		#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		print ("-->" . $ExtractRec . "\n");
		
		substr($ExtractRec, $pos - 1, $len, $suppressValues);
		
		print ("-->" . $ExtractRec . "\n");
		
	} # end-for	
		

} # end-while

	
close(hExtractFile);	