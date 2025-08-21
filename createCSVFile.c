#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>

#define MAX_COL_NAME_SIZE 100
#define MAX_FLD_SIZE 200
#define MAX_FILENAME_SIZE 100
#define MAX_FIELDS 350


//Fields information
struct FieldDesc {
    char colName[MAX_COL_NAME_SIZE];
    char colPos [4];
    char colLen[3];
    int iPos;
    int iLen;
    char colType [1];

} ;

struct FieldDesc fldDesc;

int iFieldsArrayLen = 0;
struct FieldDesc Fields[MAX_FIELDS]; 

// parm file info (file layout description)
char parmFileRec [200] = "";

// input file info
int iInRecSize = 0;
char * inputFileRec;
//char inputFileRec [2000] = "";

// Output file info
int iOutRecSize = 0;
char * csvOutFileRec;
//char csvOutFileRec [2000];


// work 
char delimiter[] = ",";
char string_delimiter[] = "\"";
char inField [MAX_FLD_SIZE];
typedef enum { false, true } boolean;
boolean bEmbeddedDelimiter;


// Function declarations
int loadFieldsArray(unsigned char * parmFilename);
int trim(unsigned char * string);
int trimLeadingDecimalZeroes(unsigned char * string);
int trimTrailingDecimalZeroes(unsigned char * string);
int calcInOutRecSize();

// 3 parameters: 1) parameter filename: field definitions 
//               2) input filename: file to create csv from
//               3) output filename: csv file

int main(int argc, char** argv ) {

    char parmFilename[MAX_FILENAME_SIZE +1] = "TestFileRecLayout.txt"; 
    char inFilename[MAX_FILENAME_SIZE + 1] = "";
    char outFilename[MAX_FILENAME_SIZE + 1] = "";

    //**********************************
    // get program parameters; 
    //    Note: first parm is program name
    //**********************************
    if (argc != 4) {
        printf("Expect 3 parameters. Incorrect number of parameters - (%d)\n",argc);
        printf("parm1: filename field definitions\n parm2: input filename\n parm3: output csv filename");
    } 
    //printf ("argc: %d\n",argc);

    strcpy(parmFilename, argv[1]);  
    strcpy(inFilename, argv[2]); 
    strcpy(outFilename, argv[3]); 
    //printf("outFilename: %s\n",outFilename);

    //**********************************
    // Open input and output files
    //**********************************
    FILE * fpInputFile = fopen(inFilename, "r");

    if (fpInputFile == NULL) {
        printf("Input file %s does not exist.", inFilename);
        return 0;
    }

    FILE * fpOutCSVFile = fopen(outFilename, "w");

    if (fpOutCSVFile == NULL) {
        printf("Output CSV file %s could not be created.", outFilename);
        return 0;
    }


    //**********************************
    // get field definitions
    //**********************************
    printf("before loadFieldsArray\n");

    loadFieldsArray(parmFilename);

    calcInOutRecSize();

    //**********************************
    // allocate space for In/Out file buffers
    //**********************************
    inputFileRec = calloc(iInRecSize, sizeof(char *));

    if (inputFileRec == NULL) {
        printf("Could not allocate memory for input file buffer.");
        return 0;        
    }

    csvOutFileRec = calloc(iOutRecSize, sizeof(char *));

    if (csvOutFileRec == NULL) {
        printf("Could not allocate memory for csv output file buffer.");
        return 0;        
    } 

    //**********************************
    // Create Header record
    //**********************************
    csvOutFileRec[0] = '\0';

    for (int i=0; i < iFieldsArrayLen; i++) {

        if (! (csvOutFileRec[0] == '\0')) {
            strcat(csvOutFileRec,",");

        }

        strcat(csvOutFileRec,Fields[i].colName);
    }
    // write completed output record
    fputs(csvOutFileRec, fpOutCSVFile);
    putc('\n',fpOutCSVFile);

    //**********************************
    // loop thru input file
    //**********************************
    //while( fgets(inputFileRec, sizeof(inputFileRec), fpInputFile)  != NULL  ) {
    while( fgets(inputFileRec, iInRecSize, fpInputFile)  != NULL  ) {

        // initialize output record str
        csvOutFileRec[0] = '\0';


        //printf("before For loop\n");
        for (int i=0; i < iFieldsArrayLen; i++) {

            // make field position zero-based starting position
            int iPos = Fields[i].iPos - 1;
            int iLen = Fields[i].iLen;

            // copy field from input file
            memcpy(inField, &inputFileRec[iPos], iLen);
            // make it a string
            inField[iLen] = '\0';

            //printf("field: |%s|\n",inField);
			
			// trim input fld
            trim(inField);
            //printf("trimmed field: |%s|\n",inField);
			
            // if its a number, format to remove leading zeroes
            if ( Fields[i].colType[0] == 'N') {
                trimLeadingDecimalZeroes(inField);        
                trimTrailingDecimalZeroes(inField);
            }    

            //printf("trimmed field: |%s|\n",inField);

            // look for embedded delimiter
            bEmbeddedDelimiter = true;
            if (strstr(inField,delimiter) == NULL) {
                bEmbeddedDelimiter = false;
            }

            //***************************************
            // copy fld string to output record str
            //***************************************

            // if 2nd field, add comma first
            if (! ( csvOutFileRec[0] == '\0') ) {
                strcat(csvOutFileRec,",");
            }    

            //printf("string_delimter:%s\n",string_delimiter);

            if (bEmbeddedDelimiter == true) {
                strcat(csvOutFileRec,string_delimiter);
                strcat(csvOutFileRec,inField);
                strcat(csvOutFileRec,string_delimiter);
            } else {
                strcat(csvOutFileRec,inField);
            } // end-if    

        }   // end-for 

        // write completed output record
        fputs(csvOutFileRec, fpOutCSVFile);
        putc('\n',fpOutCSVFile);
        //printf("csvOutFileRec: %s\n",csvOutFileRec);

    } // end-while

    fclose(fpInputFile);
    fclose(fpOutCSVFile);

    free(inputFileRec);
    free(csvOutFileRec);


}    //end-main

int trimLeadingDecimalZeroes(unsigned char * str) {

    boolean bNegSign = false;

    long len = strlen(str);
    
    // set pointers for input string
    unsigned char * sStart = str;
    unsigned char * pFoundChar;
    unsigned char * sEnd = str + len - 1;

    // Search for decimal point
    pFoundChar = strchr(str,'.');

    // no decimal point found --> assume one
    if (pFoundChar == NULL)   
        pFoundChar = sEnd + 1;

    // stop the character before the character before the decimal point
    // Need to keep one zero before decimal place
    pFoundChar -= 2;

    // Decimal point found. Skip trailing zeroes. If find period, all decimals were zeroes
    while (sStart <= pFoundChar) { 
        if ( ((unsigned char)*sStart == '0') || ((unsigned char)*sStart == '+') || ((unsigned char)*sStart == '-') ) {
            if ( (unsigned char)*sStart == '-') {
                bNegSign = true;
            }
            sStart++;

        }    
        else
            break;
    }     

    // add the neg sign if present
    if (bNegSign) {
        sStart--;
        sStart[0] = '-';
    }

    len =  sEnd - sStart + 1;
    memcpy(str, sStart, len); 
    str[len] = '\0';

    return 0;

}

int trimTrailingDecimalZeroes(unsigned char * str) {

    long len = strlen(str);
    
    // set pointers for input string
    unsigned char * pFoundChar;
    unsigned char * sStart = str;
    unsigned char * sEnd  = str + len - 1;

    // Search for decimal point
    pFoundChar = strchr(str,'.');

    // no decimal point found
    if (pFoundChar == NULL)   
        return 0;

    // Decimal point found. Skip trailing zeroes. If find period, all decimals were zeroes
    while (sEnd >= pFoundChar) { 
        if ( ((unsigned char)*sEnd == '0') || ((unsigned char)*sEnd == '.')) 
            sEnd--;
        else
            break;
    }     

    // Set new end-of-string
    len =  sEnd - sStart + 1;
    str[len] = '\0';
    
    return 0;

}

int trim(unsigned char * str) {

    long len = strlen(str);

    // set pointers for input string
    unsigned char * sStart = str;
    unsigned char * sEnd  = str + len - 1;

    //trim leading spaces - set pointer to first non space char
    while (sStart < sEnd) {
        if (isspace( (unsigned char)*sStart))  
            sStart++;
        else
            break;    
    } 

    while (sEnd >= sStart) {   
        if (isspace( (unsigned char)*sEnd)) 
            sEnd--;
        else
            break;
    } 

    len =  sEnd - sStart + 1;
	
	if (len > 0)
		memcpy(str, sStart, len); 

    str[len] = '\0';
    
    return 0;

    //printf("len: %d\n",len);

    /*
    for (int i = (len - 1); i >= 0; i--) {
        //printf("before isSpace ->  %c\n",string[i] );
        if (isspace(str[i]) ) {
            str[i] = '\0';
        } else {
            return 0;
        }  // end-if
    }  // end-for
    */
}

int calcInOutRecSize() {

    // Look at last array element
    fldDesc = Fields[iFieldsArrayLen - 1]; 

    iInRecSize = fldDesc.iPos + fldDesc.iLen + 1; 
    // count possible delimiter + string Delimiters + null string terminator
    iOutRecSize = (fldDesc.iPos + fldDesc.iLen) + (iFieldsArrayLen * 3);

    //printf("iInRecSize %d\n",iInRecSize);
    //printf("iOutRecSize %d\n",iOutRecSize);

}


int loadFieldsArray(unsigned char * parmFilename) {

    int iFldIdx = 0;
    char * token;
    char parmRec[200];

    FILE * fpParmFile = fopen(parmFilename, "r");

    if (fpParmFile == NULL) {
        printf("Parm file %s does not exist.", parmFilename);
        return 0;
    }

    // get next parm File and parse into separate fields
    while(fgets(parmRec, sizeof(parmFileRec), fpParmFile)  != NULL) {

        // Array NOF Entries exceeded?
        if (iFldIdx == MAX_FIELDS) {
            printf("Fields Array limit of %d exceeded.", MAX_FIELDS);
            return 0;
        }

        token = strtok(parmRec,",");  
        printf("token:%s\n",token);
        int x = 0;

        while(token) {
            switch(x) {
                case 0:
                    strcpy(fldDesc.colName,token);
                    break;
                case 1: 
                    strcpy(fldDesc.colPos,token);
                    fldDesc.iPos = atoi(fldDesc.colPos);
                    printf("token iPos %d\n",fldDesc.iPos);
                    break;
                case 2:    
                    strcpy(fldDesc.colLen,token);
                    fldDesc.iLen = atoi(fldDesc.colLen);
                    printf("token iLen %d\n",fldDesc.iLen);
                    break;
                case 3:    
                    strcpy(fldDesc.colType,token);
                    printf("token colType %d\n",fldDesc.colType);
                    break;

                default:
                    break;
            } //end-switch

            token = strtok(NULL, ",");
            printf("token:%s\n",token);
            
            x++;

        } // end-while

        // Create array of field definitions
        iFieldsArrayLen++;
        Fields[iFldIdx++] = fldDesc;

    } // end-while


    printf("NOF Fields=%d\n",iFieldsArrayLen);
    //printf("last field %s\nfpParmFile 
    fclose(fpParmFile);
    
    return 0;
}