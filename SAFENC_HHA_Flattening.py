import os
import sys


###############################################
# constants
###############################################

#define MAX_VAL_GRPS_ARRAY_LEN  (14 * 25) 
#define MAX_RC_GRPS_ARRAY_LEN  (633 * 25) + 2

#3404 + 2 for null string terminator
INPUT_MAXLINE = 3406 
OUTPUT_MAXLINE = 32767
MAX_VAR_OUTPUT_AREA_LEN = 2000 

RC_GRP_MAX_ARRAY_ELEMENTS = 25
RC_GRP_ELEMENT_LEN = 633

sOutputRecFixed : str = ""
sVarOutputArea : str = ""
sClmRevCntrGrp : str = ""


iNOF_DGNS_R_CD_GRP : int = 0
iNOF_MCO_PRD_GRP : int = 0
iNOF_CLM_DGNS_D_GRP : int= 0 
iNOF_CLM_DGNS_E_GRP : int  = 0
iNOF_CLM_RLT_COND_GRP : int = 0
iNOF_CLM_RLT_OCRNC_GRP : int  = 0
iNOF_CLM_OCRNC_SPAN_CD_GRP : int = 0 
iNOF_CLM_VAL_GRP : int = 0
iNOF_CLM_REV_CNTR_GRP : int = 0 


iNOFRecsRead : int = 0
iNOFRecsWritten : int = 0
iNOFClaimsWritten : int = 0
iNOFClaimLinesWritten : int = 0

# increment nof lines for claim 
iClmTotSgmtCnt : int = 0
iClmSgmtNum  : int = 0
iClmTotLineCnt : int = 0
iClmSgmtLineCnt : int = 0

InDirNFilename : str = ""
outDirNFilename : str = "" 


def getParms():

#    print("Start function getParms")	

    #global InDirNFilename,  outDirNFilename
    #InDirNFilename = r"C:\Users\PaulBaranoski\OneDrive - Index Analytics\Documents\Sprints\SAF Encounter Cloud\SAFENC_HHA_FINAL_QTR2_20230706.134541_sorted.txt"
    #outDirNFilename = r"C:\Users\PaulBaranoski\OneDrive - Index Analytics\Documents\Sprints\SAF Encounter Cloud\SAFENC_HHA_Output.txt"
     
    if len(sys.argv) > 1:
        # module being called from shell script
        lstParms = sys.argv
        if len(lstParms) != 3:
                print(f"Incorrect number of parameters to script. {len(lstParms)} parms were sent to script")
                sys.exit(12)  

        global InDirNFilename,  outDirNFilename  
        InDirNFilename = lstParms[1]
        outDirNFilename = lstParms[2]
        
        print(f"InDirNFilename:{InDirNFilename}")
        print(f"outDirNFilename:{outDirNFilename}")
    
#    print("End function getParms")	


def load_fixed_len_clm_info(inputRecord):

#    print("Start function load_fixed_len_clm_info")	

    REC_KEY = inputRecord[0:33]

    IDR_GEO_BENE_SK = inputRecord[0:10]
    IDR_CLM_DT_SGNTR_SK = inputRecord[10:23]
    IDR_CLM_TYPE_CD_SK  = inputRecord[23:28]
    IDR_CLM_NUM_SK = inputRecord[28:33]
    IDR_CLM_LINE_NUM = inputRecord[33:43]
	
    IDR_CLM_TYPE_CD = inputRecord[43:48]
    IDR_CLM_HIC_NUM = inputRecord[48:60]
    IDR_GEO_BENE_SSA_STATE_CD = inputRecord[60:62]
    IDR_CLM_FROM_DT = inputRecord[62:70]
    IDR_CLM_THRU_DT = inputRecord[70:78]
    IDR_EDPS_CREATE_DATE = inputRecord[78:86]
    IDR_CLM_CNTL_NUM = inputRecord[86:126]
    IDR_ORIG_CLM_CNTL_NUM = inputRecord[126:166]
    IDR_EDPS_LOAD_DATE = inputRecord[166:174]
    IDR_CLM_FAC_TYPE_CD = inputRecord[174:175]
    IDR_CLM_SRVC_CLSFCTN_TYPE_CD = inputRecord[175:176]
    IDR_CLM_BILL_FREQ_CD = inputRecord[176:177]
    IDR_GEO_BENE_SSA_CNTY_CD = inputRecord[177:180]
    IDR_CLM_SUBMSN_DT = inputRecord[180:188]
    IDR_CLM_CNTRCT_NUM = inputRecord[188:193]

    IDR_GEO_ZIP9_CD = inputRecord[193:202]
    IDR_BENE_SEX_CD = inputRecord[202:203]
    IDR_BENE_RACE_CD = inputRecord[203:205]
    IDR_CLM_PTNT_BIRTH_DT = inputRecord[205:213]
    IDR_CLM_CWF_BENE_MDCR_STUS_CD = inputRecord[213:215]
    IDR_CLM_LAST_NAME = inputRecord[215:275]
    IDR_CLM_1ST_NAME = inputRecord[275:310] 
    IDR_CLM_INTL_MDL_NAME = inputRecord[310:311]

    IDR_CLM_ICD_VRSN_CD = inputRecord[311:312]            
    IDR_CLM_PRNCPAL_DGNS_CD = inputRecord[312:319] 

    IDR_CLM_ENCTR_OTHR_PYR_PD_AMT = inputRecord[319:334]   # PIC 9(12)V99S
    IDR_CLM_BLG_PRVDR_NPI_NUM  = inputRecord[334:344]
    IDR_CLM_ATNDG_PRVDR_NPI_NUM  = inputRecord[344:354]
    IDR_CLM_ATNDG_PRVDR_LAST_NAME = inputRecord[354:414] 
    IDR_CLM_ATNDG_PRVDR_1ST_NAME = inputRecord[414:449] 
    IDR_CLM_ATNDG_PRVDR_MDL_NAME = inputRecord[449:474]
    IDR_CLM_OPRTG_PRVDR_NPI_NUM  = inputRecord[474:484] 
    IDR_CLM_OPRTG_PRVDR_LAST_NAME = inputRecord[484:544]
    IDR_CLM_OPRTG_PRVDR_1ST_NAME = inputRecord[544:579]
    IDR_CLM_OPRTG_PRVDR_MDL_NAME = inputRecord[579:604] 
    IDR_CLM_OTHR_PRVDR_NPI_NUM = inputRecord[604:614] 
    IDR_CLM_OTHR_PRVDR_LAST_NAME = inputRecord[614:674] 
    IDR_CLM_OTHR_PRVDR_1ST_NAME = inputRecord[674:709]
    IDR_CLM_OTHR_PRVDR_MDL_NAME = inputRecord[709:734]  

    IDR_CLM_PTNT_CNTL_NUM = inputRecord[734:814]
    IDR_CLM_PTNT_MDCL_REC_NUM = inputRecord[814:894] 
    IDR_PTNT_STUS_CD = inputRecord[894:896]
    IDR_ICD_VRSN_E_CD = inputRecord[896:897]
    IDR_DGNS_E_CD = inputRecord[897:904] 

    IDR_CLM_SBMT_CHRG_AMT = inputRecord[904:919]   # PIC 9(12)V99S
    IDR_CLM_BLG_PRVDR_ZIP9_CD = inputRecord[919:928] 

    IDR_CLM_RNDRG_PRVDR_NPI_NUM  = inputRecord[928:938] 
    IDR_CLM_RNDRG_PRVDR_LAST_NAME = inputRecord[938:998] 
    IDR_CLM_RNDRG_PRVDR_1ST_NAME = inputRecord[998:1033] 
    IDR_CLM_RNDRG_PRVDR_MDL_NAME = inputRecord[1033:1058] 

    IDR_CLM_DGNS_TOT_OCRNC_CNT = inputRecord[1058:1060] 
    IDR_CLM_DSCHRG_DT = inputRecord[1060:1068] 
    IDR_CLM_ACTV_CARE_FROM_DT = inputRecord[1068:1076]

    IDR_CLM_RFRG_PRVDR_NPI_NUM  = inputRecord[1076:1086] 
    IDR_CLM_RFRG_PRVDR_LAST_NAME = inputRecord[1086:1146] 
    IDR_CLM_RFRG_PRVDR_1ST_NAME = inputRecord[1146:1181]  
    IDR_CLM_RFRG_PRVDR_MDL_NAME = inputRecord[1181:1206] 

    IDR_CNTRCT_PBP_NUM = inputRecord[2319:2322]
    IDR_CLM_CNTRCT_TYPE_CD = inputRecord[2322:2324]
    IDR_CLM_LINE_CNTRCT_TYPE_CD = inputRecord[2324:2326]
    IDR_CLM_CHRT_RVW_SW = inputRecord[2326:2327]
    IDR_BENE_SK = inputRecord[2327:2345]
    IDR_CLM_FINL_ACTN_IND = inputRecord[2345:2346]
    IDR_CLM_LINE_FINL_ACTN_IND = inputRecord[2346:2347]
    IDR_CLM_LTST_CLM_IND = inputRecord[2347:2348]
    IDR_CLM_LINE_LTST_CLM_IND = inputRecord[2348:2349]
    IDR_CLM_LINE_ENCTR_STUS_CD = inputRecord[2349:2369]
    IDR_CLM_BLG_PRVDR_TXNMY_CD = inputRecord[2369:2419]
    IDR_CLM_ATNDG_PRVDR_TXNMY_CD = inputRecord[2419:2429]
    IDR_CLM_EDPS_STUS_CD = inputRecord[2429:2449]
    IDR_CLM_OBSLT_DT = inputRecord[2449:2457]
    IDR_CLM_ERR_SGNTR_SK = inputRecord[2457:2475]
    IDR_GEO_BENE_EFCTV_SK = inputRecord[2475:2485]
    IDR_CLM_DT_SGNTR_EFCTV_SK = inputRecord[2485:2498]
    IDR_CLM_TYPE_EFCTV_CD  = inputRecord[2498:2503]
    IDR_CLM_NUM_EFCTV_SK  = inputRecord[2503:2508]				   

    IDR_CLM_CNTRCT_AMT = inputRecord[2508:2523]    # 9(12)V99S
    IDR_CLM_PTNT_LBLTY_AMT = inputRecord[2523:2538] # 9(12)V99S

    IDR_BENE_EQTBL_BIC_CD  = inputRecord[2538:2540]
    IDR_CLM_BPRVDR_ADR_LINE_1_TXT = inputRecord[2540:2595]
    IDR_CLM_BPRVDR_ADR_LINE_2_TXT = inputRecord[2595:2650]
    IDR_CLM_BPRVDR_ADR_LINE_3_TXT = inputRecord[2650:2705]
    IDR_CLM_BPRVDR_CITY_NAME = inputRecord[2705:2735]
    IDR_CLM_BPRVDR_USPS_STATE_CD = inputRecord[2735:2737]
    IDR_BPRVDR_ADR_ZIP_CD = inputRecord[2737:2746]
    IDR_CLM_SUBSCR_ADR_LINE_1_TXT = inputRecord[2746:2801]
    IDR_CLM_SUBSCR_ADR_LINE_2_TXT = inputRecord[2801:2856]
    IDR_CLM_SUBSCR_ADR_LINE_3_TXT = inputRecord[2856:2911]
    IDR_CLM_SUBSCR_CITY_NAME = inputRecord[2911:2941] 
    IDR_CLM_SUBSCR_USPS_STATE_CD = inputRecord[2941:2943] 
    IDR_SUBSCR_ADR_ZIP_CD = inputRecord[2943:2952] 

    IDR_CLM_BLG_PRVDR_TAX_NUM = inputRecord[3377:3387]
    IDR_BENE_ID_TYPE_CD = inputRecord[3387:3388]
    IDR_BENE_MBI_ID = inputRecord[3388:3399] 
    IDR_CLM_CHRT_RVW_EFCTV_SW = inputRecord[3399:3400]
    IDR_CLM_EDPS_CHRT_RVW_SW = inputRecord[3400:3401]
    IDR_END_OF_REC_IND = inputRecord[3401:3404]

#    print(f"{REC_KEY=}")	

#    print(f"{IDR_GEO_BENE_SK=}")	
#    print(f"{IDR_CLM_LINE_NUM=}")	
    
    #####################################################
    # Build Output fixed portion of record
    #####################################################
    global iNOFClaimsWritten
    iNOFClaimsWritten += 1
    
    global sOutputRecFixed

    sOutputRecFixed = ( 

                       # REC_KEY +
	                    ('0' * 5) +  # REC_LENGTH_CNT [5];  // PIC S9(5) COMP-3.

	                    (' ' * 1) + # H_FILLER_1 [1]     
	                    IDR_CLM_TYPE_CD [1:5] +   # input file clm_type_cd is 5 bytes.
                        IDR_CLM_HIC_NUM  + 
                        IDR_BENE_ID_TYPE_CD + 
                        IDR_BENE_MBI_ID +  
                        IDR_GEO_BENE_SSA_STATE_CD + 
                        IDR_CLM_FROM_DT + 
                        IDR_CLM_THRU_DT + 
                        IDR_EDPS_CREATE_DATE +
                        IDR_CLM_CNTL_NUM[:23] +
                        IDR_ORIG_CLM_CNTL_NUM[:23] +
	                    (' ' * 7) + # H_FILLER_4 [7]     
                        IDR_EDPS_LOAD_DATE + 
                        
                        IDR_CLM_CNTL_NUM[4:13] +   # Move 1st 13 bytes to 9(18), then move that to 9(9) field.

                        ('0' * 2) + # CLM_TOT_SGMT_CNT [2]
                        ('0' * 2) + # CLM_SGMT_NUM     [2]
                        ('0' * 3) +	# CLM_TOT_LINE_CNT [3]
                        ('0' * 2) +	# CLM_SGMT_LINE_CNT [2]

	                    (' ' * 2) +  # H_FILLER_5 [2]
	                    IDR_CLM_FAC_TYPE_CD  +
	                    IDR_CLM_SRVC_CLSFCTN_TYPE_CD +
	                    IDR_CLM_BILL_FREQ_CD + 
	                    (' ' * 7) +  # H_FILLER_6 [7]
	                    IDR_GEO_BENE_SSA_CNTY_CD +
                        IDR_CLM_SUBMSN_DT + 
	                    (' ' * 16) +  # H_FILLER_7 [16]
	                    IDR_CLM_CNTRCT_NUM +
	                    IDR_CNTRCT_PBP_NUM + 
	                    IDR_CLM_CNTRCT_TYPE_CD + 

	                    IDR_CLM_CHRT_RVW_SW + 
                        IDR_CLM_CHRT_RVW_EFCTV_SW + 
                        IDR_CLM_EDPS_CHRT_RVW_SW + 
	                    (' ' * 4) +  # H_FILLER_8 [4]

                        IDR_GEO_ZIP9_CD +
                        IDR_BENE_SEX_CD + 
                        IDR_BENE_RACE_CD + 
                        IDR_CLM_PTNT_BIRTH_DT + 

                        IDR_CLM_CWF_BENE_MDCR_STUS_CD + 
                        IDR_CLM_LAST_NAME[:6] +
                        IDR_CLM_1ST_NAME[:1] +  
                        IDR_CLM_INTL_MDL_NAME[:1] +

	                    (' ' * 1) +  # H_FILLER_9 [1]
                        IDR_CLM_ICD_VRSN_CD + 
                        IDR_CLM_PRNCPAL_DGNS_CD  +
                        (' ' * 10) +  # H_FILLER_10 [10]

                        IDR_CLM_ENCTR_OTHR_PYR_PD_AMT[3:]  + #  PIC 9(12)V99S -> PIC 9(9)V99S
                        (' ' * 2) +   # H_FILLER_11 [2]
                        IDR_CLM_BLG_PRVDR_TAX_NUM  +
                        IDR_CLM_BLG_PRVDR_NPI_NUM  + 

                        (' ' * 6) +  # H_FILLER_12 [6]
                        IDR_CLM_ATNDG_PRVDR_NPI_NUM +
                        IDR_CLM_ATNDG_PRVDR_LAST_NAME[:6]  + 
                        IDR_CLM_ATNDG_PRVDR_1ST_NAME[:1]   + 
                        IDR_CLM_ATNDG_PRVDR_MDL_NAME[:1]   +

                        (' ' * 6) +  # H_FILLER_13 [6]
                        IDR_CLM_OPRTG_PRVDR_NPI_NUM  +  
                        IDR_CLM_OPRTG_PRVDR_LAST_NAME[:6] +
                        IDR_CLM_OPRTG_PRVDR_1ST_NAME[:1]  + 
                        IDR_CLM_OPRTG_PRVDR_MDL_NAME[:1]  + 

                        (' ' * 6) +  # H_FILLER_14 [6]
                        IDR_CLM_OTHR_PRVDR_NPI_NUM  + 
                        IDR_CLM_OTHR_PRVDR_LAST_NAME[:6] + 
                        IDR_CLM_OTHR_PRVDR_1ST_NAME[:1]  + 
                        IDR_CLM_OTHR_PRVDR_MDL_NAME[:1]  +  

                        (' ' * 36) +  # H_FILLER_15 [36]
	                    IDR_CLM_PTNT_CNTL_NUM[:20] +
    	                IDR_CLM_PTNT_MDCL_REC_NUM [:17] +
	                    (' ' * 20) +  # H_FILLER_16 [20]
	                    IDR_PTNT_STUS_CD +

                        IDR_ICD_VRSN_E_CD + 
                        IDR_DGNS_E_CD      + 
                        (' ' * 1) +  # H_FILLER_17 [1]
                        IDR_CLM_SBMT_CHRG_AMT[3:]   +  #  PIC 9(12)V99S -> PIC 9(9)V99S
                        (' ' * 8) +  # H_FILLER_18 [8]
                        IDR_CLM_BLG_PRVDR_ZIP9_CD +  #  IDR_CLM_SRVC_FAC_ZIP_CD 
                        (' ' * 10) +  # H_FILLER_19 [10]
                        (' ' * 6)  +  # H_FILLER_20 [6]
                        IDR_CLM_RNDRG_PRVDR_NPI_NUM  +
                        IDR_CLM_RNDRG_PRVDR_LAST_NAME [:6] +
                        IDR_CLM_RNDRG_PRVDR_1ST_NAME  [:1] +
                        IDR_CLM_RNDRG_PRVDR_MDL_NAME [:1] +
                        (' ' * 131) +  # H_FILLER_21 [131]

                        f"{iNOF_MCO_PRD_GRP:01}"       +
                        f"{iNOF_CLM_DGNS_D_GRP:02}"    + 
                        f"{iNOF_CLM_DGNS_E_GRP:02}"    +
                        f"{iNOF_CLM_RLT_COND_GRP:02}"  + 
                        f"{iNOF_CLM_RLT_OCRNC_GRP:02}" +
                        f"{iNOF_CLM_OCRNC_SPAN_CD_GRP:02}"  +
                        f"{iNOF_CLM_VAL_GRP:02}"            +
                        ('0' * 2) +  # REV_CNTR_CD_I_CNT [2]

                        (' ' * 4) +   # IDR_H_FILLER_23 [4]
                        (' ' * 20) +  # IDR_H_FILLER_24 [20]
                        IDR_CLM_DSCHRG_DT + 
                        IDR_CLM_ACTV_CARE_FROM_DT +     
                        (' ' * 12) +  #IDR_H_FILLER_25  [12]

                        IDR_CLM_RFRG_PRVDR_NPI_NUM  +
                        IDR_CLM_RFRG_PRVDR_LAST_NAME[:6] + 
                        IDR_CLM_RFRG_PRVDR_1ST_NAME[:1] + 
                        IDR_CLM_RFRG_PRVDR_MDL_NAME[:1] +
                        (' ' * 4) +  # IDR_H_FILLER_26 [2];
                        f"{iNOF_DGNS_R_CD_GRP:02}" +

                        IDR_BENE_SK  +
                        IDR_CLM_FINL_ACTN_IND  +
                        IDR_CLM_LTST_CLM_IND   + 
                        IDR_CLM_BLG_PRVDR_TXNMY_CD   + 
                        IDR_CLM_ATNDG_PRVDR_TXNMY_CD + 
                        IDR_CLM_EDPS_STUS_CD   +         
                        IDR_CLM_OBSLT_DT       +        
                        IDR_CLM_ERR_SGNTR_SK   +         
                        IDR_GEO_BENE_EFCTV_SK  +         
                        IDR_CLM_DT_SGNTR_EFCTV_SK  +      
                        IDR_CLM_TYPE_EFCTV_CD      +    
                        IDR_CLM_NUM_EFCTV_SK       +    
                        IDR_CLM_CNTRCT_AMT [3:]        +      #  PIC 9(12)V99S -> PIC 9(9)V99S
                        IDR_CLM_PTNT_LBLTY_AMT [3:]    +      #  PIC 9(12)V99S -> PIC 9(9)V99S
                        IDR_BENE_EQTBL_BIC_CD      +     

                        IDR_CLM_BPRVDR_ADR_LINE_1_TXT +
                        IDR_CLM_BPRVDR_ADR_LINE_2_TXT +
                        IDR_CLM_BPRVDR_ADR_LINE_3_TXT +
                        IDR_CLM_BPRVDR_CITY_NAME +
                        IDR_CLM_BPRVDR_USPS_STATE_CD +
                        IDR_BPRVDR_ADR_ZIP_CD +

                        IDR_CLM_SUBSCR_ADR_LINE_1_TXT +
                        IDR_CLM_SUBSCR_ADR_LINE_2_TXT +
                        IDR_CLM_SUBSCR_ADR_LINE_3_TXT +
                        IDR_CLM_SUBSCR_CITY_NAME +
                        IDR_CLM_SUBSCR_USPS_STATE_CD + 
                        IDR_SUBSCR_ADR_ZIP_CD
                        )


#    print("End function load_fixed_len_clm_info")	

    return sOutputRecFixed


def load_var_len_clm_info(inputRecord):

#    print("Start function load_var_len_clm_in")	

    sDgnsRGrp = load_DGNS_R_cd(inputRecord)
    sMCOPrdGrp = load_MCO_Contract_Num(inputRecord)
    sDgnsDGrp = load_Dgns_D_cd(inputRecord)
    sDgnsEGrp = load_Dgns_E_cd(inputRecord)
    sRltCondGrp = load_Rlt_Cond_cd(inputRecord)
    sRltOcrncGrp = load_Rlt_Ocrnc_cd(inputRecord)
    sOcrncSpanGrp = load_Ocrnc_Span_cd(inputRecord)
    sClmValGrp = load_VAL_Info(inputRecord)

    sVarOutputArea = sDgnsRGrp + sMCOPrdGrp + sDgnsDGrp + sDgnsEGrp + sRltCondGrp + sRltOcrncGrp + sOcrncSpanGrp + sClmValGrp
#    print(f"{len(sVarOutputArea)=}")

#    print("End function load_var_len_clm_in")	

    return sVarOutputArea


def load_DGNS_R_cd(inputRecord):

#    print("Start function load_DGNS_R_cd")	

    # Extract variables from input record
    IDR_CLM_PROD_VRSN_R_CD = inputRecord[1206:1207]
    lstIDR_CLM_DGNS_R_CD = []
    lstIDR_CLM_DGNS_R_CD.append(inputRecord[1207:1214])
    lstIDR_CLM_DGNS_R_CD.append(inputRecord[1214:1221])
    lstIDR_CLM_DGNS_R_CD.append(inputRecord[1221:1228])
	
    # init variables 
    sDgnsRGrp : str = ""

	# write elements that have data
    for IDR_CLM_DGNS_R_CD in lstIDR_CLM_DGNS_R_CD:
#        print (f"{IDR_CLM_DGNS_R_CD}")

        if ( IDR_CLM_DGNS_R_CD.strip() != '' and IDR_CLM_DGNS_R_CD.strip() != '~' ):
            sDgnsRGrp  += (' '  # H_FILLER_27
                           + IDR_CLM_PROD_VRSN_R_CD 
                           + IDR_CLM_DGNS_R_CD )
            			
            # increment counter of NOF array elements
            global iNOF_DGNS_R_CD_GRP # referring to global variable
            iNOF_DGNS_R_CD_GRP += 1
			
			# display length of variable area
#            print(f"{len(sDgnsRGrp)=}") 
    

#    print("End function load_DGNS_R_cd")	

    # return 
    return sDgnsRGrp


def	load_MCO_Contract_Num(inputRecord):

#    print("Start function load_MCO_Contract_Num")	

    # Extract variables from input record
    lstIDR_CLM_MCO_CNTRCT_NUM = []
    lstIDR_CLM_MCO_CNTRCT_NUM.append(inputRecord[1228:1233])
    lstIDR_CLM_MCO_CNTRCT_NUM.append(inputRecord[1233:1238])

    lstIDR_CLM_MCO_HLTH_PLAN_ID = []
    lstIDR_CLM_MCO_HLTH_PLAN_ID.append(inputRecord[1238:1252])
    lstIDR_CLM_MCO_HLTH_PLAN_ID.append(inputRecord[1252:1266])

    # init variables 
    sMCOPrdGrp : str = ""

	# write elements that have data
    for i in range(0,1):

        if (    lstIDR_CLM_MCO_CNTRCT_NUM[i].strip() != ''    and lstIDR_CLM_MCO_CNTRCT_NUM[i].strip()   != '~' 
            and lstIDR_CLM_MCO_HLTH_PLAN_ID[i].strip() != ' ' and lstIDR_CLM_MCO_HLTH_PLAN_ID[i].strip() != '~'): 
			
            sMCOPrdGrp += (' '  # H_FILLER_29
                           + lstIDR_CLM_MCO_CNTRCT_NUM[i]  
                           + (' ' * 17) +  #H_FILLER_30
                           + lstIDR_CLM_MCO_HLTH_PLAN_ID[i] )

            # increment counter of NOF array elements
            global iNOF_MCO_PRD_GRP # referring to global variable
            iNOF_MCO_PRD_GRP += 1 
			
			# display length of variable area
            print(f"{len(sMCOPrdGrp)=}") 

#    print("End function load_MCO_Contract_Num")	

    # return 
    return sMCOPrdGrp


def	load_Dgns_D_cd(inputRecord):

#    print("Start function load_Dgns_D_cd")	

    # Extract variables from input record    
    IDR_CLM_VRSN_D_CD = inputRecord[1266:1267]

    lstIDR_CLM_DGNS_D_CD = []
    lstIDR_CLM_DGNS_D_CD.append(inputRecord[1267:1274])
    lstIDR_CLM_DGNS_D_CD.append(inputRecord[1274:1281])
    lstIDR_CLM_DGNS_D_CD.append(inputRecord[1281:1288])
    lstIDR_CLM_DGNS_D_CD.append(inputRecord[1288:1295])
    lstIDR_CLM_DGNS_D_CD.append(inputRecord[1295:1302])
    lstIDR_CLM_DGNS_D_CD.append(inputRecord[1302:1309])
    lstIDR_CLM_DGNS_D_CD.append(inputRecord[1309:1316])
    lstIDR_CLM_DGNS_D_CD.append(inputRecord[1316:1323])
    lstIDR_CLM_DGNS_D_CD.append(inputRecord[1323:1330])
    lstIDR_CLM_DGNS_D_CD.append(inputRecord[1330:1337])
    lstIDR_CLM_DGNS_D_CD.append(inputRecord[1337:1344])
    lstIDR_CLM_DGNS_D_CD.append(inputRecord[1344:1351])
    lstIDR_CLM_DGNS_D_CD.append(inputRecord[1351:1358])
    lstIDR_CLM_DGNS_D_CD.append(inputRecord[1358:1365])
    lstIDR_CLM_DGNS_D_CD.append(inputRecord[1365:1372])
    lstIDR_CLM_DGNS_D_CD.append(inputRecord[1372:1379])
    lstIDR_CLM_DGNS_D_CD.append(inputRecord[1379:1386])
    lstIDR_CLM_DGNS_D_CD.append(inputRecord[1386:1393])
    lstIDR_CLM_DGNS_D_CD.append(inputRecord[1393:1400])
    lstIDR_CLM_DGNS_D_CD.append(inputRecord[1400:1407])
    lstIDR_CLM_DGNS_D_CD.append(inputRecord[1407:1414])
    lstIDR_CLM_DGNS_D_CD.append(inputRecord[1414:1421])
    lstIDR_CLM_DGNS_D_CD.append(inputRecord[1421:1428])
    lstIDR_CLM_DGNS_D_CD.append(inputRecord[1428:1435])
    lstIDR_CLM_DGNS_D_CD.append(inputRecord[1435:1442])

    # init variables
    sDgnsDGrp : str = ""

	# write elements that have data
    for IDR_CLM_DGNS_D_CD in lstIDR_CLM_DGNS_D_CD:
#        print (f"{IDR_CLM_DGNS_D_CD}")

        if ( IDR_CLM_DGNS_D_CD.strip() != '' and IDR_CLM_DGNS_D_CD.strip() != '~' ):
            sDgnsDGrp += (' ' # H_FILLER_32
                          + IDR_CLM_VRSN_D_CD
                          + IDR_CLM_DGNS_D_CD )
            			
            # increment counter of NOF array elements
            global iNOF_CLM_DGNS_D_GRP # referring to global variable
            iNOF_CLM_DGNS_D_GRP += 1
			
			# display length of variable area
#            print(f"{len(sDgnsDGrp)=}") 

#    print("End function load_Dgns_D_cd")	

    return sDgnsDGrp


def	load_Dgns_E_cd(inputRecord):

#    print("Start function load_Dgns_E_cd")	

    # Extract variables from input record
    IDR_CLM_VRSN_E_CD = inputRecord[1442:1443]

    lstIDR_CLM_DGNS_E_CD = []
    lstIDR_CLM_DGNS_E_CD.append(inputRecord[1443:1450])
    lstIDR_CLM_DGNS_E_CD.append(inputRecord[1450:1457])
    lstIDR_CLM_DGNS_E_CD.append(inputRecord[1457:1464])
    lstIDR_CLM_DGNS_E_CD.append(inputRecord[1464:1471])
    lstIDR_CLM_DGNS_E_CD.append(inputRecord[1471:1478])
    lstIDR_CLM_DGNS_E_CD.append(inputRecord[1478:1485])
    lstIDR_CLM_DGNS_E_CD.append(inputRecord[1485:1492])
    lstIDR_CLM_DGNS_E_CD.append(inputRecord[1492:1499])
    lstIDR_CLM_DGNS_E_CD.append(inputRecord[1499:1506])
    lstIDR_CLM_DGNS_E_CD.append(inputRecord[1506:1513])
    lstIDR_CLM_DGNS_E_CD.append(inputRecord[1513:1520])
    lstIDR_CLM_DGNS_E_CD.append(inputRecord[1520:1527])

    # init variables
    sDgnsEGrp : str = ""

	# write elements that have data
    for IDR_CLM_DGNS_E_CD in lstIDR_CLM_DGNS_E_CD:
#        print (f"{IDR_CLM_DGNS_E_CD}")

        if ( IDR_CLM_DGNS_E_CD.strip() != '' and IDR_CLM_DGNS_E_CD.strip() != '~' ):
            sDgnsEGrp += (' ' # H_FILLER_33
                          + IDR_CLM_VRSN_E_CD
                          + IDR_CLM_DGNS_E_CD ) 
            			
            # increment counter of NOF array elements
            global iNOF_CLM_DGNS_E_GRP # referring to global variable            
            iNOF_CLM_DGNS_E_GRP += 1
			
			# display length of variable area
#            print(f"{len(sDgnsEGrp)=}") 

#    print("End function load_Dgns_E_cd")	

    return sDgnsEGrp


def	load_Rlt_Cond_cd(inputRecord):

#    print("Start function load_Rlt_Cond_cd")	

    # Extract variables from input record
    lstIDR_CLM_RLT_COND_CD = []
    lstIDR_CLM_RLT_COND_CD.append(inputRecord[1527:1529])
    lstIDR_CLM_RLT_COND_CD.append(inputRecord[1529:1531])
    lstIDR_CLM_RLT_COND_CD.append(inputRecord[1531:1533])
    lstIDR_CLM_RLT_COND_CD.append(inputRecord[1533:1535])    
    lstIDR_CLM_RLT_COND_CD.append(inputRecord[1535:1537])    
    lstIDR_CLM_RLT_COND_CD.append(inputRecord[1537:1539])    
    lstIDR_CLM_RLT_COND_CD.append(inputRecord[1539:1541])    
    lstIDR_CLM_RLT_COND_CD.append(inputRecord[1541:1543])    
    lstIDR_CLM_RLT_COND_CD.append(inputRecord[1543:1545])    
    lstIDR_CLM_RLT_COND_CD.append(inputRecord[1545:1547])    

    lstIDR_CLM_RLT_COND_CD.append(inputRecord[1547:1549])    
    lstIDR_CLM_RLT_COND_CD.append(inputRecord[1549:1551])    
    lstIDR_CLM_RLT_COND_CD.append(inputRecord[1551:1553])    
    lstIDR_CLM_RLT_COND_CD.append(inputRecord[1553:1555])    
    lstIDR_CLM_RLT_COND_CD.append(inputRecord[1555:1557])    
    lstIDR_CLM_RLT_COND_CD.append(inputRecord[1557:1559])    
    lstIDR_CLM_RLT_COND_CD.append(inputRecord[1559:1561])    
    lstIDR_CLM_RLT_COND_CD.append(inputRecord[1561:1563])    
    lstIDR_CLM_RLT_COND_CD.append(inputRecord[1563:1565])    
    lstIDR_CLM_RLT_COND_CD.append(inputRecord[1565:1567])    

    lstIDR_CLM_RLT_COND_CD.append(inputRecord[1567:1569])    
    lstIDR_CLM_RLT_COND_CD.append(inputRecord[1569:1571])    
    lstIDR_CLM_RLT_COND_CD.append(inputRecord[1571:1573])    
    lstIDR_CLM_RLT_COND_CD.append(inputRecord[1573:1575])    
    lstIDR_CLM_RLT_COND_CD.append(inputRecord[1575:1577])    
    lstIDR_CLM_RLT_COND_CD.append(inputRecord[1577:1579])    
    lstIDR_CLM_RLT_COND_CD.append(inputRecord[1579:1581])    
    lstIDR_CLM_RLT_COND_CD.append(inputRecord[1581:1583])    
    lstIDR_CLM_RLT_COND_CD.append(inputRecord[1583:1585])    
    lstIDR_CLM_RLT_COND_CD.append(inputRecord[1585:1587])    

   # init variables
    sRltCondGrp : str = ""

	# write elements that have data
    for IDR_CLM_RLT_COND_CD in lstIDR_CLM_RLT_COND_CD:
#        print (f"{IDR_CLM_RLT_COND_CD}")

        if ( IDR_CLM_RLT_COND_CD.strip() != '' and IDR_CLM_RLT_COND_CD.strip() != '~' ):
            sRltCondGrp += (' ' # H_FILLER_33
                            + IDR_CLM_RLT_COND_CD )

            # increment counter of NOF array elements
            global iNOF_CLM_RLT_COND_GRP  # referring to global variable               
            iNOF_CLM_RLT_COND_GRP += 1
			
			# display length of variable area
#            print(f"{len(sRltCondGrp)=}") 

#    print("End function load_Rlt_Cond_cd")	
  
    return sRltCondGrp


def	load_Rlt_Ocrnc_cd(inputRecord):

#    print("Start function load_Rlt_Ocrnc_cd")	

    # Extract variables from input record
    lstIDR_CLM_RLT_OCRNC_CD = []
    lstIDR_CLM_RLT_OCRNC_CD.append(inputRecord[1587:1589])
    lstIDR_CLM_RLT_OCRNC_CD.append(inputRecord[1589:1591])
    lstIDR_CLM_RLT_OCRNC_CD.append(inputRecord[1591:1593])
    lstIDR_CLM_RLT_OCRNC_CD.append(inputRecord[1593:1595])    
    lstIDR_CLM_RLT_OCRNC_CD.append(inputRecord[1595:1597])    
    lstIDR_CLM_RLT_OCRNC_CD.append(inputRecord[1597:1599])    
    lstIDR_CLM_RLT_OCRNC_CD.append(inputRecord[1599:1601])    
    lstIDR_CLM_RLT_OCRNC_CD.append(inputRecord[1601:1603])    
    lstIDR_CLM_RLT_OCRNC_CD.append(inputRecord[1603:1605])    
    lstIDR_CLM_RLT_OCRNC_CD.append(inputRecord[1605:1607])    

    lstIDR_CLM_RLT_OCRNC_CD.append(inputRecord[1607:1609])    
    lstIDR_CLM_RLT_OCRNC_CD.append(inputRecord[1609:1611])    
    lstIDR_CLM_RLT_OCRNC_CD.append(inputRecord[1611:1613])    
    lstIDR_CLM_RLT_OCRNC_CD.append(inputRecord[1613:1615])    
    lstIDR_CLM_RLT_OCRNC_CD.append(inputRecord[1615:1617])    
    lstIDR_CLM_RLT_OCRNC_CD.append(inputRecord[1617:1619])    
    lstIDR_CLM_RLT_OCRNC_CD.append(inputRecord[1619:1621])    
    lstIDR_CLM_RLT_OCRNC_CD.append(inputRecord[1621:1623])    
    lstIDR_CLM_RLT_OCRNC_CD.append(inputRecord[1623:1625])    
    lstIDR_CLM_RLT_OCRNC_CD.append(inputRecord[1625:1627])    

    lstIDR_CLM_RLT_OCRNC_CD.append(inputRecord[1627:1629])    
    lstIDR_CLM_RLT_OCRNC_CD.append(inputRecord[1629:1631])    
    lstIDR_CLM_RLT_OCRNC_CD.append(inputRecord[1631:1633])    
    lstIDR_CLM_RLT_OCRNC_CD.append(inputRecord[1633:1635])    
    lstIDR_CLM_RLT_OCRNC_CD.append(inputRecord[1635:1637])    
    lstIDR_CLM_RLT_OCRNC_CD.append(inputRecord[1637:1639])    
    lstIDR_CLM_RLT_OCRNC_CD.append(inputRecord[1639:1641])    
    lstIDR_CLM_RLT_OCRNC_CD.append(inputRecord[1641:1643])    
    lstIDR_CLM_RLT_OCRNC_CD.append(inputRecord[1643:1645])    
    lstIDR_CLM_RLT_OCRNC_CD.append(inputRecord[1645:1647])    

    lstIDR_CLM_RLT_OCRNC_DT = []
    lstIDR_CLM_RLT_OCRNC_DT.append(inputRecord[1647:1655])
    lstIDR_CLM_RLT_OCRNC_DT.append(inputRecord[1655:1663])
    lstIDR_CLM_RLT_OCRNC_DT.append(inputRecord[1663:1671])
    lstIDR_CLM_RLT_OCRNC_DT.append(inputRecord[1671:1679])    
    lstIDR_CLM_RLT_OCRNC_DT.append(inputRecord[1679:1687])    
    lstIDR_CLM_RLT_OCRNC_DT.append(inputRecord[1687:1695])    
    lstIDR_CLM_RLT_OCRNC_DT.append(inputRecord[1695:1703])    
    lstIDR_CLM_RLT_OCRNC_DT.append(inputRecord[1703:1711])    
    lstIDR_CLM_RLT_OCRNC_DT.append(inputRecord[1711:1719])    
    lstIDR_CLM_RLT_OCRNC_DT.append(inputRecord[1719:1727])    

    lstIDR_CLM_RLT_OCRNC_DT.append(inputRecord[1727:1735])    
    lstIDR_CLM_RLT_OCRNC_DT.append(inputRecord[1735:1743])    
    lstIDR_CLM_RLT_OCRNC_DT.append(inputRecord[1743:1751])    
    lstIDR_CLM_RLT_OCRNC_DT.append(inputRecord[1751:1759])    
    lstIDR_CLM_RLT_OCRNC_DT.append(inputRecord[1759:1767])    
    lstIDR_CLM_RLT_OCRNC_DT.append(inputRecord[1767:1775])    
    lstIDR_CLM_RLT_OCRNC_DT.append(inputRecord[1775:1783])    
    lstIDR_CLM_RLT_OCRNC_DT.append(inputRecord[1783:1791])    
    lstIDR_CLM_RLT_OCRNC_DT.append(inputRecord[1791:1799])    
    lstIDR_CLM_RLT_OCRNC_DT.append(inputRecord[1799:1807])    

    lstIDR_CLM_RLT_OCRNC_DT.append(inputRecord[1807:1815])    
    lstIDR_CLM_RLT_OCRNC_DT.append(inputRecord[1815:1823])    
    lstIDR_CLM_RLT_OCRNC_DT.append(inputRecord[1823:1831])    
    lstIDR_CLM_RLT_OCRNC_DT.append(inputRecord[1831:1839])    
    lstIDR_CLM_RLT_OCRNC_DT.append(inputRecord[1839:1847])    
    lstIDR_CLM_RLT_OCRNC_DT.append(inputRecord[1847:1855])    
    lstIDR_CLM_RLT_OCRNC_DT.append(inputRecord[1855:1863])    
    lstIDR_CLM_RLT_OCRNC_DT.append(inputRecord[1863:1871])    
    lstIDR_CLM_RLT_OCRNC_DT.append(inputRecord[1871:1879])    
    lstIDR_CLM_RLT_OCRNC_DT.append(inputRecord[1879:1887])   

   # init variables
    sRltOcrncGrp : str = ""

	# write elements that have data
    for i in range(0,29):
#        print (f"{lstIDR_CLM_RLT_OCRNC_CD[i]}")

        if ( lstIDR_CLM_RLT_OCRNC_CD[i].strip() != '' and lstIDR_CLM_RLT_OCRNC_CD[i].strip() != '~' ):
            sRltOcrncGrp += (' ' # H_FILLER_35
                             + lstIDR_CLM_RLT_OCRNC_CD[i]
                             + lstIDR_CLM_RLT_OCRNC_DT[i] )

            # increment counter of NOF array elements
            global iNOF_CLM_RLT_OCRNC_GRP  # referring to global variable              
            iNOF_CLM_RLT_OCRNC_GRP += 1
			
			# display length of variable area
#            print(f"{len(sRltOcrncGrp)=}") 

#    print("End function load_Rlt_Ocrnc_cd")	

    return sRltOcrncGrp


def	load_Ocrnc_Span_cd(inputRecord):

#    print("Start function load_Ocrnc_Span_cd")	

    # Extract variables from input record
    lstIDR_CLM_OCRNC_SPAN_CD = []
    lstIDR_CLM_OCRNC_SPAN_CD.append(inputRecord[1887:1889])
    lstIDR_CLM_OCRNC_SPAN_CD.append(inputRecord[1889:1891])
    lstIDR_CLM_OCRNC_SPAN_CD.append(inputRecord[1891:1893])
    lstIDR_CLM_OCRNC_SPAN_CD.append(inputRecord[1893:1895])    
    lstIDR_CLM_OCRNC_SPAN_CD.append(inputRecord[1895:1897])    
    lstIDR_CLM_OCRNC_SPAN_CD.append(inputRecord[1897:1899])    
    lstIDR_CLM_OCRNC_SPAN_CD.append(inputRecord[1899:1901])    
    lstIDR_CLM_OCRNC_SPAN_CD.append(inputRecord[1901:1903])    
    lstIDR_CLM_OCRNC_SPAN_CD.append(inputRecord[1903:1905])    
    lstIDR_CLM_OCRNC_SPAN_CD.append(inputRecord[1905:1907])  

    lstIDR_CLM_OCRNC_SPAN_FROM_DT = []
    lstIDR_CLM_OCRNC_SPAN_FROM_DT.append(inputRecord[1907:1915])
    lstIDR_CLM_OCRNC_SPAN_FROM_DT.append(inputRecord[1915:1923])
    lstIDR_CLM_OCRNC_SPAN_FROM_DT.append(inputRecord[1923:1931])
    lstIDR_CLM_OCRNC_SPAN_FROM_DT.append(inputRecord[1931:1939])    
    lstIDR_CLM_OCRNC_SPAN_FROM_DT.append(inputRecord[1939:1947])    
    lstIDR_CLM_OCRNC_SPAN_FROM_DT.append(inputRecord[1947:1955])    
    lstIDR_CLM_OCRNC_SPAN_FROM_DT.append(inputRecord[1955:1963])    
    lstIDR_CLM_OCRNC_SPAN_FROM_DT.append(inputRecord[1963:1971])    
    lstIDR_CLM_OCRNC_SPAN_FROM_DT.append(inputRecord[1971:1979])    
    lstIDR_CLM_OCRNC_SPAN_FROM_DT.append(inputRecord[1979:1987])  

    lstIDR_CLM_OCRNC_SPAN_THRU_DT = []
    lstIDR_CLM_OCRNC_SPAN_THRU_DT.append(inputRecord[1987:1995])
    lstIDR_CLM_OCRNC_SPAN_THRU_DT.append(inputRecord[1995:2003])
    lstIDR_CLM_OCRNC_SPAN_THRU_DT.append(inputRecord[2003:2011])
    lstIDR_CLM_OCRNC_SPAN_THRU_DT.append(inputRecord[2011:2019])    
    lstIDR_CLM_OCRNC_SPAN_THRU_DT.append(inputRecord[2019:2027])    
    lstIDR_CLM_OCRNC_SPAN_THRU_DT.append(inputRecord[2027:2035])    
    lstIDR_CLM_OCRNC_SPAN_THRU_DT.append(inputRecord[2035:2043])    
    lstIDR_CLM_OCRNC_SPAN_THRU_DT.append(inputRecord[2043:2051])    
    lstIDR_CLM_OCRNC_SPAN_THRU_DT.append(inputRecord[2051:2059])    
    lstIDR_CLM_OCRNC_SPAN_THRU_DT.append(inputRecord[2059:2067])  

   # init variables
    sOcrncSpanGrp : str = ""

	# write elements that have data
    for i in range(0,9):
#        print (f"{lstIDR_CLM_OCRNC_SPAN_CD[i]}")

        if ( lstIDR_CLM_OCRNC_SPAN_CD[i].strip() != '' and lstIDR_CLM_OCRNC_SPAN_CD[i].strip() != '~' ):
            sOcrncSpanGrp += (' ' # H_FILLER_36
                               + lstIDR_CLM_OCRNC_SPAN_CD[i]
                               + lstIDR_CLM_OCRNC_SPAN_FROM_DT[i]
                               + lstIDR_CLM_OCRNC_SPAN_THRU_DT[i] )

            # increment counter of NOF array elements
            global iNOF_CLM_OCRNC_SPAN_CD_GRP  # referring to global variable              
            iNOF_CLM_OCRNC_SPAN_CD_GRP += 1
			
			# display length of variable area
#            print(f"{len(sOcrncSpanGrp)=}")

#    print("End function load_Ocrnc_Span_cd")	

    return sOcrncSpanGrp


def load_VAL_Info (inputRecord):

#    print("Start function load_VAL_Info")

    # Extract variables from input record
    lstIDR_VAL_CD = []
    lstIDR_VAL_CD.append(inputRecord[2952:2954])
    lstIDR_VAL_CD.append(inputRecord[2954:2956])
    lstIDR_VAL_CD.append(inputRecord[2956:2958])
    lstIDR_VAL_CD.append(inputRecord[2958:2960])
    lstIDR_VAL_CD.append(inputRecord[2960:2962])
    lstIDR_VAL_CD.append(inputRecord[2962:2964])
    lstIDR_VAL_CD.append(inputRecord[2964:2966])
    lstIDR_VAL_CD.append(inputRecord[2966:2968])
    lstIDR_VAL_CD.append(inputRecord[2968:2970])
    lstIDR_VAL_CD.append(inputRecord[2970:2972])

    lstIDR_VAL_CD.append(inputRecord[2972:2974])
    lstIDR_VAL_CD.append(inputRecord[2974:2976])
    lstIDR_VAL_CD.append(inputRecord[2976:2978])
    lstIDR_VAL_CD.append(inputRecord[2978:2980])
    lstIDR_VAL_CD.append(inputRecord[2980:2982])
    lstIDR_VAL_CD.append(inputRecord[2982:2984])
    lstIDR_VAL_CD.append(inputRecord[2984:2986])
    lstIDR_VAL_CD.append(inputRecord[2986:2988])
    lstIDR_VAL_CD.append(inputRecord[2988:2990])
    lstIDR_VAL_CD.append(inputRecord[2990:2992])

    lstIDR_VAL_CD.append(inputRecord[2992:2994])
    lstIDR_VAL_CD.append(inputRecord[2994:2996])
    lstIDR_VAL_CD.append(inputRecord[2996:2998])
    lstIDR_VAL_CD.append(inputRecord[2998:3000])
    lstIDR_VAL_CD.append(inputRecord[3000:3002])

    lstIDR_VAL_AMT = []
    lstIDR_VAL_AMT.append(inputRecord[3002:3017])
    lstIDR_VAL_AMT.append(inputRecord[3017:3032])
    lstIDR_VAL_AMT.append(inputRecord[3032:3047])
    lstIDR_VAL_AMT.append(inputRecord[3047:3062])
    lstIDR_VAL_AMT.append(inputRecord[3062:3077])
    lstIDR_VAL_AMT.append(inputRecord[3077:3092])
    lstIDR_VAL_AMT.append(inputRecord[3092:3107])
    lstIDR_VAL_AMT.append(inputRecord[3107:3122])
    lstIDR_VAL_AMT.append(inputRecord[3122:3137])
    lstIDR_VAL_AMT.append(inputRecord[3137:3152])

    lstIDR_VAL_AMT.append(inputRecord[3152:3167])
    lstIDR_VAL_AMT.append(inputRecord[3167:3182])
    lstIDR_VAL_AMT.append(inputRecord[3182:3197])
    lstIDR_VAL_AMT.append(inputRecord[3197:3212])
    lstIDR_VAL_AMT.append(inputRecord[3212:3227])
    lstIDR_VAL_AMT.append(inputRecord[3227:3242])
    lstIDR_VAL_AMT.append(inputRecord[3242:3257])
    lstIDR_VAL_AMT.append(inputRecord[3257:3272])
    lstIDR_VAL_AMT.append(inputRecord[3272:3287])
    lstIDR_VAL_AMT.append(inputRecord[3287:3302])

    lstIDR_VAL_AMT.append(inputRecord[3302:3317])
    lstIDR_VAL_AMT.append(inputRecord[3317:3332])
    lstIDR_VAL_AMT.append(inputRecord[3332:3347])
    lstIDR_VAL_AMT.append(inputRecord[3347:3362])
    lstIDR_VAL_AMT.append(inputRecord[3362:3377])

   # init variables
    sClmValGrp : str = ""

	# write elements that have data
    for i in range(0,24):
#        print (f"{lstIDR_VAL_CD[i]}")

        if ( lstIDR_VAL_CD[i].strip() != '' and lstIDR_VAL_CD[i].strip() != '~' ):
            sClmValGrp += (  lstIDR_VAL_CD[i]
                           + lstIDR_VAL_AMT[i] )


            # increment counter of NOF array elements
            global iNOF_CLM_VAL_GRP  # referring to global variable             
            iNOF_CLM_VAL_GRP += 1
			
			# display length of variable area
#            print(f"{len(sClmValGrp)=}")

#    print("End function load_VAL_Info")

    return sClmValGrp


def  load_RC_grp(inputRecord):

#    print("Start function load_RC_grp")	
 
    # Extract variables from input record	
    IDR_CLM_LINE_NUM = inputRecord[33:43]
    IDR_LINE_REV_CTR_CD = inputRecord[2067:2077]
    IDR_LINE_FROM_DT = inputRecord[2077:2085] 
    IDR_LINE_HCPCS_CD = inputRecord[2085:2090]

    lstIDR_LINE_HCPCS_MDFR_CD = []
    lstIDR_LINE_HCPCS_MDFR_CD.append(inputRecord[2090:2092])
    lstIDR_LINE_HCPCS_MDFR_CD.append(inputRecord[2092:2094])
    lstIDR_LINE_HCPCS_MDFR_CD.append(inputRecord[2094:2096])
    lstIDR_LINE_HCPCS_MDFR_CD.append(inputRecord[2096:2098])
    lstIDR_LINE_HCPCS_MDFR_CD.append(inputRecord[2098:2100])
    
    IDR_LINE_NDC_CD = inputRecord[2100:2111]
    IDR_LINE_NDC_QTY_QLFYR_CD = inputRecord[2111:2113] 
    IDR_LINE_NDC_QTY = inputRecord[2113:2131]            # PIC 9(13)V9(4)S
    IDR_CLM_LINE_SRVC_UNIT_QTY = inputRecord[2131:2149]  # PIC 9(13)V9(4)S
    IDR_LINE_OTHR_PYR_PD_AMT = inputRecord[2149:2164]    # PIC 9(12)V9(2)S
    IDR_LINE_SBMT_CHRG_AMT = inputRecord[2164:2179]      # PIC 9(12)V9(2)S
    IDR_LINE_RNDRG_PRVDR_NPI_NUM = inputRecord[2179:2189]
    IDR_LINE_RNDRG_PRVDR_NAME = inputRecord[2189:2309]
    IDR_LINE_RNDRG_PRVDR_SPCLTY_CD = inputRecord[2309:2311]
    IDR_LINE_THRU_DT = inputRecord[2311:2319] 

    IDR_CLM_LINE_CNTRCT_TYPE_CD  = inputRecord[2324:2326] 
    LINE_FINL_ACTN_IND  = inputRecord[2346:2347] 
    LINE_LTST_CLM_IND  = inputRecord[2348:2349] 
    LINE_ENCTR_STUS_CD  = inputRecord[2349:2369] 

 
    global sClmRevCntrGrp  # Tell python we are using the global variable
 
	# write elements that have data
    sClmRevCntrGrp += (' '   # H_FILLER_38
                        + IDR_LINE_REV_CTR_CD              
                        + IDR_LINE_FROM_DT            
                        + ( ' ' * 15)  #char H_FILLER_39  [15];                
                        + IDR_CLM_LINE_NUM          
                        + IDR_LINE_HCPCS_CD )

    for IDR_LINE_HCPCS_MDFR_CD in lstIDR_LINE_HCPCS_MDFR_CD:
        sClmRevCntrGrp += IDR_LINE_HCPCS_MDFR_CD

    sClmRevCntrGrp += (   IDR_CLM_LINE_CNTRCT_TYPE_CD
                        + ( ' ' * 5)  #H_FILLER_40 
                        + IDR_LINE_NDC_CD
                        + IDR_LINE_NDC_QTY_QLFYR_CD  
                        + IDR_LINE_NDC_QTY  # S9(7)V9(3) COMP-3. (11)           
                        + IDR_CLM_LINE_SRVC_UNIT_QTY  # s9(07) COMP-3. (8)       
                        + ( ' ' * 42) # H_FILLER_41 
	
                        + IDR_LINE_OTHR_PYR_PD_AMT  # S9(9)V99 COMP-3. (12)
                        + ( ' ' * 18) # H_FILLER_42   
                        + IDR_LINE_SBMT_CHRG_AMT  # S9(9)V99 COMP-3.(12)
                        + ( ' ' * 13) # H_FILLER_43 
                        + IDR_LINE_RNDRG_PRVDR_NPI_NUM
                        + IDR_LINE_RNDRG_PRVDR_NAME[:6]   
                        + ( ' ' * 2) # H_FILLER_43A     
                        + IDR_LINE_RNDRG_PRVDR_SPCLTY_CD
                        + LINE_FINL_ACTN_IND         
                        + LINE_LTST_CLM_IND           
                        + LINE_ENCTR_STUS_CD        
                        + ( ' ' * 358) # H_FILLER_44
                        + IDR_LINE_THRU_DT )   


	# count NOF claim lines written
    global iNOFClaimLinesWritten   # Tell python: referring to global variable
    global iClmTotLineCnt 
    global iClmSgmtLineCnt 

    iNOFClaimLinesWritten += 1
    iClmTotLineCnt += 1 
    iClmSgmtLineCnt += 1

#    print("End function load_RC_grp")

    return sClmRevCntrGrp


def buildNWriteOutputRec(ofSAFENC, sEndRecCd):

#    print("Start function buildNWriteOutputRec")	

    # Convert integet counts to display numbers
    sNOF_CLM_REV_CNTR_GRP = f"{iNOF_CLM_REV_CNTR_GRP:02}"

    sClmTotSgmtCnt =  f"{iClmTotSgmtCnt:02}"   
    sClmSgmtNum =  f"{iClmSgmtNum:02}"   
    sClmTotLineCnt =  f"{iClmTotLineCnt:03}"         
    sClmSgmtLineCnt =  f"{iClmSgmtLineCnt:02}"         

    global sOutputRecFixed   # let python konw we are referring to global var
    global sVarOutputArea    # let python konw we are referring to global var
    global sClmRevCntrGrp    # let python konw we are referring to global var

    # Build record without counts to get record length
    sFullOutputRec =  ( sOutputRecFixed +
                        sVarOutputArea + 
                        sClmRevCntrGrp +
                        sEndRecCd )

    iREC_LENGTH_CNT = len(sFullOutputRec) 
    sREC_LENGTH_CNT = f"{iREC_LENGTH_CNT:05}"                   

    # Add counts to sOutputRecfixed string
    sFullOutputRec = (  sREC_LENGTH_CNT +
                        sOutputRecFixed [5:130] +
                        sClmTotSgmtCnt +  
                        sClmSgmtNum +    
                        sClmTotLineCnt +          
                        sClmSgmtLineCnt +  
                        sOutputRecFixed [139:660] +  
    
                        sNOF_CLM_REV_CNTR_GRP + 

                        sOutputRecFixed [662:] +  
                        sVarOutputArea + 
                        sClmRevCntrGrp +
                        sEndRecCd + '\n')  


    #write ofSAFENC
    ofSAFENC.write(sFullOutputRec)

	# count NOF records written
    global iNOFRecsWritten  # Tell python: referring to global variable
    iNOFRecsWritten += 1

#    print("End function buildNWriteOutputRec")	


def main():

    ###############################################
    # global variables
    ###############################################
    sPrevClmKey : str = "" 
    sCurClmKey : str = ""
    sREC_KEY : str = ""

    global sOutputRecFixed # Tell python we are using the global variable
    sOutputRecFixed = ""

    global sVarOutputArea  # Tell python we are using the global variable
    sVarOutputArea = ""

    global sClmRevCntrGrp  # Tell python we are using the global variable

    # initialize NOF RC array structs counter
    global iNOF_CLM_REV_CNTR_GRP  # Tell python: referring to global variable

    global iNOFRecsRead 
    global iNOFRecsRead 
    global iNOFRecsWritten 
    global iNOFClaimsWritten     
    global iNOFClaimLinesWritten 

    global iClmTotSgmtCnt 
    global iClmSgmtNum  
    global iClmTotLineCnt 
    global iClmSgmtLineCnt 

    ###############################################
    # variables
    ###############################################
    bReadingFirstRec = True

    ###############################################
    # get parameter input/output filenames
    ###############################################
    getParms()

    ###############################################
    # process input file
    ###############################################
    ofSAFENC = open(outDirNFilename,"w",encoding="utf-8") 
    
    try:

        with open(InDirNFilename,"r",encoding="utf-8") as ifSAFENC:

            for inputRecord in ifSAFENC: 

                # count NOF records read
                iNOFRecsRead += 1
            
                # check for claim break; copy claim key sans line-num
                if not bReadingFirstRec:
                    sCurClmKey = inputRecord[:33]

                    #print("Not First time-thru")
                    #print(f"sPrevClmKey: {sPrevClmKey}")	
                    #print(f"sCurClmKey: {sCurClmKey}")
                    
                    # if curKey = prevKey
                    if (sCurClmKey == sPrevClmKey): 
                        #print(f"process current key: {sCurClmKey}")
                        sREC_KEY = sCurClmKey
                    
                    else:
                        #print("Key change: write last output line record")
                    
                        # write out complete claim sgmt record
                        buildNWriteOutputRec(ofSAFENC,"EOC")
                                            
                        # make previous key = current key
                        #print("\nLoad sPrevClmKey with new claim key");	
                        sPrevClmKey = sCurClmKey
                        
                        # initialize counters
                        iClmTotSgmtCnt = 1
                        iClmSgmtNum  = 1


                        # initialize NOF RC array structs counter
                        iNOF_CLM_REV_CNTR_GRP = 0
                        iClmTotLineCnt = 0 
                        iClmSgmtLineCnt = 0

                        # init RevCtrGrp string for next array of elements
                        sClmRevCntrGrp = ""

                        # load variable claim info
                        sVarOutputArea = load_var_len_clm_info(inputRecord)

                        # load fixed length claim info
                        sOutputRecFixed = load_fixed_len_clm_info(inputRecord)


        
                # first-time thru logic
                else:  
                    bReadingFirstRec = False

                    #print("First time-thru")

                    # load cur and prev key to be same value
                    sREC_KEY = inputRecord[:33]
                    sCurClmKey = sREC_KEY
                    sPrevClmKey = sREC_KEY
                    #print(f"sCurClmKey: {sCurClmKey} ")

                    # initialize counters
                    iClmTotSgmtCnt = 1
                    iClmSgmtNum  = 1

                    # initialize NOF RC array structs counter
                    iNOF_CLM_REV_CNTR_GRP = 0
                    iClmTotLineCnt = 0 
                    iClmSgmtLineCnt = 0

                    # load variable claim info
                    sVarOutputArea = load_var_len_clm_info(inputRecord)		

                    # load fixed length claim info
                    sOutputRecFixed = load_fixed_len_clm_info(inputRecord)
	
                

                # process claim line info
#                print("Process clm line num record");	
            
                # increment NOF RC GRP elements for this segment
                iNOF_CLM_REV_CNTR_GRP += 1

                # If array is full; write  
                if (iNOF_CLM_REV_CNTR_GRP > RC_GRP_MAX_ARRAY_ELEMENTS): 
                    buildNWriteOutputRec(ofSAFENC,"EOR")

#                    print("Create new RC array of 25 elements")

                    # creating new segment	
                    iClmTotSgmtCnt += 1
                    iClmSgmtNum += 1

                    # set next set of 25 ctr back to one.
                    iNOF_CLM_REV_CNTR_GRP = 1
                    iClmSgmtLineCnt = 0
                    
                    # init RevCtrGrp string for next array of elements
                    sClmRevCntrGrp = ""

                # load next RC group
                load_RC_grp(inputRecord)

        # close output file 
        ofSAFENC.close()


    except Exception as ex:
        print(f"Input file error: {ex}")
        sys.exit(12)


    # write totals to log file
    print("")
    print(f"IDR Extract Records read: {iNOFRecsRead}")            
    print(f"      NOF Claims written: {iNOFClaimsWritten}")      
    print(f" NOF Claim Lines written: {iNOFClaimLinesWritten}")    
    print(f"  SAFENC records written: {iNOFRecsWritten}")  




if __name__ == "__main__":
    
    main()