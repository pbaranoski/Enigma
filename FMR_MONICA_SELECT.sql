
		SELECT                                                                   
		LPAD(TO_CHAR(GEO_RGN_CD,'FM00'),2) AS REGION,          
		RPAD(CLM_CNTRCTR_NUM,5,' ')  AS CARRIER_NUMBER,                            
		RPAD(CLM_RNDRG_FED_PRVDR_SPCLTY_CD,2,' ')                                  
		AS PROVIDER_SPECIALTY,                                                         
		RPAD(COALESCE(HCPCS_CD,' '),5,' ') AS PROCEDURE_CODE,                         
		RPAD(COALESCE(HCPCS_MDFR_CD, ' '),2,' ') AS MODIFIER,                           
		RPAD(HCPCS_FMR_CLSFCTN_CD,1,' ') AS DATA_CLASS,                           
		RPAD('138',3) AS FILLER,                                                     
		TO_CHAR(COALESCE(CARR_RANK,0),'FM00000')  AS CARRIER_RANK,                            
		TO_CHAR(COALESCE(NATL_RANK,0),'FM00000')  AS NATIONAL_RANK,                           
		TO_CHAR(COALESCE(CARR_ENRLMT,0),'FM00000000000')                           
		AS CARRIER_ENROLLMENT,                                                         
		TO_CHAR(COALESCE(NATL_ENRLMT, 0),'FM00000000000')                            
		AS NATIONAL_ENROLLMENT,                                                        
		TO_CHAR(COALESCE(CARR_ALOWD_SRVCS,0),'FM000000000')                                     
		AS CARRIER_ALLOWED_SERVICES,                                                   
		TO_CHAR(COALESCE(NATL_ALOWD_SRVCS,0),'FM00000000000')                                      
		AS NATIONAL_ALLOWES_SERVICES,                                                
		TO_CHAR(COALESCE(CARR_ALOWD_CHRGS,0),'FM00000000000')                                   
		AS CARRIER_ALLOWED_CHARGES,                                                  
		TO_CHAR(COALESCE(NATL_ALOWD_CHRGS,0), 'FM000000000000') 
		AS NATIONAL_ALLOWED_CHARGES,                                                 
		TO_CHAR(COALESCE(CARR_FREQ,0), 'FM000000000')                                          
		AS CARRIER_FREQUENCY,                                                        
		TO_CHAR(COALESCE(NATL_FREQ,0) ,'FM00000000000')                                            
		AS NATIONAL_FREQUENCY,                                                       
		TO_CHAR(COALESCE(CARR_DND_SRVCS,0), 'FM000000000')                                   
		AS CARRIER_DENIED_SERVICES,                                                  
		TO_CHAR(COALESCE(NATL_DND_SRVCS,0), 'FM00000000000')                                    
		AS NATIONAL_DENIED_SERVICES,                                                 
		TO_CHAR(COALESCE(CARR_PRR_YR_ALOWD_CHRGS,0),'FM00000000000')           
		AS CARRIER_PRIOR_YEAR_CHARGES,                                               
		TO_CHAR(COALESCE(NATL_PRR_YR_ALOWD_CHRGS,0), 'FM000000000000')            
		AS NATIONAL_PRIOR_YEAR_CHARGES                                                  
		FROM BIA_{ENVNAME}.CMS_TARGET_XTR_{ENVNAME}.CLM_FMRD                                              
		GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21                  
		ORDER BY 1,2,3,8 )

 