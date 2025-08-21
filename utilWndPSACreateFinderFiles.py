import tkinter as tk
from tkinter import filedialog as fd
from tkinter import messagebox
#from tkinter.tix import *

import pandas as pd

import os
import re
from datetime import datetime
from io import StringIO

cur_dir = ""
#sFileHeader = ""

sMDC_DESC_TBL = '''
MDC_CD,MDC_DESC
PRE,Pre-MDC
01,Diseases & Disorders of the Nervous System
02,Diseases & Disorders of the Eye
03,"Diseases & Disorders of the Ear,Nose,Mouth & Throat"
04,Diseases & Disorders of the Respiratory System
05,Diseases & Disorders of the Circulatory System
06,Diseases & Disorders of the Digestive System
07,Diseases & Disorders of the Hepatobiliary System And Pancreas
08,Diseases & Disorders of the Musculoskeletal System And Connective Tissue
09,"Diseases & Disorders of the Skin, Subcutaneous Tissue And Breast"
10,"Endocrine, Nutritional And Metabolic System"
11,Diseases & Disorders of the Kidney And Urinary Tract
12,Diseases & Disorders of the Male Reproductive System
13,Diseases & Disorders of the Female Reproductive System
14,"Pregnancy, Childbirth And Puerperium"
15,Newborn And Other Neonates (Perinatal Period)
16,Diseases & Disorders of the Blood and Blood Forming Organs and Immunological Disorders
17,Myeloproliferative Diseases and Disorders (Poorly DifferentiatedNeoplasms)
18,"Infectious & Parasitic Diseases, Systemic or Unspecified Sites"
19,Mental Diseases and Disorders
20,Alcohol/Drug Use or Drug Induced Organic Mental Disorders
21,"Injuries, Poisoning And Toxic Effect of Drugs"
22,Burns
23,Factors Influencing Health Status and Other Contacts with HealthServices
24,Multiple Significant Trauma
25,Human Immunodeficiency Virus Infection
'''

def removeDFCols(df, lstCols2Keep):

    lstDFCols = df.columns.values.tolist()
    #print(lstDFCols)
    
    lstCols2Drop = [sCol for sCol in lstDFCols if sCol not in lstCols2Keep]

    for sColTxt in lstCols2Drop:
        df.drop(sColTxt, axis=1, inplace=True)

    return df  


def sel_out_fldr(lblLabel):

    global cur_dir
    global sFileHeader

    filedir = fd.askdirectory(
        title='Select a directory for output files',
        initialdir=cur_dir
    )


    # Assign selected filename to screen label
    lblLabel.config(text=filedir)

    # set the current default directory for dialog box
    cur_dir = os.path.dirname(filedir)
    print (cur_dir)


def sel_xlsx_file(lblLabel):

    global cur_dir

    filetypes = (
        ('Excel files', '*.xlsx'),
    )

    filename = fd.askopenfilename(
        title='Open a file',
        initialdir=cur_dir,
        filetypes=filetypes 
    )


    # Verify filename has extension
    # and that extension is xlsx
    arrFileParts = os.path.splitext(filename)
    if arrFileParts[1].strip() == "":
        filename = arrFileParts[0] + ".xlsx"
    elif arrFileParts[1].strip() != "xlsx":
        filename = arrFileParts[0] + ".xlsx"     

    # Assign selected filename to screen label
    lblLabel.config(text=filename)

    # set the current default directory for dialog box
    cur_dir = os.path.dirname(filename)
    print (cur_dir)


def initiateCreateFilesAction():

    global IN_EXCEL_FINDER_FILE
    global OUT_DIRECTORY

    ########################################
    # 1) Perform validation of input fields
    # 2) Perform Compare
    ########################################

    # load variables with window values
    txtInExcelFile = lblInFile1Text.cget("text")
    txtOutDir = lblOutFileText.cget("text")

    # Verify that input values have been entered/selected

    if txtInExcelFile == "":
        messagebox.showerror("Error", "Input Excel Finder file has not been selected!")
        return

    if txtOutDir == "":
        messagebox.showerror("Error", "Output File directory has not been selected!")
        return

    #####################################################
    # 1) Load variables needed by compareFiles function
    #    using screen input values.
    # 2) Perform csv file comparison
    #####################################################
    IN_EXCEL_FINDER_FILE = txtInExcelFile
    OUT_DIRECTORY = txtOutDir
 
    createFiles()

    ###########################################
    # Comparison has completed
    ###########################################
    messagebox.showinfo("Complete", "Results have been generated!")


def trimTrailingSpaces(df):
    
    # remove trailing spaces in column name
    ####df.columns = df.columns.str.rstrip()
    
    # Remove duplicate spaces; strip leading and trailing spaces
    df = df.replace("\s+", " ", regex=True).apply(lambda x: x.str.strip())
    print (f"df={df}")
   
    return df


def createFiles():

    #########################################################
    # Create pandas excel object from Excel file 
    #########################################################
    xl = pd.ExcelFile(IN_EXCEL_FINDER_FILE)

    #########################################################
    # Process individual Excel sheets 
    #########################################################
    for sheetName in xl.sheet_names:

        print (f"sheetName: {sheetName}")

    ######################################################
    # Determine which client Finder File is being processed
    ######################################################
        if IN_EXCEL_FINDER_FILE.find("Inpatient DRG") == -1:
            DRG_CLIENT_FINDER_FILE_SW = "N"
        else:    
            DRG_CLIENT_FINDER_FILE_SW = "Y"

        #print (f"DRG_CLIENT_FINDER_FILE_SW:{DRG_CLIENT_FINDER_FILE_SW}")
    ##########################################################
    # Process Excel sheet:
    ##########################################################
        #columns = xl.parse(sheetName).columns
        #converters = {column: str for column in columns}

        if DRG_CLIENT_FINDER_FILE_SW == "Y":  
            # Remove extra header (merged columns)
            dfCurrentSheet = xl.parse(sheet_name=sheetName, skiprows=1, dtype=str) 
        else:
            dfCurrentSheet = xl.parse(sheet_name=sheetName, skiprows=0, dtype=str)   
        #print(dfCurrentSheet)

        # Remove trailing spaces from column header Names
        dfCurrentSheet = trimTrailingSpaces(dfCurrentSheet)

        ######################################################
        # Perform additional processin for DRG Finder File
        ######################################################
        if DRG_CLIENT_FINDER_FILE_SW == "Y":

            # Create data frame of MDC_CD/MDC_DESC for df join
            strMDCTable = StringIO(sMDC_DESC_TBL)
            dfMDCDesc = pd.read_csv(strMDCTable, sep =",", quotechar='"',  dtype=str)
            #print (f"dfMDCDesc:{dfMDCDesc}")

            # Only keep some columns from client Finder file
            lstCols2Keep = ("MS-DRG","MDC","MDC_DESC")
            dfCurrentSheet = removeDFCols(dfCurrentSheet, lstCols2Keep)

            # Join MDC/MDC_DESC data frame with Finder file data
            dfCurrentSheetWDesc = dfCurrentSheet.merge(dfMDCDesc, how='inner', left_on="MDC", right_on="MDC_CD" )

            # Only keep some columns after join
            lstCols2Keep = ("MS-DRG","MDC_CD","MDC_DESC")
            dfCurrentSheet = removeDFCols(dfCurrentSheetWDesc, lstCols2Keep)

            print(f"dfCurrentSheetWDesc:{dfCurrentSheetWDesc}")

        ######################################################
        # Create output filename using sheet name
        ######################################################
        sTMSTMP = datetime.now().strftime("%Y%m%d.%H%M%S")

        #PSA_FINDER_FILE_APC_Categories_20231205.csv
        #PSA_FINDER_FILE_HCPCS_APC_CATEGORIES_20231205.csv
        #PSA_FINDER_FILE_DRG_MDC_20231206.csv

        if sheetName == "APC":
            sOutFilename = f"PSA_FINDER_FILE_APC_Categories_{sTMSTMP}.csv"
        elif sheetName == "ASC":
            sOutFilename = f"PSA_FINDER_FILE_HCPCS_APC_CATEGORIES_{sTMSTMP}.csv"
        elif DRG_CLIENT_FINDER_FILE_SW == "Y":     
            sOutFilename = f"PSA_FINDER_FILE_DRG_MDC_{sTMSTMP}.csv"
        else:
            print(f"Unexpected sheet name:{sheetName}")    
            return 12    


        print(f"sOutFilename:{sOutFilename}")

    ##########################################################
    # Write Excel sheet as csv file
    ##########################################################
        sOutPathAndFilename = os.path.join(OUT_DIRECTORY, sOutFilename)
        dfCurrentSheet.to_csv(sOutPathAndFilename, sep=',', encoding='utf-8', index=False, header=True)

  
    return 0

def main():

    ####################################################
    # define variables
    ####################################################
    global MDIWnd
    global lblInFile1Text
    global lblInFile2Text
    global lblOutFileText
    global txtOmitCols
    global intChkbxIgnoreCase 

    ####################################################
    # Build window
    ####################################################
    MDIWnd= tk.Tk()
    MDIWnd.title("Create PSA Finder Files")
    MDIWnd.geometry("1100x300")

    lblHdr = tk.Label(MDIWnd, text='Create PSA Finder Files')
    lblHdr.config(font=('helvetica', 14))
    lblHdr.grid(row=0, column=3, columnspan=3, padx=5, pady=10)

    lblSpacer = tk.Label(MDIWnd, text="          ")
    lblSpacer.grid(row=0, column=0)

    ##############################
    # Select InFile1
    ##############################
    btnInFile1 = tk.Button(text='  Select File  ', command=lambda:sel_xlsx_file(lblInFile1Text), bg='blue', fg='white', font=('helvetica', 8, 'bold'))
    btnInFile1.grid(row=1, column=1, pady=6)

    #lblInFile1Label = tk.Label(MDIWnd, text='Input File 1:', bd=1, relief="sunken")
    lblInFile1Label = tk.Label(MDIWnd, text='Excel Finder File:')
    lblInFile1Label.config(font=('helvetica', 10))
    lblInFile1Label.grid(row=1, column=2, sticky='e', pady=1)

    # Selected InFile1
    lblInFile1Text = tk.Label(MDIWnd, text="")
    lblInFile1Text.config(font=('helvetica', 10))
    lblInFile1Text.grid(row=1, column=3, sticky='w')

    ###############################
    # Output File Label
    ###############################
    btnOutFile = tk.Button(text='Select Directory', command=lambda:sel_out_fldr(lblOutFileText), bg='blue', fg='white', font=('helvetica', 8, 'bold'))
    btnOutFile.grid(row=3, column=1, padx=2, pady=2)

    lblOutFileLabel = tk.Label(MDIWnd, text='Output Directory:')
    lblOutFileLabel.config(font=('helvetica', 10))
    lblOutFileLabel.grid(row=3, column=2, sticky='e')

    # Selected Output File
    lblOutFileText = tk.Label(MDIWnd, text='')
    lblOutFileText.config(font=('helvetica', 10))
    lblOutFileText.grid(row=3, column=3, sticky='w')

    ###############################
    # Buttons
    ###############################
    btnCreate = tk.Button(text='Create Files', command=initiateCreateFilesAction, bg='blue', fg='white', font=('helvetica',10, 'bold'))
    btnCreate.grid(row=6, column=3, sticky='w', pady=10)

    ####################################
    # Process Window messages
    ####################################
    MDIWnd.mainloop()


if __name__ == "__main__":
    
    main()




