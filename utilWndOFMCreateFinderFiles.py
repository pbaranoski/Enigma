import tkinter as tk
from tkinter import filedialog as fd
from tkinter import messagebox
#from tkinter.tix import *

import pandas as pd

import os
import re
from datetime import datetime

cur_dir = ""
#sFileHeader = ""


def removeDFCols(df, lstCols2Keep):

    lstDFCols = df.columns.values.tolist()
    print(lstDFCols)
    
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
    df.columns = df.columns.str.rstrip()
   
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

    ##########################################################
    # Process Excel sheet:
    # Remove all columns except Contract ID and Plan ID
    ##########################################################
        #columns = xl.parse(sheetName).columns
        #converters = {column: str for column in columns}
        dfCurrentSheet = xl.parse(sheetName) 

        # Remove trailing spaces from column header Names
        dfCurrentSheet = trimTrailingSpaces(dfCurrentSheet)

        # Only keep Contract and Plan IDs
        lstCols2Keep = ("Contract ID","Plan ID","Mailbox")
        dfCurrentSheet = removeDFCols(dfCurrentSheet, lstCols2Keep)

        # Add leading zeroes to Plan ID if missing
        dfCurrentSheet['Plan ID'] = dfCurrentSheet['Plan ID'].map(lambda x: f'{x:0>3}')

        ######################################################
        # Create output filename using sheet name
        ######################################################
        """
        sTabName = sheetName
        sTabName = re.sub("[,-]","",sTabName)
        sTabName = sTabName.replace(" and ","")
        sTabName = sTabName.replace("LLC","")
        sTabName = sTabName.replace(" ","")
        sContractor = sTabName
        """
        sMailbox = dfCurrentSheet['Mailbox'].iloc[0] 
        sTMSTMP = datetime.now().strftime("%Y%m%d.%H%M%S")

        #sOutFilename = f"OFM_PDE_Finder_File_{sContractor}_{sTMSTMP}.txt"
        sOutFilename = f"OFM_PDE_Finder_File_{sMailbox}_{sTMSTMP}.txt"

        print(f"sOutFilename:{sOutFilename}")

    ##########################################################
    # Write Excel sheet as csv file
    ##########################################################
        sOutPathAndFilename = os.path.join(OUT_DIRECTORY, sOutFilename)
        dfCurrentSheet.to_csv(sOutPathAndFilename, sep=',', encoding='utf-8', index=False, header=None)

  
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
    MDIWnd.title("Create OFM Finder Files")
    MDIWnd.geometry("1100x300")

    lblHdr = tk.Label(MDIWnd, text='Create OFM Finder Files')
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




