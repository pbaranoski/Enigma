########################################################################################################
# 
# Paul Baranoski 2024-05-22 Add my personal email when certain group emails are used. While I'm included
#                           in the group email, I'm still not getting emails.
# Paul Baranoski 2024-06-05 Remove logic to add my personal email. 
# Paul Baranoski 2024-08-26 Modify Email server and port per Sampath Mettu.
# Sean Whitelock 2024-09-06 Add starttls() to secure Connection.
# Paul Baranoski 2025-01-09 Add support for email attachments.
# Paul Baranoski 2025-05-20 Add catch-all Exception when smtp server is not set up. Add code to use SMTP_SERVER constant set in SET_XTR_ENV.sh to connect to SMTP server
#                           instead of hard-coded server name. Depending on the environment, the SMTP server will be named differently.
# Paul Baranoski 2025-06-12 Add ability to process multiple file attachments with the 5th optional parm. This parameter needs to be a comma-delimited string.
########################################################################################################
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
import smtplib
import sys
from os.path import basename 
import os


def sendEmail(sender, receivers, subject, messageText, filenamesNPath2Attach=""):

    print("")
    print("In function sendEmail")

    sToEmails = ""
    
    lstReceivers = receivers.split(",")
    sToEmails = "<" + ">,<".join(lstReceivers) + ">"  

    try:

        msg = MIMEMultipart('alternative')
        htmlText = MIMEText(messageText, 'html')

        msg['Subject']= subject   
        msg['From']   = sender
        msg['To'] = sToEmails
        
        # Add message text
        msg.attach(htmlText)  
        
        #############################
        # Add attachment if exists
        #############################
        if filenamesNPath2Attach != "":

            print("Add file attachments")

            print(f"{filenamesNPath2Attach=}")
            
            # create list of filenames to attach to email
            lstAttachments = filenamesNPath2Attach.split(",")
            print(f"{len(lstAttachments)=}")
                
            for sAttFilenameNPath in lstAttachments: 
                basefilename = basename(sAttFilenameNPath)
                print(f"{basefilename=}")

                with open(sAttFilenameNPath, "rb") as attFile:
                    msgAttach = MIMEApplication(attFile.read(), Name=basefilename)
                # file is closed    
                msgAttach['Content-Disposition'] = f'attachment; filename={basefilename}'    
                msg.attach(msgAttach)   
            
            print("files have been attached to email")
 
        #print("Before getting smtpServer")	
        #secure SMTP protocol (port 465, uses SSL)
        # use this for standard SMTP protocol   (port 25, no encryption)
        #smtpServer = smtplib.SMTP('localhost', 25, None)
        #smtpServer = smtplib.SMTP('internal-Enterpris-SMTPProd-I20YLD1GTM6L-357506541.us-east-1.elb.amazonaws.com', 25, None)
        #smtpServer = smtplib.SMTP('cloud-smtp-prod.biaaws.local', 587, None)
        smtpServer = smtplib.SMTP(SMTP_SERVER, 587, None)
        smtpServer.starttls()
        print("SMTP: Connected to smtpServer")

        smtpServer.ehlo()
        print("SMTP ehlo: Successfully identified as client to server")
        #print("setdebuglevel")
        smtpServer.set_debuglevel(1)
        print("STMP: set debuglevel")
        smtpServer.ehlo()
        #print("after ehlo")

        smtpServer.sendmail(sender, lstReceivers, msg.as_string())  
        
        print ("Successfully sent email")

        smtpServer.close()

    except smtplib.SMTPSenderRefused as ex:
        print ("SMTP Error: Sender Refused")
        print(ex.smtp_error)
        print(ex.sender)
        raise

    except smtplib.SMTPAuthenticationError as e:
        print("SMTP Error: SMTP Authorization failed") 
        print(e.smtp_error)
        print(e.strerror)
        raise

    except smtplib.SMTPNotSupportedError as es:
        print("SMTP Error: Auth Extension not supported by server")
        print(es.strerror) 
        raise

    except smtplib.SMTPException as x:
        print("SMTP Error: Exception")
        print(x.with_traceback()) 
        raise  
    except Exception as err: 
        print ("some other error")
        print(f"err.strerroe:{err}")
        raise    
    except Exception as z: 
        print("General Exception")
        print(z) 
        raise         
        
#######################################################
# Is the module being called by a shell script?
# Yes --> grab parms from command line and call 
#         sendEmail function.
# NOTE: sys.argv[0] is module name 
# 
# python3 sendEmailHTML.py "BIA_SUPPORT@cms.hhs.gov" "pbaranoski-con@index-analytics.com" "Test HTML email" "<html><body><table cellspacing='1px' border='1' ><tr bgcolor='#00B0F0'><th>JIRA ticket #</th><th>Extract filename</th></tr><tr><td>IDRBI-71131-20231017-134214</td><td>FEHB_CMS_HOS_20230701_20230930_20231017.txt.gz</td></tr><tr><td>IDRBI-71128-20231017-135858</td><td>FEHB_CMS_INP_20230701_20230930_20231017.txt.gz</td></tr><tr><td>IDRBI-71134-20231017-145638</td><td>PSPSQ6_SUPPRESS_20230922.161250.csv</td></tr><tr><td>RAND_CMS_CAR_20210815_20210818</td><td>RAND_CMS_CAR_20210815_20210818_20231116.txt.gz</td></tr></body></html>"
#######################################################        
print("")
print("In sendEmailHTML.py")
print(f"{len(sys.argv)=}")
NOFParms=(int(len(sys.argv)) - 1)
print(f'{NOFParms=}')

# sys.argv = 4 parms + 1 program name --> total 5 parms    
if NOFParms > 1:
    # module being called from shell script
    lstParms = sys.argv
    sender = lstParms[1]
    receivers = lstParms[2]
    subject = lstParms[3]
    messageText = lstParms[4]
    files2AttachPathNFilename = None
    
    print(f"{sender=}")
    print(f"{receivers=}")
    print(f"{subject=}") 
    print(f"{messageText=}") 
    
    # if 5th parm passed it is filename to attach
    if NOFParms == 5:
        files2AttachPathNFilename = lstParms[5] 
        print(f"{files2AttachPathNFilename=}") 
    
    ################################################################################
    # Problem: Newline characters passed from shell script as "\n" or "\\n" are 
    #          ignored by python program as legitamate newlines in messageText.
    #          (They appear only as characters).
    # Solution: Split messageText by "\\n" to create array ("\n" will not work), 
    #           then building new messageText with newlines coded in python program
    #           in proper place in messageText.
    ################################################################################
    emailBody = ""

    messageTextArr = messageText.split('\\n')
    #print("NOF email lines: "+str(len(messageTextArr)))
    for msg in messageTextArr:
        emailBody += msg + "\n" 
    
    print(f"emailBody: {emailBody}")  
        
    try:    
        # Get SMTP Server for current environment    
        SMTP_SERVER=os.getenv('SMTP_SERVER')
        print(f"{SMTP_SERVER=}")
        
        if NOFParms == 5:
            sendEmail(sender, receivers, subject, emailBody, files2AttachPathNFilename)
        else:
            sendEmail(sender, receivers, subject, emailBody)        

    except Exception as e:
        print(e)
        sys.exit(12)
   
else:
    # module NOT called from shell script
    pass 
    
    

