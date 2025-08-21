########################################################################################################
# 
# Paul Baranoski 2024-05-22 Add my personal email when certain group emails are used. While I'm included
#                           in the group email, I'm still not getting emails.
# Paul Baranoski 2024-06-05 Remove logic to add my personal email. 
# Paul Baranoski 2024-07-03 Add optional parameters bccReceivers and replyMessage
# Paul Baranoski 2024-08-26 Modify Email server and port per Sampath Mettu.
# Sean Whitelock 2024-09-06 Add starttls() to secure Connection.
# Paul Baranoski 2025-05-20 Add catch-all Exception when smtp server is not set up. Add code to use SMTP_SERVER constant set in SET_XTR_ENV.sh to connect to SMTP server
#                           instead of hard-coded server name. Depending on the environment, the SMTP server will be named differently.
########################################################################################################

import smtplib
import sys
import os

def sendEmail(sender, receivers, subject, messageText, bccReceivers="", replyMsg="Note: Do not reply to this email. Send inquiries to bit-extractalerts@index-analytics.com."):

    print("In sendEmail function")
    
    sToEmails = ""
    
    lstReceivers = receivers.split(",")
    sToEmails = "<" + ">,<".join(lstReceivers) + ">"  

    if bccReceivers.strip() != "":
      lstReceivers.append(bccReceivers)


    message = f"""From: <{sender}>
To: {sToEmails}
Subject: {subject}

{messageText}

{replyMsg}
    """

    try:

        print("Before getting smtpServer")	
        #smtpServer = smtplib.SMTP('localhost', 25, None)
        #smtpServer = smtplib.SMTP('internal-Enterpris-SMTPProd-I20YLD1GTM6L-357506541.us-east-1.elb.amazonaws.com', 25, None)
        # cloud-smtp-nonprod.bitaws.local
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

        smtpServer.sendmail(sender, lstReceivers, message)  
        
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
        
    except Exception as z: 
        print("General Exception")
        print(z) 
        raise 
        
        
#######################################################
# Is the module being called by a shell script?
# Yes --> grab parms from command line and call 
#         sendEmail function.
# NOTE: sys.argv[0] is module name 
#######################################################        
if len(sys.argv) > 1:
    # module being called from shell script
    lstParms = sys.argv
    sender = lstParms[1]
    receivers = lstParms[2]
    subject = lstParms[3]
    messageText = lstParms[4]
    bccReceivers=None
    replyMsg=None
    
    print("len Sys argv"+str(len(sys.argv)))
    if len(sys.argv) > 5:
        bccReceivers= lstParms[5] 
        replyMsg = lstParms[6]
    
    print(f"sender:{sender}")
    print(f"receivers:{receivers}")
    print(f"subject:{subject}") 
    print(f"messageText:{messageText}") 
    print(f"bccReceivers:{bccReceivers}") 
    print(f"replyMsg:{replyMsg}") 
        
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
        
        if len(sys.argv) == 7:
            sendEmail(sender, receivers, subject, emailBody, bccReceivers, replyMsg)
        else:
            sendEmail(sender, receivers, subject, emailBody)
        
    except: 
        sys.exit(12)
    
else:
    # module NOT called from shell script
    pass 

    
    

