# <copyright file="snowconvert_helpers.py" company="Mobilize.Net">
#     Copyright (C) Mobilize.Net info@mobilize.net - All Rights Reserved
#
#     This file is part of the Mobilize Frameworks, which is
#     proprietary and confidential.
#
#     NOTICE:  All information contained herein is, and remains
#     the property of Mobilize.Net Corporation.
#     The intellectual and technical concepts contained herein are
#     proprietary to Mobilize.Net Corporation and may be covered
#     by U.S. Patents, and are protected by trade secret or copyright law.
#     Dissemination of this information or reproduction of this material
#     is strictly forbidden unless prior written permission is obtained
#     from Mobilize.Net Corporation.
# </copyright>
#
# IDRC-8341 - Ben Mercer - update quit_application to set exit 4 if there is a non-zero error code, rather than setting exit of the error code.
#                          Snowflake error codes exceed 256, the max exit allowed in shell.  If an exit code is > 256, and divisible by 256, that means shell will think the 
#                          exit code is zero. 
#
# IDRC-8950 - Ramesh Nagamani - update execute_sql_statement to display start time, end time, execution time and query id for each and every sql. Irrespective of query status 
#                               start time, end time, execution time and query id will be displayed.  
# IDRS-38553 - Vishnu Srungaram - update log_on function to make use of the secrets to login to snowflake. 
#                                 

import sys
import logging
import subprocess
import datetime
import snowflake.connector
from os import getenv
from os import access
from os import R_OK
from os import system
from os import makedirs, path, stat
from functools import singledispatch
import re
import csv
import atexit
import traceback
import json
import boto3
from botocore.exceptions import ClientError


# global status values
activity_count = 0
error_code = 0
error_level = 0
warning_code = 0
system_return_code = 0
quit_application_already_called = False

# severities dictionary
_severities_dictionary = dict()
_default_error_level = 8

# last executed sql statement
_previous_executed_sql = ""

has_passed_variables = False
passed_variables = {}

################################################################
# We use this function.  
################################################################
def configure_log(log_file = None):

    import os

    if (log_file is None):
        cur_time = str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S") ) 
        log_name = str( os.path.basename(sys.argv[0])).replace('.py', f'_SF.{cur_time}.log')     
        #stream = str(os.getcwd()).split('/')[5]
        stream = sys.path[0].split('/')[3]
        log_file = f'/app/IDRC/{stream}/CMS/logs/{log_name}'

    print(f'Snowflake connector log file = {log_file}')

    logging.basicConfig(
        filename=log_file,
        level=logging.DEBUG,
        filemode='w')
        
    # Suppress the log entries that involve the keys and secret parms   
    logging.getLogger('botocore.endpoint').setLevel(logging.WARNING)
    logging.getLogger('botocore.auth').setLevel(logging.WARNING)
    logging.getLogger('botocore.parsers').setLevel(logging.WARNING)


################################################################
# Do we use these functions?  
################################################################
def get_from_args_or_environment(arg_pos, env_variable_name, args):
    if (arg_pos < len(args)):
        return args[arg_pos]
    env_value = getenv(env_variable_name)
    return env_value

def get_argkey(astr):
     if astr.startswith('--param-'):
         astr = astr[8:astr.index('=')]
     return astr

def get_argvalue(astr):
     if astr.startswith('--param-'):
         astr = astr[astr.index('=')+1:]
     return astr

def read_param_args(args):
    script_args = [item for item  in args if item.startswith("--param-")]
    dictionary = {}
    if len(script_args) > 0:
        dictionary = { get_argkey(x) : get_argvalue(x) for x in args}
        if len(dictionary) != 0:
            has_passed_variables = True
            print("Using variables")
            print(dictionary)
    return dictionary
 
 
################################################################
# Needed by execute_sql_statement
################################################################
def expandvars(path, params, skip_escaped=False):
    """Expand environment variables of form $var and ${var}.
       If parameter 'skip_escaped' is True, all escaped variable references
       (i.e. preceded by backslashes) are skipped.
       Unknown variables are set to 'default'. If 'default' is None,
       they are left unchanged.
    """
    def replace_var(m):
        varname = m.group(3) or m.group(2)
        passvalue = params.get(varname, None)
        return getenv(varname, m.group(0) if passvalue is None else passvalue)
    reVar = (r'(?<!\)' if skip_escaped else '') + r'(\$|\&)(\w+|\{([^}]*)\})'
    return re.sub(reVar, replace_var, path)

def expands_using_params(statement, params):
    def replace_var(m):
        varname = m.group(1)
        passvalue = params.get(varname, None)
        if (passvalue is None):
            return m.group(0)
        else:
            return str(passvalue)
    reVar = r'\{([^}]*)\}'
    return re.sub(reVar, replace_var, statement) 


def expandvar(str):
    return expandvars(str,passed_variables)


################################################################
# Defining variable (List)
################################################################
opened_connections = []


################################################################
# We use this function.
################################################################
def log_on(sf_logon_file = None):

    import os
    from snowflake import connector
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    timeout=120
    envname = get_environment()
    
    if (sf_logon_file is None):
        stream = sys.path[0].split('/')[3]
    else:
        stream = os.path.abspath(sf_logon_file).split('/')[3]
    
    stream_upper = stream.upper()
    stream_lower = stream.lower()
    secret_stream_lower=stream_lower

    print(f"Stream name under log_on is:  {stream}")
    print(f"upper case Stream name under log_on is:  {stream_upper}")
    print(f"lower case Stream name under log_on is:  {stream_lower}")
    
    if stream_upper in ('BEPSD','PDB','PTB','XTR'):
        appl_name = "bia"
    else:
        appl_name="idrs"

    print(f"appl_name under log_on is:  {appl_name}")

    if appl_name == 'bia':
        secret_name = "idrc/snowflake/bia/idrc_"+envname+"_bia_"+stream_lower+"_etl"
    else:
        secret_name = "idrc/snowflake/idrs/idrc_"+envname+"_sf_om_etl_"+secret_stream_lower
    
   
    secret_env_var_name = f"MY_APP_SECRET_{stream_upper}"

    print(f"lower case secret_stream_lower under log_on is:  {secret_stream_lower}")
    print(f"secret_name under log_on is:  {secret_name}")
    print(f"secret_env_var_name under log_on is:  {secret_env_var_name}")
    
    if assume_role(sf_logon_file):
        get_secret_as_env(secret_name,secret_env_var_name)
        
        # Access the secret from environment
        #print(f"Parse the secret value :  {secret_env_var_name} ")
        param_secret = getenv(secret_env_var_name)
        if param_secret:
            secret = json.loads(param_secret)
            #print(secret)
            sf_user = secret.get("sfUser")
            sf_db = secret.get("sfDatabase")
            sf_wh = secret.get("sfWarehouse")
            env = secret.get("environment")
        else:
            print(f"Secret from environment variable ({secret_env_var_name}) is not found:")
            
    script_name = os.path.abspath(sys.argv[0]).split('/')[-1]    

    print(f'Script Name = {script_name}')
 
    if env.lower() in ('dev', 'tst', 'impl'):
        sf_account = 'cms-idrnp.privatelink'
    else:
        sf_account = 'cms-idr.privatelink'
    privatekey=secret.get("privatekey").encode('utf-8')
   
    p_key= serialization.load_pem_private_key(
        privatekey,
        password=None,
        backend=default_backend()
        )
 
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
        )
 
    con = connector.connect(
        user=sf_user,
        private_key=pkb,
        account=sf_account,
        database=sf_db,
        warehouse = sf_wh,
        login_timeout=timeout,
        session_parameters={
        'QUERY_TAG': script_name,
                           }
        )
    
    return con

################################################################
# Log_on function dependencies.
################################################################
def get_environment():
    ENVNAME = getenv("ENVNAME")
    
    if ENVNAME not in ('DEV' , 'TST' , 'IMPL' , 'PRD'):
        print(f'Environment should be either DEV or TST or IMPL or PRD. ENVNAME is set to {ENVNAME}')
        quit_application()
    else:
        print(f'ENVNAME is set to {ENVNAME}')
        envname = ENVNAME.lower()  
    
    return envname

def assume_role(sf_logon_file = None):
    import os
    start_time = datetime.datetime.now()
    print("Assume Role Start:",start_time.strftime("%Y %m %d %H:%M:%S")) 

    #stream = sys.path[0].split('/')[3]
    if (sf_logon_file is None):
        stream = sys.path[0].split('/')[3]
    else:
        stream = os.path.abspath(sf_logon_file).split('/')[3]
    print(f'Stream name under assume_role is:  {stream}')
    stream_upper = stream.upper()
    print(f'Stream in upper case under assume_role is:  {stream_upper}')
    stream_lower = stream.lower()
    print(f'Stream in lower case under assume_role is:  {stream_lower}')
    
    acct_val = getenv("IDRC_DATALAKE_AWS_ACCT")
    if acct_val:
        print(f'acct_val is  {acct_val}')
    else:
        print(f'Env variable IDRC_DATALAKE_AWS_ACCT is not set')
    
    stream_role_arn_prefix = "arn:aws:iam::"+acct_val+":role/delegatedadmin/adodeveloper/"
    print(f'stream_role_arn_prefix under assume_role is:  {stream_role_arn_prefix}')
   

    if stream_upper == 'XTR': 
        stream_role_arn=stream_role_arn_prefix+"idrc-infa-om-etl-ddom-role"
   

    stsclient = boto3.client('sts')
    identity = stsclient.get_caller_identity()
    current_arn = identity['Arn']
    print("Current IAM Role ARN:", current_arn)
    target_role_name="arn:aws:sts::"+acct_val+":assumed-role/idrc-infa-om-etl-bene-role"
    print("Target IAM Role ARN:", target_role_name)
    if ":assumed-role/" in current_arn and f"{target_role_name}" in current_arn:
        print(f"Already in target role: {current_arn}")
        # Just export the creds from the current session 
        session = boto3.Session()
        # Set temporary credentials in evironment variables
        creds = session.get_credentials().get_frozen_credentials()
        os.environ["AWS_ACCESS_KEY_ID"] = creds.access_key
        os.environ["AWS_SECRET_ACCESS_KEY"] = creds.secret_key
        if creds.token:
            os.environ["AWS_SESSION_TOKEN"] = creds.token
        print("Temporary credentials assigned to environment variables for Already assumed role")        
        return True     
    
    
    session_name = "AssumeRoleSession"
    print("Assuming the role and setting credentials as environment variables")
    try:        
        
        response = stsclient.assume_role(
            RoleArn=stream_role_arn,
            RoleSessionName=session_name
            )
        creds = response["Credentials"]
        
        # Set temporary credentials in evironment variables
        os.environ["AWS_ACCESS_KEY_ID"] = creds["AccessKeyId"]
        os.environ["AWS_SECRET_ACCESS_KEY"] = creds["SecretAccessKey"]
        os.environ["AWS_SESSION_TOKEN"] = creds["SessionToken"]
        
        print("Temporary credentials assigned to environment variables")
        assumed_arn = response["AssumedRoleUser"]["Arn"]
        print(f"Assumed IAM Role ARN: {assumed_arn}")

        return True 
    
    except ClientError as e:

        msg = "*** Failure under assuming the role : ClientError : " + str(e)
        print(msg,file=sys.stderr)
        quit_application()
        return False
    
    new_identity = stsclient.get_caller_identity()
    print("Assumed IAM Role ARN:", new_identity['Arn'])

def get_secret_as_env(secret_name,secret_env_var_name):
    import os
    try:
        #boto3 will use the credentials from the environment variables
        
        region_name = "us-east-1"
        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
        service_name='secretsmanager',
        region_name=region_name
        )
        print("Attempting to get secret  ")

        response = client.get_secret_value(SecretId=secret_name)
        value = response['SecretString']

        # Set the retrieved secret as an evironment variable
        os.environ[secret_env_var_name] = value        
        
        print(f'Secret assigned to environment variable: {secret_env_var_name} .')

    except ClientError as e:

        msg = "*** Failure under get_secret_as_env : ClientError : " + str(e)
        print(msg,file=sys.stderr)
        quit_application()
        return False
            
    except Exception as e:
        msg = "*** Failure under get_secret_as_env : Exception : " + str(e)
        print(msg,file=sys.stderr)
        quit_application()
        return False

###################################################
# execute_sql_statement - helper functions 
###################################################
def at_exit_helpers():
    print("Script done >>>>>>>>>>>>>>>>>>>>")
    for c in opened_connections:
        if not c.is_closed():
            c.close()
    quit_application()

def exception_hook(exctype, value, traceback):
    traceback_formatted = traceback.format_exception(exctype, value, traceback)
    traceback_string = "*** Failure: " + "".join(traceback_formatted)
    print(traceback_string, file=sys.stderr)
    quit_application(1)

def using(*argv):
    using_dict = {}
    Import.using(using_dict,*argv)
    return using_dict

def import_file(filename, separator = ' '):
    return Import.file(filename, separator)

def import_reset():
    return Import.reset()

################################################################
# We use this function.
################################################################
def execute_sql_statement(sql_string, con, using=None, query_tag: str = None, exit_on_error: bool = True):
    """
    Executes the given SQL statement using the passed Snowflake connection, optionally quitting
    application if SQL execution errors.
    
    Parameters:
      sql_string    : SQL statement
      con           : Snowflake connection
      using         :
      query_tag     : optional tag to associate with query
      exit_on_error : Boolean value, if True, will exit the application program if SQL statement errors.
                      If False, just prints the error message, but does not quit.
    
    Returns:
      None
    """
    global activity_count
    cur = con.cursor()
    try:
        print("Executing: {0}.".format(sql_string))
        if ("$" in sql_string or "&" in sql_string):
            print ("Expanding variables in SQL statement")
            sql_string = expandvars(sql_string, passed_variables)
            print ("Expanded string: {0}".format(sql_string))
        if (using is not None):
                #we need to change variables from {var} to %(format)
                sql_string = re.sub(r'\{([^}]*)\}',r'%(\1)',sql_string)
                #print(f"using parameters {using}")
                #sql_str    ing = expands_using_params(sql_string, using)
                #print(f"Applying using vars {sql_string}")

        # set query tag if supplied
        if query_tag is not None:
            cur.execute(f"alter session set query_tag='{query_tag}'", params=using)
        #
        start_time = datetime.datetime.now()
        print("Query Start Time:",start_time.strftime("%Y %m %d %H:%M:%S")) 
        cur.execute(sql_string, params=using)
        
        activity_count = cur.rowcount
        if activity_count >= 1:
            _print_result_set(cur)
        else:
            if (Export.expandedfilename is not None):
                _print_result_set(cur)
    except snowflake.connector.errors.ProgrammingError as e:

        error_code, error_level = _handle_sql_error(e)

        if error_code != 0 and exit_on_error:
            quit_application(error_code)
    except Exception as e:

        msg = "*** Failure " + str(e)
        print(msg,file=sys.stderr)
        error_code = 999
        if exit_on_error:
           quit_application(error_code)
    finally:
        _previous_executed_sql = sql_string
        end_time = datetime.datetime.now()
        sf_qry_id=cur.sfqid
        print("Query End Time:",end_time.strftime("%Y %m %d %H:%M:%S")) 
        print("Query Execution Time in Python for Query ID {0} is ".format(sf_qry_id), end_time-start_time)        
        
        cur.close()


################################################################
# Needed by execute_sql_statement
################################################################
def _print_result_set(cur):
    if (Export.expandedfilename is None):
        # if there is not export file set then print to console
        print("Printing Result Set:")
        print(','.join([col[0] for col in cur.description]))
        for row in cur:
            print(','.join([str(val) for val in row]))
        print()
    else:
        print(">>>>>> Exporting to " + Export.expandedfilename)
        reportdir = path.dirname(Export.expandedfilename)
        makedirs(reportdir, exist_ok=True)
        with open(Export.expandedfilename, 'a') as f:
            for row in cur:
                allarenone = all(v is None for v in row)
                if (allarenone):
                    print("Row is 'None' it will not be exported")
                else:
                    rowval=Export.separator.join([str(val) for val in row])
                    print(rowval, file=f)


def _handle_sql_error(e):
    global error_code, error_level
    error_code = e.errno
    if error_code not in _severities_dictionary or _severities_dictionary[error_code] != 0:
        msg = "*** Failure " + str(e)
        print(msg, file=sys.stderr)
        if error_code in _severities_dictionary:
            error_level = max(error_level, _severities_dictionary[error_code])
        else:
            error_level = max(error_level, _default_error_level)
    return error_code, error_level


@singledispatch
def set_error_level(arg, severity_value):
    "Invoked set_error_level with arg={0}, severity_value={1}".format(arg, severity_value)


@set_error_level.register(int)
def _(arg, severity_value):
    _severities_dictionary[arg] = severity_value


@set_error_level.register(list)
def _(arg, severity_value):
    for code in arg:
        _severities_dictionary[code] = severity_value


def set_default_error_level(severity_value):
    global _default_error_level
    _default_error_level = severity_value


def os(args):
    global system_return_code
    system_return_code = system(args)

## reads the given filename and executes the code
def readrun(line, skip=0):
    expandedpath = path.expandvars(line)
    if path.isfile(expandedpath):
        return open(expandedpath).readlines()[skip:]
    else:
        return []


def remark(arg):
    print(arg)

################################################################
# We use this function
################################################################
def quit_application(code=None):
#    code = code or error_level
#    print(f"Error Code {code}")
#    sys.exit(code)
    global quit_application_already_called
    if quit_application_already_called:
        return
    quit_application_already_called = True
    code = code or error_level
    print(f"Error Code {code}")
    # IDRC-8341 - set exit 4 if non-zero error code.  This ensures the exit code doesn't exceed Unix max allowed of 256.
    #sys.exit(code)
    
    if code == 0:
        sys.exit(0)
    else:
        sys.exit(4)


#############################################
# 
#############################################
atexit.register(at_exit_helpers)

#############################################
# two functions names exception_hook?
#############################################
def exception_hook(exctype, value, tback):
    print(f"*** Failure: {value}", file=sys.stderr)
    traceback_formatted = traceback.format_exception(exctype, value, tback)
    traceback_string = "".join(traceback_formatted)
    print(traceback_string, file=sys.stderr)
    quit_application(1)

sys.excepthook = exception_hook


#############################################
# Classes Import, Export, Parameters
#############################################   
class Import:
    expandedfilename=None
    separator=' '
    reader = None
    no_more_rows=False
    read_obj = None

    def file(file, separator=' '):

        Import.separator = separator
        Import.expandedfilename = path.expandvars(file)
        Import.reader=None
        if (not Import.read_obj is None):
            Import.read_obj.close()
        Import.read_obj=None
        Import.no_more_rows = False

    def using(globals,*argv):
        print (argv)
        try:
            variables_li = [] 
            types_li = []
            i = 0
            while i < len(argv):
                elem = argv[i]
                if (i % 2 == 0): 
                    variables_li.append(elem) 
                else: 
                    types_li.append(elem)
                i += 1
            i = 0
            # init the global variables for the using clause
            while i < len(variables_li):
                initvalue = None
                if (types_li[i].startswith("DECIMAL")):
                    initvalue = 0
                else:
                    if (types_li[i].startswith("DATE")):
                        initvalue = datetime.date.min
                    else:
                         if (types_li[i].startswith("TIMESTAMP")):
                            initvalue = datetime.datetime.min
                globals[variables_li[i]] = initvalue
                i += 1
            # open file in read mode
            if (Import.expandedfilename is not None):
                if (Import.reader is None):
                    read_obj = open(Import.expandedfilename, 'r')
                    print(f">>>>>>>>> Importing from {Import.expandedfilename}")
                    if (stat(Import.expandedfilename).st_size == 0):
                        print("Import file is empty")
                        return
                    else:
                        # pass the file object to reader() to get the reader object
                        Import.reader = csv.reader(read_obj)
                # read next row
                print("Reading row")
                row = next(Import.reader)
                # row variable is a list that represents a row in csv
                i = 0
                while i < len(variables_li):
                    globals[variables_li[i]] = row[i]
                    i += 1
        except StopIteration:
            Import.no_more_rows = True
            print ("No more rows")
        except Exception as e:
            print (f"*** Failure importing {e}")
        print("Done importing")
    def reset():
            Import.expandedfilename = None
            Import.separator = ' '

Import.file = staticmethod(Import.file)
Import.using = staticmethod(Import.using)    

class Export:
    expandedfilename=None
    separator=' '
    def report(file, separator=' '):
        Export.separator = separator
        Export.expandedfilename = path.expandvars(file)
## obsolete
    def title_dashes(state="ON",withValue=None):
        pass
## obsolete
    def width(width):
        pass
## resets any previous export settings    
    def reset():
        Export.expandedfilename = None
        Export.separator = ' '

Export.title_dashes = staticmethod(Export.title_dashes)
Export.reset = staticmethod(Export.reset)
Export.report = staticmethod(Export.report)
Export.width = staticmethod(Export.width)

class Parameters:
    passed_variables = {}

## Loading extra parameters from command line
passed_variables = read_param_args(sys.argv[1:])
