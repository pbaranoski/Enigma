#!/usr/bin/env python3
"""
blbtn_clm_ext_Driver.py

Driver for the weekly Blue Button claim extract (originally `blbtn_clm_ext.sh`).
This Python port uses `SET_XTR_ENV.py` (for environment variables) and follows
similar patterns to `aModelPythonDriver.py` for logging, emailing, and S3 steps.

Usage
-----
python blbtn_clm_ext_Driver.py [YYYY-MM-DD YYYY-MM-DD]

- With 0 parameters: compute weekly start/end dates per legacy shell logic.
- With 2 parameters: use override start/end dates.

Exit codes
----------
0  : success
12 : failure in any stage (matches original shell behavior)
"""

import os
import sys
import subprocess
from datetime import date, datetime, timedelta
from pathlib import Path

# Load environment (sets many os.environ values)
try:
    import SET_XTR_ENV as XTRENV
except Exception as e:
    print(f"Failed to import SET_XTR_ENV.py: {e}")
    sys.exit(12)

# Optional logging helper (pattern taken from aModelPythonDriver.py)
try:
    import LoggerStandard as EnigmaLog
except Exception:
    EnigmaLog = None

# Optional helper used by other drivers to parse S3 filenames & counts
try:
    from FilenameCounts import getExtractFilenamesAndCounts
except Exception:
    getExtractFilenamesAndCounts = None

# ---------------------------
# Helpers
# ---------------------------

def set_env_var(key: str, val: str) -> None:
    os.environ[key] = str(val)


def compute_week_dates(override_from: str = None, override_to: str = None):
    """Replicate the legacy shell logic to compute weekly start/end dates.
    Returns (wkly_strt_dt, wkly_end_dt) in YYYY-MM-DD strings.
    """
    if override_from and override_to:
        return override_from, override_to

    today = date.today()

    # Shell logic baseline: start from 14 days ago
    dow = (today - timedelta(days=14)).strftime('%A')
    wkly_strt_dt = (today - timedelta(days=14)).strftime('%Y-%m-%d')

    # Default decrement value 14 + 7 ("a week before 14 days ago"): d = 21
    d = 21
    d_hold = d

    # Find Monday prior to today by moving back from 'today - d days'
    while dow != 'Monday':
        d -= 1
        dow = (today - timedelta(days=d)).strftime('%A')
        wkly_strt_dt = (today - timedelta(days=d)).strftime('%Y-%m-%d')

    # End-of-week determination
    if d == d_hold:
        # If today is Monday => end is a week from yesterday (today - 8 days)
        wkly_end_dt = (today - timedelta(days=8)).strftime('%Y-%m-%d')
    else:
        # 6 days after the found Monday
        d2 = -d + 6
        wkly_end_dt = (today + timedelta(days=d2)).strftime('%Y-%m-%d')

    return wkly_strt_dt, wkly_end_dt


def run_cmd(cmd_list, check=True):
    """Run a command and capture stdout/stderr. Returns (rc, CompletedProcess)."""
    try:
        proc = subprocess.run(
            cmd_list,
            capture_output=True,
            text=True,
            check=check,
        )
        return proc.returncode, proc
    except subprocess.CalledProcessError as e:
        return e.returncode, e
    except Exception as e:
        # Mimic shell exit semantics
        return 12, subprocess.CompletedProcess(cmd_list, 12, stdout='', stderr=str(e))


# ---------------------------
# Main processing
# ---------------------------

def main():
    # Establish timestamp and log file
    tmstmp = datetime.now().strftime('%Y%m%d.%H%M%S')
    log_dir = os.environ.get('LOG_PATH', '/app/IDRC/XTR/CMS/logs')
    logname = f"{log_dir}/blbtn_clm_ext_{tmstmp}.log"

    # Initialize logging
    rootLogger = None
    if EnigmaLog is not None:
        try:
            rootLogger = EnigmaLog.setLogging(logname)
            rootLogger.info(f"\nblbtn_clm_ext_Driver.py started at {tmstmp}")
        except Exception:
            rootLogger = None
    if rootLogger is None:
        # Fallback: simple file writes
        Path(logname).parent.mkdir(parents=True, exist_ok=True)
        Path(logname).touch(exist_ok=True)
        with open(logname, 'a') as lf:
            lf.write(f"\nblbtn_clm_ext_Driver.py started at {tmstmp}\n")

    # Change working directory to RUN (like shell script)
    run_dir = os.environ.get('RUN', '/app/IDRC/XTR/CMS/scripts/run')
    try:
        os.chdir(run_dir)
    except Exception as e:
        _log(rootLogger, logname, f"Failed to chdir to RUN dir {run_dir}: {e}")
        return 12
    _log(rootLogger, logname, f"pwd={os.getcwd()}")

    # Validate parameter count (0 or 2)
    nof_parms = len(sys.argv) - 1
    if nof_parms not in (0, 2):
        _log(rootLogger, logname, f"Incorrect # of parameters sent to script. NOF parameters: {nof_parms}")
        return 12
    else:
        _log(rootLogger, logname, f"There were {nof_parms} override parameters to script.")

    parm_from = sys.argv[1] if nof_parms == 2 else None
    parm_to = sys.argv[2] if nof_parms == 2 else None
    _log(rootLogger, logname, "")
    _log(rootLogger, logname, "Parameters to script:")
    _log(rootLogger, logname, f" NOF parameters for script: {nof_parms}")
    _log(rootLogger, logname, f" ParmOverrideFromDt={parm_from}")
    _log(rootLogger, logname, f" ParmOverrideToDt={parm_to}")

    # Compute weekly dates
    _log(rootLogger, logname, "")
    _log(rootLogger, logname, "Calculate weekly extract dates")
    wkly_strt_dt, wkly_end_dt = compute_week_dates(parm_from, parm_to)
    _log(rootLogger, logname, f"wkly_strt_dt={wkly_strt_dt}")
    _log(rootLogger, logname, f"wkly_end_dt={wkly_end_dt}")

    # Export environment variables for downstream Python usage
    set_env_var('TMSTMP', tmstmp)
    set_env_var('wkly_strt_dt', wkly_strt_dt)
    set_env_var('wkly_end_dt', wkly_end_dt)

    # Execute Python code to extract claims data
    py_cmd = os.environ.get('PYTHON_COMMAND', 'python3')
    _log(rootLogger, logname, "")
    _log(rootLogger, logname, "Start execution of blbtn_clm_ext.py program")
    rc, proc = run_cmd([py_cmd, 'blbtn_clm_ext.py'], check=False)
    _log_proc(rootLogger, logname, proc)
    if rc != 0:
        _log(rootLogger, logname, "")
        _log(rootLogger, logname, "Python script blbtn_clm_ext.py failed")
        subject = f"Weekly Blue Button Extract - Failed ({os.environ.get('ENVNAME', 'UNK')})"
        msg = "The weekly Blue Button extract has failed."
        send_email(rootLogger, logname, py_cmd, os.environ.get('CMS_EMAIL_SENDER', 'BIA_SUPPORT@cms.hhs.gov'),
                   os.environ.get('BLBTN_EMAIL_FAILURE_RECIPIENT', os.environ.get('ENIGMA_EMAIL_FAILURE_RECIPIENT', 'bit-extractalerts@index-analytics.com')),
                   subject, msg)
        return 12

    _log(rootLogger, logname, "")
    _log(rootLogger, logname, "Python script blbtn_clm_ext.py completed successfully.")

    # Concatenate S3 files (use existing shell scripts for now)
    _log(rootLogger, logname, "")
    _log(rootLogger, logname, "Concatenate S3 files using CombineS3Files.sh")
    s3bucket = f"{os.environ.get('XTR_BUCKET', '')}/{os.environ.get('BLBTN_BUCKET_FLDR', '')}"
    _log(rootLogger, logname, f"S3BUCKET={s3bucket}")
    concat_filename = f"blbtn_clm_ext_{tmstmp}.txt.gz"
    _log(rootLogger, logname, f"concatFilename={concat_filename}")
    rc, proc = run_cmd([f"{run_dir}/CombineS3Files.sh", s3bucket, concat_filename], check=False)
    _log_proc(rootLogger, logname, proc)
    if rc != 0:
        _log(rootLogger, logname, "")
        _log(rootLogger, logname, "Shell script CombineS3Files.sh failed.")
        subject = f"Combining S3 files in blbtn_clm_ext_Driver.py - Failed ({os.environ.get('ENVNAME', 'UNK')})"
        msg = "Combining S3 files in blbtn_clm_ext_Driver.py has failed."
        send_email(rootLogger, logname, py_cmd, os.environ.get('CMS_EMAIL_SENDER', 'BIA_SUPPORT@cms.hhs.gov'),
                   os.environ.get('ENIGMA_EMAIL_FAILURE_RECIPIENT', 'bit-extractalerts@index-analytics.com'),
                   subject, msg)
        return 12

    # Get list of S3 files and record counts (for success email)
    _log(rootLogger, logname, "")
    _log(rootLogger, logname, "Get S3 Extract file list and record counts")
    s3_files = None
    if getExtractFilenamesAndCounts is not None:
        try:
            s3_files = getExtractFilenamesAndCounts(rootLogger, logname)
        except Exception as e:
            _log(rootLogger, logname, f"getExtractFilenamesAndCounts failed: {e}")
    else:
        _log(rootLogger, logname, "FilenameCounts module not available.")
    _log(rootLogger, logname, f"S3Files={s3_files}")

    # Send success email
    _log(rootLogger, logname, "")
    _log(rootLogger, logname, "Send success email with S3 Extract filename.")
    subject = f"Weekly Blue Button claim extract ({os.environ.get('ENVNAME', 'UNK')})"
    msg = (
        "The Weekly Blue Button claim extract has completed.\n\n"
        "The following file(s) were created:\n\n"
        f"{s3_files}"
    )
    rc_email = send_email(rootLogger, logname, py_cmd,
                          os.environ.get('CMS_EMAIL_SENDER', 'BIA_SUPPORT@cms.hhs.gov'),
                          os.environ.get('BLBTN_EMAIL_SUCCESS_RECIPIENT', 'bit-extractalerts@index-analytics.com'),
                          subject, msg)
    if rc_email != 0:
        _log(rootLogger, logname, "")
        _log(rootLogger, logname, "Error in calling sendEmail.py")
        subject = f"Sending Success email in blbtn_clm_ext_Driver.py - Failed ({os.environ.get('ENVNAME', 'UNK')})"
        msg = "Sending Success email in blbtn_clm_ext_Driver.py has failed."
        send_email(rootLogger, logname, py_cmd, os.environ.get('CMS_EMAIL_SENDER', 'BIA_SUPPORT@cms.hhs.gov'),
                   os.environ.get('ENIGMA_EMAIL_FAILURE_RECIPIENT', 'bit-extractalerts@index-analytics.com'),
                   subject, msg)
        return 12

    # EFT Process
    _log(rootLogger, logname, " ")
    _log(rootLogger, logname, "EFT Blue Button Claim Extract File")
    rc, proc = run_cmd([f"{run_dir}/ProcessFiles2EFT.sh", s3bucket], check=False)
    _log_proc(rootLogger, logname, proc)
    if rc != 0:
        _log(rootLogger, logname, "")
        _log(rootLogger, logname, "Shell script ProcessFiles2EFT.sh failed.")
        subject = f"Blue Button Claim Extract EFT process - Failed ({os.environ.get('ENVNAME', 'UNK')})"
        msg = "Blue Button Claim Extract EFT process has failed."
        send_email(rootLogger, logname, py_cmd, os.environ.get('CMS_EMAIL_SENDER', 'BIA_SUPPORT@cms.hhs.gov'),
                   os.environ.get('ENIGMA_EMAIL_FAILURE_RECIPIENT', 'bit-extractalerts@index-analytics.com'),
                   subject, msg)
        return 12

    # Clean-up data directory
    data_dir = os.environ.get('ENVPATH', '/app/IDRC/XTR/CMS') + '/data/'
    ext_dt_cfg_file = os.environ.get('EXT_DT_CONFIG_FILE')
    _log(rootLogger, logname, "")
    _log(rootLogger, logname, f"Remove {ext_dt_cfg_file} from data directory")
    if ext_dt_cfg_file:
        try:
            os.remove(f"{data_dir}{ext_dt_cfg_file}")
        except Exception as e:
            _log(rootLogger, logname, f"Warning: could not remove {ext_dt_cfg_file}: {e}")

    # End
    _log(rootLogger, logname, "")
    _log(rootLogger, logname, "blbtn_clm_ext_Driver.py completed successfully.")
    _log(rootLogger, logname, f"Ended at {tmstmp}")
    _log(rootLogger, logname, "")

    return 0


def _log(rootLogger, logname, msg):
    if rootLogger is not None:
        rootLogger.info(msg)
    else:
        with open(logname, 'a') as lf:
            lf.write(str(msg) + "\n")


def _log_proc(rootLogger, logname, proc):
    try:
        stdout = getattr(proc, 'stdout', '') or ''
        stderr = getattr(proc, 'stderr', '') or ''
        rc = getattr(proc, 'returncode', None)
        if stdout:
            _log(rootLogger, logname, f"\n{stdout}")
        if stderr:
            _log(rootLogger, logname, f"\n{stderr}")
        if rc is not None:
            _log(rootLogger, logname, f"proc.returncode={rc}")
    except Exception:
        pass


def send_email(rootLogger, logname, py_cmd, sender, recipient, subject, msg):
    rc, proc = run_cmd([py_cmd, 'sendEmail.py', sender, recipient, subject, msg], check=False)
    _log_proc(rootLogger, logname, proc)
    return rc


if __name__ == '__main__':
    rc = main()
    sys.exit(rc)
