def setLogging(LOGNAME):

    import logging
    import sys
    
    loggerName = LOGNAME
    rootLogger = logging.getLogger(loggerName)
    rootLogger.setLevel(logging.INFO)

    if not rootLogger.handlers:
        fh = logging.FileHandler(f"{LOGNAME}",encoding='utf-8')
        sh = logging.StreamHandler(sys.stdout)
        
        formatter = logging.Formatter(
            fmt="%(asctime)s %(levelname)-8s %(funcName)-30s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        
        fh.setFormatter(formatter)
        rootLogger.addHandler(fh)
        #rootLogger.addHandler(sh)
        
        #rootLogger.propagate = False  
        
        #os.chmod(LOG_DIR, 0o777)  # for Python3

    return rootLogger
    
    
