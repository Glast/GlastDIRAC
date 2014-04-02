#!/usr/bin/env python

""" Pipeline kill script - use host certificate and shifter credentials for killing a job
Created 03/2014
@author: S. Zimmer (OKC/SU)

"""

if __name__ == "__main__":
    import sys, os, shutil, glob
    from DIRAC.Core.Base import Script
    from DIRAC import gLogger, exit as dexit
    from DIRAC.Interfaces.API.Dirac import Dirac
    from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
    from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
    # thanks to Stephane for suggesting this fix!
    Script.addDefaultOptionValue('/DIRAC/Security/UseServerCertificate','y')
    Script.parseCommandLine(ignoreErrors=True)
    Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... JobID ...' % Script.scriptName,
                                     'Arguments:',
                                     '  JobID:    DIRAC Job ID' ] ) )
    args = Script.getPositionalArgs() 
    # get necessary credentials
    op = Operations("glast.org")
    shifter = op.getValue("Pipeline/Shifter","/DC=org/DC=doegrids/OU=People/CN=Stephan Zimmer 799865")
    shifter_group = op.getValue("Pipeline/ShifterGroup","glast_user")
    result = gProxyManager.downloadProxyToFile(shifter,shifter_group,requiredTimeLeft=10000)
    if not result['OK']:
        gLogger.error("No valid proxy found; ",result['Message'])
        dexit(1)
    proxy = result[ 'Value' ]
    os.environ['X509_USER_PROXY'] = proxy
    gLogger.info("using proxy %s"%proxy)
    d = Dirac(True,"myRepo.rep")
    exitCode = 0
    errorList = []
    if len( args ) < 1:
        Script.showHelp()
    for job in args:
        result = dirac.kill( job )
        if result['OK']:
            gLogger.info('Killed job %s' % ( job ))
        else:
            errorList.append( ( job, result['Message'] ) )
            exitCode = 2
    for error in errorList:
        gLogger.error("ERROR %s: %s" % error)
    DIRAC.exit( exitCode )
