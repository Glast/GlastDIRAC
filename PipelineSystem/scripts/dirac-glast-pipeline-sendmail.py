#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-sys-sendmail
# Author :  Matvey Sapunov
# Author :  Stephan Zimmer
########################################################################

"""
  Utility to send an e-mail using DIRAC notification service. This is for Glast.
    
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Base                                    import Script

from DIRAC import S_OK

class Params(object):
    def __init__(self):
        self.to = ''
        self.fr = ''
        self.debugMail = None
        self.subject = ''
        self.body = ''
        self.filename = ''
    def setTo(self,opt):
        self.to = opt
        return S_OK()
    def setFrom(self,opt):
        self.fr = opt
        return S_OK()
    def setSubject(self,opt):
        self.subject = opt
        return S_OK()
    def setBody(self,opt):
        self.body = opt.replace("\\n","\n")
        return S_OK()
    def setFileName(self,opt):
        self.filename = opt
        return S_OK()
    def setDebugMail(self,opt):
        self.debugMail = opt
        return S_OK()
    def registerSwitchs(self):
        Script.registerSwitch("T:", "To=", "mail To", self.setTo)
        Script.registerSwitch("F:","From=","mail from", self.setFrom)
        Script.registerSwitch("S:","Subject=","mail Subject",self.setSubject)
        Script.registerSwitch("B:","Body=","mail Body",self.setBody)
        Script.registerSwitch("f:","File=","Body content file",self.setFileName)
        Script.registerSwitch("dm:","debugMail=","mail address for debugging purposes",self.setDebugMail)
        Script.setUsageMessage( '$s -T you@mail.com -F me@mail.com -S subject -B "My Body\n is ace"' )

if __name__== "__main__":
    cli = Params()
    cli.registerSwitchs()
    Script.parseCommandLine( ignoreErrors = True )
    
    from DIRAC                                              import gLogger, exit as DIRACexit
    
    from DIRAC.FrameworkSystem.Client.NotificationClient    import NotificationClient
    
    if not cli.to or not cli.fr or not cli.subject:
        gLogger.error( "Missing argument" )
        DIRACexit( 2 )
    if not cli.body and not cli.filename:
        gLogger.error("Missing body")
        DIRACexit(2)
    if cli.filename:
        cli.body = "".join(file(cli.filename,"r").readlines())
    ntc = NotificationClient()
    gLogger.notice("Sending:"," ".join([cli.to , cli.subject , cli.body , cli.fr] ))
    gLogger.verbose("sendMail(%s,%s,%s,%s,%s)" % ( cli.to , cli.subject , cli.body , cli.fr , False ))
    result = ntc.sendMail( cli.to , cli.subject , cli.body , cli.fr , localAttempt = False )
    if not result[ "OK" ]:
        gLogger.error( result[ "Message" ] )
        DIRACexit( 6 )
  
    DIRACexit( 0 )
