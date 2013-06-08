#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-sys-sendmail
# Author :  Matvey Sapunov
########################################################################

"""
  Utility to send an e-mail using DIRAC notification service. This is for Glast.
    
"""

__RCSID__ = "$Id$"

import socket , sys , os

from DIRAC.Core.Base                                    import Script

from DIRAC import S_OK

class Params(object):
  def __init__(self):
    self.to = ''
    self.fr = ''
    self.subject = ''
    self.body = ''
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
    self.body = opt.strip()
    return S_OK()
  
  def registerSwitchs(self):
    Script.registerSwitch("T:", "To=", "mail To", self.setTo)
    Script.registerSwitch("F:","From=","mail from", self.setFrom)
    Script.registerSwitch("S:","Subject=","mail Subject",self.setSubject)
    Script.registerSwitch("B:","Body=","mail Body",self.setBody)
    Script.setUsageMessage( '$s -T you@mail.com -F me@mail.com -S subject -B "My Body\n is ace"' )

if __name__== "__main__":
  cli = Params()
  cli.registerSwitchs()
  Script.parseCommandLine( ignoreErrors = True )
  
  from DIRAC                                              import gLogger, exit as DIRACexit
  
  from DIRAC.FrameworkSystem.Client.NotificationClient    import NotificationClient
  
  if not cli.to or not cli.fr or not cli.subject or not cli.body:
    gLogger.error( "Missing argument" )
    DIRACexit( 2 )
  

  ntc = NotificationClient()
  gLogger.verbose("Sending:"," ".join(cli.to , cli.subject , cli.body , cli.fr ))
  print "sendMail(%s,%s,%s,%s,%s)" % ( cli.to , cli.subject , cli.body , cli.fr , False )
  result = ntc.sendMail( cli.to , cli.subject , cli.body , cli.origin , localAttempt = False )
  if not result[ "OK" ]:
    gLogger.error( result[ "Message" ] )
    DIRACexit( 6 )
  
  DIRACexit( 0 )
