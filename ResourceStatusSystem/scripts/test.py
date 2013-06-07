#/bin/env python

if __name__=="__main__":
  from DIRAC.Core.Base import Script
  Script.parseCommandLine()
  
  from DIRAC import gLogger, exit as dexit
  
  from GlastDIRAC.ResourceStatusSystem.Client.SoftwareTagClient import SoftwareTagClient
  from DIRAC.ConfigurationSystem.Client.Helpers.Resources                import getQueues

  sw = SoftwareTagClient()
  mytag  = 'SomeTag'
  mysite = 'LCG.LAL.fr'
  
  #This is what the siteadmin does
  res = sw.addTagAtSite(mytag, mysite)
  if not res['OK']:
    gLogger.error(res['Message'])
    dexit(1)
  else:
    gLogger.notice("Added %s to %s" % (mytag, mysite))
    
  #This is the most common Call from Clients
  res = sw.getSitesForTag(mytag)
  if not res['OK']:
    gLogger.error(res['Message'])
  else:
    gLogger.notice("Sites: ", res['Value'])
  
  #Get the tags with valid status
  res = sw.getTagsWithStatus("New")
  if not res['OK']:
    gLogger.error(res['Message'])
  else:
    gLogger.notice("Found the tags:",res['Value'])
  
  #Get tags with fucked up status
  res = sw.getTagsWithStatus("NewSomething")
  if not res['OK']:
    gLogger.error(res['Message'])
  else:
    gLogger.notice("Found the tags:", res['Value'])
  
  res = sw.updateStatus(mytag, mysite, "Probing")
  if not res['OK']:
    gLogger.error(res['Message'])
  else:
    gLogger.notice("Updated %s at %s to %s" % (mytag, mysite, "Probing"))
  
  res = getQueues(siteList = [mysite])
  if not res['OK']:
    gLogger.error(res['Message'])
    dexit(0)
  
  cetest = res['Value'][mysite].keys()[0]
  res = sw.updateCEStatus(mytag, cetest, "Valid")
  if not res['OK']:
    gLogger.error(res['Message'])
  else:
    gLogger.notice("Updated %s to %s at %s" %(mytag, "Valid", cetest))

  #try again now that at least one CE as a Valid tag
  res = sw.getSitesForTag(mytag)
  if not res['OK']:
    gLogger.error(res['Message'])
  else:
    gLogger.notice("Sites for tag: ", res['Value'])
  
  #Remove the association tag-site (mark as removed)
  res = sw.removeTagAtSite(mytag,mysite)
  if not res['OK']:
    gLogger.error(res['Message'])
  else:
    gLogger.notice("Removed tag from site (still in the DB)")
    
  #Remove tag association
  res = sw.cleanTagAtSite(mytag, mysite)
  if not res['OK']:
    gLogger.error(res['Message'])
  else:
    gLogger.notice("Entries linking %s to %s are removed from the DB" %(mytag, mysite))
    
  dexit(0)