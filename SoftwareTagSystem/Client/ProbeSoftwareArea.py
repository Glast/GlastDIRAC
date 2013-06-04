#!/bin/env python

import os
from DIRAC import S_OK, S_ERROR

def getMappingTagToDirectory(tag):
  return S_OK("")

def getMappingTagFromDirectory(directory):
  return S_OK("")

def ProbeSoftwareArea():
  """ Look into the shared area and report back to the SoftwareTag service
  """
  from DIRAC import gLogger, gConfig

  #site = siteName()
  ce = gConfig.getValue('/LocalSite/GridCE', '')
  if not ce:
    return S_ERROR("CE undefined, cannot proceed")
  
  from GlastDIRAC.SoftwareTagSystem.Client.SoftwareTagClient import SoftwareTagClient
  swtc = SoftwareTagClient()

  if not 'VO_GLAST_ORG_SW_DIR' in os.environ:
    res = swtc.updateCEStatus("", ce, "Bad")
    if not res['OK']:
      return S_ERROR("Failed to report Bad site, missing software area.")
    return S_ERROR("Missing VO_GLAST_ORG_SW_DIR environment variable")

  list_sw = os.listdir(os.environ['VO_GLAST_ORG_SW_DIR'])
  
  gLogger.notice("Found the following software directories:")
  message = None
  for item in list_sw:
    gLogger.notice("   %s"%item)
    #Need mapping between Tag name and local software directory name
    res = getMappingTagFromDirectory(item)
    if not res['OK']:
      gLogger.error("Failed finding relation between directory and Tag")
      continue
    
    res = swtc.updateCEStatus(res['Value'], ce, 'Valid')
    if not res['OK']:
      message = "Failed to report back: %s" %res['Message']
  
  if message:
    return S_ERROR(message)
  return S_OK()

if __name__ == '__main__':
  from DIRAC.Core.Base import Script
  Script.parseCommandLine()
  
  from DIRAC import gLogger, exit as dexit
  
  res = ProbeSoftwareArea()
  if not res['OK']:
    gLogger.error(res['Message'])
    dexit(1)
  
  dexit(0)
    