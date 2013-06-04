#!/bin/env python

import os
from DIRAC import S_OK, S_ERROR
def ProbeSoftwareArea():
  """ Look into the shared area and report back to the SoftwareTag service
  """
  from DIRAC import gLogger, siteName

  site = siteName()
  
  from GlastDIRAC.SoftwareTagSystem.Client.SoftwareTagClient import SoftwareTagClient
  swtc = SoftwareTagClient()

  if not 'VO_GLAST_ORG_SW_DIR' in os.environ:
    #res = swtc.reportBadSite(site)
    #if not res['OK']:
    #  return S_ERROR("Failed to report Bad site, missing software area.")
    return S_ERROR("Missing VO_GLAST_ORG_SW_DIR environment variable")

  list_sw = os.listdir(os.environ['VO_GLAST_ORG_SW_DIR'])
  
  gLogger.notice("Found the following software directories:")
  message = None
  for item in list_sw:
    gLogger.notice("   %s"%item)
    #res = swtc.validatePresence(item, site)
    #if not res['OK']:
    #  message = "Failed to report back: %s" %res['Message']
  
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
    