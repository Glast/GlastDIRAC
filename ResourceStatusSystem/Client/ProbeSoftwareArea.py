#!/bin/env python

import os
from DIRAC import S_OK, S_ERROR

def getMappingTagToDirectory(tag):
  """ Returns the directory given a tag name
  """
  #tags are VO.glast.org-OS/FLAOUR/TAG, and in the SW dir it's the same
  tag = tag.replace("VO.glast.org-","")
  items = tag.split("/")
  if len(items)>3:
    return S_ERROR("Bad tag structure")
  soft_os = items[0]
  variant = items[1]
  package = items[2]
  version = items[3]
  directory = os.path.join(["glast/ground/releases",soft_os,variant,package,version])
  return S_OK(directory)

def getMappingTagFromDirectory(directory):
  """ Returns the tag name given a directory name
  """
  
  return S_OK("")

def ProbeSoftwareArea():
  """ Look into the shared area and report back to the SoftwareTag service
  """
  from DIRAC import gLogger, gConfig

  #site = siteName()
  ce = gConfig.getValue('/LocalSite/GridCE', '')
  if not ce:
    return S_ERROR("CE undefined, cannot proceed")
  
  from GlastDIRAC.ResourceStatusSystem.Client.SoftwareTagClient import SoftwareTagClient
  swtc = SoftwareTagClient()

  if not 'VO_GLAST_ORG_SW_DIR' in os.environ:
    res = swtc.updateCEStatus("", ce, "Bad")
    if not res['OK']:
      return S_ERROR("Failed to report Bad site, missing software area.")
    return S_ERROR("Missing VO_GLAST_ORG_SW_DIR environment variable")

  base_sw_dir = os.environ['VO_GLAST_ORG_SW_DIR']
  list_sw = os.listdir(base_sw_dir)
  
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
      gLogger.error("Failed to report back: %s" %res['Message'])
      message = res['Message']
  
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
    