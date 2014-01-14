#!/bin/env python
""" Probe Software Area 

Created 06/06/2013
@author: S. Poss (CERN)

"""
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
  items = directory.split("/")
  version = items[-1]
  package = items[-2]
  variant = items[-3]
  soft_os = items[-4]
  tag = "VO-glast.org-"+soft_os+"/"+variant+"/"+package+"/"+version
  return S_OK(tag)

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
  
  gLogger.notice("Found the following software directory:", base_sw_dir)
  message = None
  
  directory_list = []  
  for root, dirnames, files in os.walk(os.path.join(base_sw_dir,"glast/ground/releases")):
    if "bin" in dirnames:
      directory_list.append(root)
    

  for directory in directory_list:
    gLogger.notice("Decoding %s and tries to make a tag out of it" % directory)
    #Need mapping between Tag name and local software directory name
    res = getMappingTagFromDirectory(directory)
    if not res['OK']:
      gLogger.error("Failed finding relation between directory and Tag")
      continue
    tag = res['Value']
    gLogger.notice("Found tag ", tag)
    res = swtc.updateCEStatus(tag, ce, 'Valid')
    if not res['OK']:
      gLogger.error("Failed to report back: %s" %res['Message'])
      message = res['Message']
    else:
      gLogger.notice("Tag now Valid!")
  
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
    