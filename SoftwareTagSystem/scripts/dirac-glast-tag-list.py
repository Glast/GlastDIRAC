#!/bin/env python
"""
The user interface script for tag handling. Idea is to provide 4 methods
dirac-glast-tag-list <site> <tag> <status>
dirac-glast-tag-list will list all sites with all tags.
including the --site= parameter, it will only act on the site 
"""
from DIRAC.Core.Base import Script
from DIRAC import S_OK


class Params(object):
    def __init__(self):
        self.site = []
        self.tag = None
        self.status = "OK"
    def getTag(self):
        return self.tag
    def getStatus(self):
        return self.status
    def getSite(self):
        return self.site
    def setSite(self,opt):
        self.site = opt.split(',')
        return S_OK()
    def setTag(self,opt):
        self.tag = opt.split(',')
        return S_OK()
    def setStatus(self,opt):
        self.status = opt
        return S_OK()
    def registerswitch(self):
        Script.registerSwitch("", 'tag=', 'tag(s), comma separated', self.setTag)
        Script.registerSwitch("", 'site=', 'site(s) to consider, comma separated', self.setSite)

 
if __name__ == "__main__":
    cli_p = Params()
    cli_p.registerswitch()
    # thanks to Stephane for suggesting this fix!
    Script.parseCommandLine()
    from DIRAC import gLogger, exit as dexit
    from DIRAC.Core.Security.ProxyInfo import getProxyInfo
    res = getProxyInfo()
    if not res['OK']:
        gLogger.error(res["Message"])
        gLogger.error("Bad proxy, or no proxy")
        dexit(1)
    group = res['Value']['group']
    #if not group == 'glastsgm_user' or group == "glast_user" :
    #    gLogger.error(res["Message"])
    #    gLogger.error('Invalid group, cannot proceed')
    #    dexit(1)

    from GlastDIRAC.SoftwareTagSystem.Client import SoftwareTagClient
    client = SoftwareTagClient.SoftwareTagClient()
    sites = None
    statii = ["OK","BAD"]
    status = "OK"
    if not cli_p.getStatus() is None:
        status = cli_p.getStatus()
    if not status in statii:
        gLogger.error("Statii allowed are %s"%str(statii))
        gLogger.error("exiting.")
        dexit(1)

    if cli_p.getSite() is None:
        sites = client.getEntriesFromField("SiteName")
    else:
        sites = cli_p.getSite()
    gLogger.debug("SITES: %s"%str(sites))
    for site in sites:
        print "**** %s ****"%site
        res = client.getTagsAtSite(site,status=status)
        #dexit(0)
    
    if cli_p.getTag() is None:
        tags = client.getEntriesFromField("Software_Tag")       
    else:
        tags = cli_p.getTag()
    gLogger.debug("TAGS: %s"%str(tags))
    for tag in tags:
        print "*TAG: %s*"%tag
        print client.getSitesForTag(tag,status=status)
        #dexit(0)
