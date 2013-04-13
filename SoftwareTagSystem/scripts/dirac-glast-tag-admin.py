#!/bin/env python
"""
The user interface script for tag handling. Idea is to provide 4 methods
dirac-glast-tag-admin addTag <tag>
dirac-glast-tag-admin removeTag <tag>
dirac-glast-tag-admin flagOkay <tag>
dirac-glast-tag-admin flagBad <tag>
including the --site= parameter, it will only act on the site 
"""
from DIRAC.Core.Base import Script
from DIRAC import S_OK

statuslist = ["add",'remove','flagOK','flagBad']

class Params(object):
    def __init__(self):
        self.site = []
        self.tag = ''
        self.status = ""
    def setSite(self,opt):
        self.site = opt.split(',')
        return S_OK()
    def setTag(self,opt):
        self.tag = opt
        return S_OK()
    def setStatus(self,opt):
        if not opt in statuslist:
            return S_ERROR("Status must be among %s" % statuslist)
        self.status = opt
        return S_OK()
    def registerswitch(self):
        Script.registerSwitch("", 'tag=', 'tag name', self.setTag)
        Script.registerSwitch("", 'site=', 'site(s) to consider, comma separated', self.setSite)
        Script.registerSwitch("",'action=','which action to perform, among %s'%statuslist,self.setStatus)
        Script.setUsageMessage("--tag and --action are mandatory")
    
if __name__ == "__main__":
    cli_p = Params()
    cli_p.registerswitch()
    # thanks to Stephane for suggesting this fix!
    Script.parseCommandLine()

    from DIRAC import gLogger, exit as dexit
    from DIRAC.Core.Security.ProxyInfo import getProxyInfo
    res = getProxyInfo()
    if not res['OK']:
        gLogger.error("Bad proxy, or no proxy")
        dexit(1)
    group = res['Value']['group']
    if not group == 'SomeDIRACgroup' :
        gLogger.error('Invalid group, cannot proceed')
        dexit(1)

    from GlastDIRAC.SoftwareTagSystem.Client import SoftwareTagClient
    
    if not cli_p.tag or not cli_p.status:
        Script.showHelp()
        dexit(1)
    client = SoftwareTagClient()
    sites = []
    if cli_p.site:
        sites = cli_p.site
    else:
        sites = client.getSites()
    mode = cli_p.status
    
    tag = cli_p.tag
    if mode == "add":
        for site in sites:
            client.addTagAtSite(tag,site)
    elif mode == "remove":
        for site in sites:
            client.removeTagAtSite(tag,site)
    elif mode.startswith("flag"):
        status = None
        if mode == "flagOK":
            status = "Okay"
        else:
            status = "Bad"
        client.updateStatus(tag,site,status)

    dexit(0)
    
