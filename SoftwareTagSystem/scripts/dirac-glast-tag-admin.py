"""
The user interface script for tag handling. Idea is to provide 4 methods
dirac-glast-tag-admin addTag <tag>
dirac-glast-tag-admin removeTag <tag>
dirac-glast-tag-admin flagOkay <tag>
dirac-glast-tag-admin flagBad <tag>
including the --site= parameter, it will only act on the site 
"""
def setSpecialOption( optVal ):
    from DIRAC import S_OK
    global specialOptions
    option,value = optVal.split('=')
    specialOptions[option] = value
    return S_OK()

if __name__ == "__main__":
    from DIRAC.Core.Base import Script
    from GlastDIRAC.SoftwareTagSystem.Client import SoftwareTagClient
    specialOptions = {}
    Script.registerSwitch( "opt:", "options=", "Special option (currently supported: site", setSpecialOption)
        # thanks to Stephane for suggesting this fix!
    Script.parseCommandLine()
        
    args = Script.getPositionalArgs()
    if len(args)==0:
        raise S_ERROR("not allowed.")
    client = SoftwareTagClient()
    sites = []
    if 'site' in specialOptions:
        sites = [specialOptions['site']]
    else:
        sites = client.getSites()
    mode = args[0]
    if not mode in ['addTag','removeTag','flagOkay','flagBad']:
        raise S_ERROR("not supported")
    else:
        if len(args)<1:
            raise S_ERROR("Need arg 2.")
    tag = args[1]
    if mode == "addTag":
        for site in sites:
            client.addTagAtSite(tag,site)
    elif mode == "removeTag":
        for site in sites:
            client.removeTagAtSite(tag,site)
    elif mode.startswith("flag"):
        status = None
        if mode == "flagOkay":
            status = "Okay"
        else:
            status = "Bad"
        client.updateStatus(tag,site,status)
    