#!/usr/bin/env python
""" Pipeline Submission Script 

Created 04/16/2012
@author: S. Zimmer (OKC/SU)

"""
from DIRAC.Core.Base import Script
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getQueues
from DIRAC.Core.Utilities.Grid import ldapsearchBDII
from DIRAC import gConfig, gLogger, exit as dexit
from DIRAC.Core.Security.ProxyInfo import getVOfromProxyGroup
from DIRAC.Core.Base import Script

def ldapCEs(vo): 
    # returns the list of CEs that are associated with the correct VO
    res = ldapsearchBDII( filt = "(&(objectClass=GlueCE)(GlueCEAccessControlBaseRule=VO:%s))"%vo, attr = "GlueCEUniqueID", host = None, base = None)
    celist = [res['Value'][i]['attr']['GlueCEUniqueID'].split(":")[0] for i,value in enumerate(res['Value']) if res['OK']]
    return celist

def ldapTag(ce,vo):
    # returns the list of tags that are associated with this CE
    res = ldapsearchBDII( filt = "(&(objectClass=GlueSubCluster)(GlueChunkKey=GlueClusterUniqueID=%s))"%ce, attr = "GlueHostApplicationSoftwareRuntimeEnvironment")
    tags = []
    if res['OK'] and len(res['Value'])!=0:
        tagdict = res['Value'][-1]
        if tagdict['attr'].has_key('GlueHostApplicationSoftwareRunTimeEnvironment'):
            tags = [tag for tag in tagdict['attr']['GlueHostApplicationSoftwareRunTimeEnvironment'] if vo in tag]
    return tags

def main(vo):
    # thanks to Stephane for suggesting this fix!  
    #res1 = gConfig.getSections( 'Resources/Sites/LCG/', listOrdered = True )
    res = getQueues()
    if not res['OK']:
        gLogger.error(res['Message'])
        gLogger.error("Cannot obtain Queues")
        dexit(1)
    sites = res['Value'].keys()
    values = [res['Value'][key].keys() for key in sites]
    sites_ce = dict(zip(sites,values))
    vo_ces = ldapCEs(vo)
    final_dict = {}
    for site in sites_ce:
        final_dict[site]={"Tags":[],"CE":[]}
        ces_current_site = sites_ce[site]
        for ce in ces_current_site:
            if ce in vo_ces:
                curr_ces = final_dict[site]["CE"]
                curr_ces.append(ce)
                final_dict[site].update({"Tags":ldapTag(ce,vo),"CE":curr_ces})
                #final_dict[site]={"Tags":ldapTag(ce,vo),"CE":[ce]}
    ret_dict = {}
    for key in final_dict:
        if len(final_dict[key]['CE'])!=0:
            ret_dict[key]=final_dict[key]
    return ret_dict

if __name__ == "__main__":
    Script.parseCommandLine()
    vo = "glast.org"
    res = getVOfromProxyGroup()
    if not res['OK']:
        gLogger.error(res['Message'])
        gLogger.error('Could not get VO from CS, assuming glast.org')
        dexit(1)
    else:
        gLogger.info(res)
        vo = res['Value']
    d = main(vo)
    for key in d:
        print('Name of DIRAC site %s\nName of CEs: %s'%(key,str(d[key]["CE"])))
        for tag in d[key]["Tags"]:
            print('\t%s'%tag)
        print('\n')
