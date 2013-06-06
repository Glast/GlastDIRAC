'''
Created on Jun 6, 2013

@author: S. Zimmer (OKC/SU)
'''
from DIRAC import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Resources                import getQueues

def getSiteForCEs(ces,vo="glast.org"):
    """ We want to get the site for a given CE because that's what the job expects
    copy of private DB method.
    """
    
    res = getQueues(community = vo)
    if not res['OK']:
        return S_ERROR("Could not get site for CE")
    sitedict = res['Value']
    
    final_sdict = {}
    for site, s_ces in sitedict.items():
        for ce in ces:
            if ce in s_ces:
                if not site in final_sdict:
                    final_sdict[site] = []
                final_sdict[site].append(ce)
    
    return S_OK(final_sdict)