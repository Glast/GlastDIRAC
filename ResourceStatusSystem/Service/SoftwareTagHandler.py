"""
Service code for GLAST stuff
Created 03/2013

@author: S. Poss (CERN)
"""

__RCSID__ = " $Id: $ "

from DIRAC.Core.DISET.RequestHandler                    import RequestHandler
from GlastDIRAC.ResourceStatusSystem.DB.GlastAdditionalInfoDB import GlastAdditionalInfoDB
from DIRAC import S_OK
from types import StringTypes

glastdb = False

def initializeSoftwareTagHandler( ServiceInfo ):
  global glastdb
  glastdb = GlastAdditionalInfoDB()
  return S_OK()

class SoftwareTagHandler(RequestHandler):

  types_getSitesForTag = [StringTypes]
  def export_getSitesForTag(self, tag, status='Valid'):
    """ Get the sites that have the Tag
    Returns currently the list of CEs
    """
    return glastdb.getSitesForTag(tag, status=status)
  
  types_getTagsAtSite = [StringTypes]
  def export_getTagsAtSite(self, site, status='Valid'):
    """ Get the list of tags at a given Site. Goes through all CEs of the site
    """
    return glastdb.getTagsAtSite(site, status=status)
  
  types_addTagAtSite = [StringTypes, StringTypes]
  def export_addTagAtSite(self, tag, site):
    """ Add a new tag at the site.
    """
    return glastdb.addTagAtSite( tag, site )
  
  types_getTagsWithStatus = [StringTypes]
  def export_getTagsWithStatus(self, status, olderthan= None):
    """ Get all tags:celist that have the given status. Can select with olderthan
    in seconds
    """
    return glastdb.getTagsWithStatus(status, olderthan)
  
  types_cleanTagAtSite = [StringTypes, StringTypes]
  def export_cleanTagAtSite(self, tag, site):
    """ Remove from the DB! the relations between the tag and all CEs at that site.
    """
    return glastdb.cleanTagAtSite(tag, site)
  
  types_removeTagAtSite = [StringTypes, StringTypes]
  def export_removeTagAtSite(self, tag, site):
    """ Mark tag as removed at all CEs of the site
    """
    return glastdb.removeTagAtSite(tag, site)

  types_updateStatus = [StringTypes, StringTypes, StringTypes]
  def export_updateStatus(self, tag, site, status):
    """ Update ALL the relations' statuses between tag and site's CEs
    """
    return glastdb.updateStatus(tag, site, status)
  
  types_updateCEStatus = [StringTypes, StringTypes, StringTypes]
  def export_updateCEStatus(self, tag, ce, status):
    """ Update individual status of relation between CE and tag
    Mostly called when running the ProbeSoftwareAgent
    """
    return glastdb.updateCEStatus(tag, ce, status)
  
  types_getEntriesFromField = [StringTypes]
  def export_getEntriesFromField(self, field):
    """ Dump the DB for a given field.
    """
    return glastdb.getEntriesFromField(field)
