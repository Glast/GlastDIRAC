"""
Service code for GLAST stuff
"""

__RCSID__ = " $Id: $ "

from DIRAC.Core.DISET.RequestHandler                    import RequestHandler
from GlastDIRAC.SoftwareTagSystem.DB.GlastAdditionnalInfoDB import GlastAdditionnalInfoDB #Fix path
from DIRAC import S_OK
from types import StringTypes, DictType

glastdb = False

def initializeSoftwareTagHandler( ServiceInfo ):
  global glastdb
  glastdb = GlastAdditionnalInfoDB()
  return S_OK()

class SoftwareTagHandler(RequestHandler):

  types_getSitesForTag = [StringTypes]
  def export_getSitesForTag(self, tag,status):
    return glastdb.getSitesForTag(tag,status=status)
  
  types_getTagsAtSite = [StringTypes]
  def export_getTagsAtSite(self, site,status):
    return glastdb.getTagsAtSite(site,status=status)
  
  types_addTagAtSite = [StringTypes, StringTypes]
  def export_addTagAtSite(self, tag, site):
    return glastdb.addTagAtSite( tag, site )
  
  types_removeTagAtSite = [StringTypes, StringTypes]
  def export_removeTagAtSite(self, tag, site):
    return glastdb.removeTagAtSite(tag, site)

  types_updateStatus = [StringTypes, StringTypes, StringTypes]
  def export_updateStatus(self, tag, site, status):
    return glastdb.updateStatus(tag, site, status)
  
  types_getSites = []
  def export_getSites(self):
    return glastdb.getSites()
 
  types_getTags = []
  def export_getTags(self):
    return glastdb.getTags()

  