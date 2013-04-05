"""
Service code for GLAST stuff
"""

__RCSID__ = " $Id: $ "

from DIRAC.Core.DISET.RequestHandler                    import RequestHandler
from GlastDIRAC.SoftwareTagSystem.DB.GlastAdditionnalInfoDB import GlastAdditionnalInfoDB #Fix path
from DIRAC import S_OK
from types import StringTypes, DictType

glastdb = False

def initializeGlastHandler( ServiceInfo ):
  global glastdb
  glastdb = GlastAdditionnalInfoDB()
  return S_OK()

class SoftwareTagHandler(RequestHandler):

  types_getSitesForTag = [StringTypes]
  def export_getSitesForTag(self, tag):
    return glastdb.getSitesForTag(tag)
  
  types_getTagsAtSite = [StringTypes]
  def export_getTagsAtSite(self, site):
    return glastdb.getTagsAtSite(site)
  
  types_addTagAtSite = [StringTypes, StringTypes]
  def export_addTagAtSite(self, tag, site):
    return glastdb.addTagAtSite( tag, site )
  
  types_removeTagAtSite = [StringTypes, StringTypes]
  def export_removeTagAtSite(self, tag, site):
    return glastdb.removeTagAtSite(tag, site)
  