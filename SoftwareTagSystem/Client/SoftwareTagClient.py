"""
Client for the glast software tables
Exposes for free the methods 

getSitesForTag(tag)
getTagsAtSite(tag)
addTagAtSite(tag,site)
removeTagAtSite(tag,site)
getSites()
updateStatus(tag,site,status)

"""

from DIRAC.Core.Base.Client                               import Client

class SoftwareTagClient (Client):
  """ Client of the SoftwareTagHandler.
  """
  def __init__(self, **kwargs ):
    Client.__init__(self, **kwargs )
    self.setServer("SoftwareTag/SoftwareTag")
