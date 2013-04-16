###########################################################################
# $HeadURL: $
###########################################################################

""" DB for GlastAdditionalInfoDB
"""
__RCSID__ = " $Id: $ "

from DIRAC                                                             import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB                                                import DB
#from DIRAC.ConfigurationSystem.Client.Helpers.Operations            import Operations

class GlastAdditionnalInfoDB ( DB ):
  def __init__( self, maxQueueSize = 10 ):
    """ 
    """
    #self.ops = Operations()
    self.dbname = 'GlastAdditionnalInfoDB'
    self.logger = gLogger.getSubLogger('GlastAdditionnalInfoDB')
    DB.__init__( self, self.dbname, 'SoftwareTag/GlastAdditionnalInfoDB', maxQueueSize  )
    self.fields = ["SiteName","Status","Software_Tag"]
    self._createTables( { "SoftwareTags_has_Sites" :{"Fields":{"idRelation":"INT NOT NULL AUTO_INCREMENT",
                                                               "SiteName":"VARCHAR(45) NOT NULL",
                                                               "Status":"ENUM('OK','BAD') DEFAULT 'OK'",
                                                               "Software_Tag":"VARCHAR(60) NOT NULL"},
                                                     "PrimaryKey" : ['idRelation'],
                                                     'Indexes' : { "Index":["idRelation","Software_Tag","SiteName"]}
                                                     }             
                        }
                      )
  #####################################################################
  # Private methods

  def __getConnection( self, connection ):
    if connection:
      return connection
    res = self._getConnection()
    if res['OK']:
      return res['Value']
    gLogger.warn( "Failed to get MySQL connection", res['Message'] )
    return connection
  
  def _checkProperty(self, ItemProperty, name, connection = False ):
    """ Check a given site.
    """
    connection = self.__getConnection( connection )
    
    res = self.getFields("SoftwareTags_has_Sites", ItemProperty, {ItemProperty : name},
                         conn = connection)#"SELECT Name FROM Sites WHERE Name='%s';" % (site)
    if not res['OK']:
      return S_ERROR("Could not get property %s with name %s" % (ItemProperty, name))
    if len(res['Value']):
      return res
    else:
      return S_ERROR("Could not find any property %s with name %s" % (ItemProperty, name))
  
  
  
  ##################################################################
  ## Public methods that will need to be exported to the service 
  def getSitesForTag(self, tag, status='OK', connection = False):
    """ Get the Sites that have a given tag
    """
    res = self._checkProperty("Software_Tag", tag, self.__getConnection( connection ))
    if not res['OK']:
      return S_ERROR("Site was not found")
    res = self.getFields("SoftwareTags_has_Sites", "SiteName", {"Software_Tag": tag},{"Status":status}, 
                         conn = self.__getConnection( connection ))
    return res


  def getTagsAtSite(self, site, status='OK',connection = False):
    """ Get the software tags that where registered at a given site
    """
    res = self._checkProperty("SiteName", site, self.__getConnection( connection ))
    if not res['OK']:
      return S_ERROR("Site was not found")
    res = self.getFields("SoftwareTags_has_Sites", "Software_Tag", {"SiteName": site},{"Status":status}, 
                         conn = self.__getConnection( connection ))
    return res
  
  def addTagAtSite(self, tag, site, connection = False):
    """ Register a tag at a site, with status=Okay
    """
    #res = self._checkTag(tag, connection)
    #if not res['OK']:
    #  return S_ERROR("Tag was not found")
    #res = self._checkSite(site, connection)
    #if not res['OK']:
    #  return S_ERROR("Site was not found")
    res = self.insertFields("SoftwareTags_has_Sites", ['SiteName', 'Software_Tag'], [site, tag], 
                            conn = self.__getConnection( connection ))
    return res
  
  def removeTagAtSite(self, tag, site, connection = False):
    """ If a tag is removed, it needs to be unregistered
    """
    res = self._checkProperty("Software_Tag",tag, self.__getConnection( connection ))
    if not res['OK']:
      return S_ERROR("Tag was not found")
    res = self._checkProperty("Sites_Name", site, self.__getConnection( connection ))
    if not res['OK']:
      return S_ERROR("Site was not found")
    
    
    res = self.deleteEntries("SoftwareTags_has_Sites",
                             {"Software_Tag":tag, "SiteName": site}, 
                             conn = self.__getConnection( connection ))
    return res
  
  def updateStatus(self,tag,site,status,connect=False):
      """ to interact with the status field """
      res = self.updateFields("SoftwareTags_has_Sites", ['SiteName','Software_Tag','Status'],[site,tag,status])
      if not res['OK']:
          return S_ERROR("Error updating Status")

  def getEntriesFromField(self,field=None,connect=False):
      if not field in self.fields:
          return S_ERROR("Could not find field %s in DB"%str(field))
      res = self.getFields("SoftwareTags_has_Sites",[field],conn=connect)
      if not res['OK']:
          return S_ERROR(res['Message'])
      else:
          klist = []
          klist = [value for value in res['Value'] if not value in klist]
          return S_OK(klist)
  