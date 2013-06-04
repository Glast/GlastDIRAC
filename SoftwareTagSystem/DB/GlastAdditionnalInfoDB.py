###########################################################################
# $HeadURL: $
###########################################################################

""" DB for GlastAdditionalInfoDB
"""
__RCSID__ = " $Id: $ "

from DIRAC                                                             import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB                                                import DB
#from DIRAC.ConfigurationSystem.Client.Helpers.Operations            import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getQueues
class GlastAdditionnalInfoDB ( DB ):
  def __init__( self, maxQueueSize = 10 ):
    """ 
    """
    #self.ops = Operations()
    self.dbname = 'GlastAdditionnalInfoDB'
    self.logger = gLogger.getSubLogger('GlastAdditionnalInfoDB')
    DB.__init__( self, self.dbname, 'SoftwareTag/GlastAdditionnalInfoDB', maxQueueSize  )
    self.fields = ["CEName","Status","Software_Tag"]
    self._createTables( { "SoftwareTags_has_Sites" :{"Fields":{"idRelation":"INT NOT NULL AUTO_INCREMENT",
                                                               "CEName":"VARCHAR(45) NOT NULL",
                                                               "Status":"ENUM('OK','BAD') DEFAULT 'OK'",
                                                               "Software_Tag":"VARCHAR(60) NOT NULL"},
                                                     "PrimaryKey" : ['idRelation'],
                                                     'Indexes' : { "Index":["idRelation","Software_Tag","CEName"]}
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
    
    res = self.getFields("SoftwareTags_has_Sites", ItemProperty, 
                         {ItemProperty : name},
                         conn = connection)#"SELECT Name FROM Sites WHERE Name='%s';" % (site)
    if not res['OK']:
      return S_ERROR("Could not get property %s with name %s" % (ItemProperty, name))
    if len(res['Value']):
      return res
    else:
      return S_ERROR("Could not find any property %s with name %s" % (ItemProperty, name))
  
  def __getCESforSite(self, site):
    """ As the name suggests, get all the CEs for a given site
    """
    res = getQueues(siteList = [site])
    if not res['OK']:
      return S_ERROR("Could not get CEs for site")
    if not res['Value']:
      return S_ERROR("No CEs for site %s" % site)
    
    ces = res['Value'][site].keys()
    if not ces:
      return S_ERROR("No CEs for site %s" % site)
    return S_OK(ces)
  
  ##################################################################
  ## Public methods that will need to be exported to the service 
  def getSitesForTag(self, tag, status='OK', connection = False):
    """ Get the Sites that have a given tag
    """
    res = self._checkProperty("Software_Tag", tag, self.__getConnection( connection ))
    if not res['OK']:
      return S_ERROR("Tag was not found")
    res = self.getFields("SoftwareTags_has_Sites", "CEName", 
                         {"Software_Tag": tag}, 
                         {"Status":status}, 
                         conn = self.__getConnection( connection ))
    ces = []
    for row in res['Value']:
      ces.append(row[0])
    return S_OK(ces)


  def getTagsAtSite(self, site, status='OK',connection = False):
    """ Get the software tags that where registered at a given site
    """
    res = self.__getCESforSite(site)
    if not res['OK']:
      return res
    tags = []
    for ce in res['Value']:
      res = self._checkProperty("CEName", ce, self.__getConnection( connection ))
      if not res['OK']:
        gLogger.error("CE not in the DB:", ce)
        continue
    
      res = self.getFields("SoftwareTags_has_Sites", "Software_Tag", 
                           {"CEName": ce}, {"Status":status}, 
                           conn = self.__getConnection( connection ))
      for row in res['Value']:
        tag = row[0]
        if not tag in tags:
          tags.append(tag)
    return S_OK(tags)
  
  def addTagAtSite(self, tag, site, connection = False):
    """ Register a tag at a site, with status=Okay
    """
    #res = self._checkTag(tag, connection)
    #if not res['OK']:
    #  return S_ERROR("Tag was not found")
    #res = self._checkSite(site, connection)
    #if not res['OK']:
    #  return S_ERROR("Site was not found")
    res = self.__getCESforSite(site)
    if not res['OK']:
      return res
    for ce in res['Value']:
      res = self.insertFields("SoftwareTags_has_Sites", 
                              ['CEName', 'Software_Tag'], 
                              [ce, tag], 
                              conn = self.__getConnection( connection ))
    return res
  
  def removeTagAtSite(self, tag, site, connection = False):
    """ If a tag is removed, it needs to be unregistered
    """
    res = self._checkProperty("Software_Tag", tag, self.__getConnection( connection ))
    if not res['OK']:
      return S_ERROR("Tag was not found")
    res = self.__getCESforSite(site)
    if not res['OK']:
      return res
    for ce in res['Value']:
      res = self._checkProperty("CEName", ce, self.__getConnection( connection ))
      if not res['OK']:
        return S_ERROR("Site was not found")
    
    
      res = self.deleteEntries("SoftwareTags_has_Sites",
                               {"Software_Tag":tag, "CEName": ce}, 
                               conn = self.__getConnection( connection ))
    return res
  
  def updateStatus(self, tag, site, status, connection = False):
      """ to interact with the status field """
      res = self.__getCESforSite(site)
      if not res['OK']:
        return res
      for ce in res['Value']:
        res = self.updateFields("SoftwareTags_has_Sites", ['CEName',
                                                           'Software_Tag','Status'],
                                [ce, tag, status], 
                                conn = self.__getConnection( connection ))
      if not res['OK']:
          return S_ERROR("Error updating Status")

  def getEntriesFromField(self, field = None, connection = False):
      """ Get all entries for a given field: allows DB dump
      """
      if not field in self.fields:
          return S_ERROR("Could not find field %s in DB" % str(field))
      res = self._checkProperty("Software_Tag", field, connection)
      
      if not res['OK']:
          return S_ERROR(res['Message'])
      
      klist  =[]
      klist = [value[0] for value in res['Value'] if not value[0] in klist]
      return S_OK(klist)
  