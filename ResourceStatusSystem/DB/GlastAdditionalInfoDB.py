###########################################################################
# $HeadURL: $
###########################################################################

""" DB for GlastAdditionalInfoDB
Created 03/2013
@author: S. Poss (CERN)

"""
__RCSID__ = " $Id: $ "

from DIRAC                                                             import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB                                                import DB
from DIRAC.ConfigurationSystem.Client.Helpers.Resources                import getQueues
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVO
class GlastAdditionalInfoDB ( DB ):
  def __init__( self, maxQueueSize = 10 ):
    """ 
    """
    self.dbname = 'GlastAdditionalInfoDB'
    self.logger = gLogger.getSubLogger('GlastAdditionalInfoDB')
    DB.__init__( self, self.dbname, 'ResourceStatus/GlastAdditionalInfoDB', maxQueueSize  )
    self.fields = ["CEName","Status","Software_Tag"]
    self._createTables( { "SoftwareTags_has_Sites" :{"Fields":{"idRelation":"INT NOT NULL AUTO_INCREMENT",
                                                               "CEName":"VARCHAR(45) NOT NULL",
                                                               "Status":"ENUM('New','Installing','Valid','Bad','Removed') DEFAULT 'New'",
                                                               "Software_Tag":"VARCHAR(255) NOT NULL",
                                                               "LastUpdateTime":"DATETIME"},
                                                     "PrimaryKey" : ['idRelation'],
                                                     'Indexes' : { "Index":["idRelation","Software_Tag","CEName", 'Status']}
                                                     }             
                        }
                      )
    self.vo = getVO('glast.org')
    ##tags statuses: 
    self.tag_statuses = ['New','Installing','Valid','Bad','Removed']
    #State machine: New -> Probing -> Valid/Bad -> Removed -> Probing -> etc.    
    
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
    """ Check a given Property. Probably does not do what it's supposed to...
    """
    connection = self.__getConnection( connection )
    
    res = self.getFields("SoftwareTags_has_Sites", [ItemProperty], 
                         {ItemProperty : name},
                         conn = connection)#"SELECT Name FROM Sites WHERE Name='%s';" % (site)
    if not res['OK']:
        return S_ERROR("Could not get property %s with name %s" % (ItemProperty, name))
    if len(res['Value']):
        if not res['Value'][0][0]:
          return S_ERROR("Value for %s not found" % ItemProperty)
        return res
    else:
        return S_ERROR("Could not find any property %s with name %s" % (ItemProperty, name))
  
  def __getCESforSite(self, site):
    """ As the name suggests, get all the CEs for a given site
    """
    slist = None
    if site !='ALL':
      slist = [site]
    res = getQueues(siteList = slist, community = self.vo)
    if not res['OK']:
        return S_ERROR("Could not get CEs for site")
    if not res['Value']:
        return S_ERROR("No CEs for site %s" % site)
    ces  =[]
    for key, items in res['Value'].items():
        if site !='ALL':
            if key != site:
                continue
        ces.extend(items.keys())
    if not ces:
        return S_ERROR("No CEs for site %s" % site)
    return S_OK(ces)
  
  def __getSiteForCEs(self, ces):
    """ We want to get the site for a given CE because that's what the job expects
    """
    
    res = getQueues(community = self.vo)
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
  
  ##################################################################
  ## Public methods that will need to be exported to the service 
  def getSitesForTag(self, tag, status='Valid', connection = False):
    """ Get the Sites that have a given tag
    """
    if not status in self.tag_statuses:
        return S_ERROR("Status %s undefined" % status)
    
    res = self._checkProperty("Software_Tag", tag, self.__getConnection( connection ))
    if not res['OK']:
        gLogger.error("Tag undefined", res['Message'])
        return S_ERROR("Tag was not found")
    res = self.getFields("SoftwareTags_has_Sites", ["CEName"], 
                         {"Software_Tag": tag, "Status":status}, 
                         conn = self.__getConnection( connection ))
    if not res['OK']:
        return res
    if not res['Value']: #(because if no site is found, this returns ((,))
        return S_ERROR("No site for this tag %s with status %s was found" %(tag, status))
      
    ces = [row[0] for row in res['Value']]
    res = self.__getSiteForCEs(ces)
    if not res['OK']:
        return res
    return S_OK(res['Value'].keys())


  def getTagsAtSite(self, site, status='Valid',connection = False):
    """ Get the software tags that where registered at a given site
    """
    if not status in self.tag_statuses:
        return S_ERROR("Status %s undefined." % status)
    res = self.__getCESforSite(site)
    if not res['OK']:
        return res
    tags = []
    for ce in res['Value']:
        res = self._checkProperty("CEName", ce, self.__getConnection( connection ))
        if not res['OK']:
            gLogger.error("CE not in the DB:", res['Message'])
            continue

        res = self.getFields("SoftwareTags_has_Sites", ["Software_Tag"], 
                             {"CEName": ce, "Status":status}, 
                             conn = self.__getConnection( connection ))
        if not res['OK']:
          gLogger.error("Issue getting info", res['Message'])
          continue
        
        for row in res['Value']:
            tag = row[0]
            if not tag in tags:
                tags.append(tag)
    return S_OK(tags)
  
  def getTagsWithStatus(self, status, olderthan=None, connection = False):
    """ Get the different tags/site with a given status
    """
    if not status in self.tag_statuses:
        return S_ERROR("Invalid status %s." % status)
    
    conDict = {'Status':status}
    older = None
    if olderthan:
        import datetime
        older= (datetime.datetime.utcnow() - datetime.timedelta( seconds = olderthan ) ).strftime( '%Y-%m-%d %H:%M:%S' )
    res = self.getFields('SoftwareTags_has_Sites', ['Software_Tag','CEName'], 
                         conDict, older=older, timeStamp = 'LastUpdateTime',
                         conn = self.__getConnection( connection ))
    if not res['OK']:
        gLogger.error("No tag with status %s:" % status, res['Message'])
        return res
    tagsdict = {}
    for row in res['Value']:
        if not row[0] in tagsdict:
            tagsdict[row[0]] = []
        tagsdict[row[0]].append(row[1]) # this has e.g. {'someTag':[CE1,CE2]}
      
    return S_OK(tagsdict)
  
  def addTagAtSite(self, tag, site, connection = False):
    """ Register a tag at a site, with status=New
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
    ces = res['Value']
    res = self.getFields('SoftwareTags_has_Sites', ['Status', 'CEName'], {'Software_Tag':tag,'CEName':ces},  
                           conn = self.__getConnection( connection ))
    if not res['OK']:
        gLogger.error("Failed checking tag at site", res['Message'])
        return S_ERROR("Failed looking up existence of tag at site")

    ces_in_db = [row[1] for row in res['Value']]
    
    inserted  = {}
    for ce in ces:
        if ce not in ces_in_db:
            res = self.insertFields("SoftwareTags_has_Sites", 
                                    ['CEName', 'Software_Tag', 'LastUpdateTime'], 
                                    [ce, tag, "UTC_TIMESTAMP()"], 
                                    conn = self.__getConnection( connection ))
            if not res['OK']:
                self.log.error("Failed inserting new row:", res['Message'])
                continue
            if not tag in inserted:
                inserted[tag] = []    
            inserted[tag].append(ce)  
    return S_OK(inserted)
  
  def removeTagAtSite(self, tag, site, connection = False):
    """ Mark the tag at site as Removed. This allows to know that the tag is maybe still there
    """
    
    return self.updateStatus(tag, site, 'Removed', connection)
  
  def cleanTagAtSite(self, tag, site, connection = False):
    """ Remove the relation between tag and Site. Should be called only once
    cleanup is really done.
    """
    if not tag:
      return S_ERROR("Tag MUST be specified")
    res = self._checkProperty("Software_Tag", tag, self.__getConnection( connection ))
    if not res['OK']:
        gLogger.error("Tag not found", res['Message'])
        return S_ERROR("Tag was not found")
    res = self.__getCESforSite(site)
    if not res['OK']:
        return res
    successful = []
    failed = []
    for ce in res['Value']:
        res = self._checkProperty("CEName", ce, self.__getConnection( connection ))
        if not res['OK']:
            gLogger.error("CE not in DB!", res['Message'])
            continue
    
        
        res = self.deleteEntries("SoftwareTags_has_Sites",
                                 {"Software_Tag":tag, "CEName": ce}, 
                                 conn = self.__getConnection( connection ))
        if not res['OK']:
            failed.append(ce)
        else:
            successful.append(ce)
    return S_OK({"Successful":successful, "Failed":failed})
  
  def updateStatus(self, tag, site, status, connection = False):
      """ To interact with the status field """
      if not status in self.tag_statuses:
          return S_ERROR("Status %s undefined." % status)
      if not site:
          return S_ERROR("Site MUST be specified, even if 'ALL'")  
      if status == 'Installing' :
          return S_ERROR("Transition to Installing does not make sense for this method.")
      condDict = {}
      if tag:
        res = self._checkProperty("Software_Tag", tag, self.__getConnection( connection ))
        if not res['OK']:
            gLogger.error("Tag not found:", res['Message'])
            return S_ERROR("Tag was not found")
        condDict['Software_Tag'] = tag
       
      res = self.__getCESforSite(site)
      if not res['OK']:
          return res
      updatefields = ['Status', 'LastUpdateTime']
      updatestatus = [status, 'UTC_TIMESTAMP()']
      successful = []
      failed = []
      for ce in res['Value']:
          condDict['CEName'] = ce
          res = self.updateFields("SoftwareTags_has_Sites", 
                                  updatefields,
                                  updatestatus,
                                  condDict, 
                                  conn = self.__getConnection( connection ))
          if not res['OK']:
            failed.append(ce)
          else:
            successful.append(ce)
             
      return S_OK({'Successful':successful, 'Failed':failed})

  def updateCEStatus(self, tag, ce, status, connection = False):
    """ Update the tags at CE relations. Usually through the Agent of the jobs
    """
    if not status in self.tag_statuses:
        return S_ERROR("Status %s undefined." % status)
      
    res = self._checkProperty("CEName", ce, self.__getConnection( connection ))
    if not res['OK']:
      gLogger.error("CE not in DB!", res['Message'])
      return S_ERROR("CE not in DB!")
    
    condDict = {}
    if tag:
        res = self._checkProperty("Software_Tag", tag, self.__getConnection( connection ))
        if not res['OK']:
            gLogger.error("Tag not found:",res['Message'])
            return S_ERROR("Tag was not found")
        condDict["Software_Tag"]=tag
           
    condDict = {"CEName":ce}    

    updatefields = ['Status', 'LastUpdateTime']
    updatestatus = [status, 'UTC_TIMESTAMP()']
    

    res = self.updateFields("SoftwareTags_has_Sites", 
                            updatefields,
                            updatestatus,
                            condDict,
                            conn = self.__getConnection( connection ))
    return res

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
  
