""" This agent does not do much at first. It's task is eventually to submit and monitor
software monitoring jobs
"""
from DIRAC.Core.Base.AgentModule                         import AgentModule
from DIRAC import S_OK
from GlastDIRAC.ResourceStatusSystem.Client.SoftwareTagClient import SoftwareTagClient

class SoftwareMonitorAgent(AgentModule):
  """ This agent picks up "New" tags and submits jobs and those that
  are OK will report back to the service directly. For now it enforces the 
  transition from New to Probing to Valid.
  Also resets the tags that have been Probing for too long to New
  """
  
  def initialize(self):
    """ Initialize the agent.
    """
    self.am_setOption( "PollingTime", 86400 ) #Once a day is enough
    
    self.swtc = SoftwareTagClient()
    self.submitjobs = self.am_getOption( 'SubmitJobs', False )
    if self.submitjobs:
      self.log.info("Will submit probe jobs to validate the software tags")
    else:
      self.log.info("Will mark as Valid all 'New' tags directly.")
      
    self.delay = self.am_getOption("Delay", 86400)
    self.log.info("Will reset to 'New' the tasks that have been 'Probing' for %s seconds" % self.delay)
    self.am_setOption( 'shifterProxy', 'SoftwareManager' ) 
    #Needs to be able to submit job for that VO
    
    return S_OK()
  
  def execute(self):
    """ Get all New tags, mark them as Installing. Old Installing tags are reset to New 
    """
    res = self.swtc.getTagsWithStatus("New")
    if not res['OK']:
      return res
    if not res['Value']:
      self.log.info("No 'New' tags to consider")
      
    for tag, ces in res['Value'].items():
      for ce in ces:
        res = self.swtc.updateCEStatus(tag, ce, 'Installing')
        if not res['OK']:
          self.log.error(res['Message'])
          continue
        res = None
      
        if self.submitjobs:
          res = self.submitProbeJobs(ce)
        else:
          res = self.swtc.updateCEStatus(tag, ce, 'Valid')
        
        if not res['OK']:
          self.log.error(res['Message'])
     
    ##Also, reset to New tags that were in Probing for too long.
    res = self.swtc.getTagsWithStatus("Installing",olderthan=self.delay)
    if not res['OK']:
      self.log.error("Failed to get old 'Installing' tags")
    else:
      if not res['Value']:
        self.log.info("No 'Installing' tags to reset")
        
      for tag, ces in res['Value'].items():
        for ce in ces:
          res = self.swtc.updateCEStatus(tag, ce, 'New')
          if not res['OK']:
            self.log.error(res['Message'])
            continue
    return S_OK()
  
  def submitProbeJobs(self, ce):
    """ Submit some jobs to the CEs
    """
    
    #need credentials, should be there since the initialize
    
    from DIRAC.Interfaces.API.Dirac import Dirac
    d = Dirac()
    from DIRAC.Interfaces.API.Job import Job
    
    from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
    
    ops = Operations()
    scriptname = ops.getValue("ResourceStatus/SofwareManagementScript","ProbeSoftwareArea.py")
    
    j = Job()
    j.setDestinationCE(ce)
    j.setCPUTime(1000)
    j.setName("Probe %s" % ce)
    j.setJobGroup("SoftwareProbe")
    j.setExecutable("$DIRAC/GlastDIRAC/ResourceStatusSystem/Client/%s" % scriptname, logFile='SoftwareProbe.log')
    j.setOutputSandbox('*.log')
    res = d.submit(j)
    if not res['OK']:
      return res
      
    return S_OK()