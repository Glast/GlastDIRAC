""" This agent does not do much at first. It's task is eventually to submit and monitor
software monitoring jobs
"""
from DIRAC.Core.Base.AgentModule                         import AgentModule
from DIRAC import S_OK
from GlastDIRAC.SoftwareTagSystem.Client.SoftwareTagClient import SoftwareTagClient

class SoftwareMonitorAgent(AgentModule):
  """ This agent picks up "New" tags and submits jobs (Will eventually) and those that
  are OK will report back to the service directly. For now it enforces the 
  transition from New to Probing to Valid.
  """
  
  def initialize(self):
    """ Initialize the agent.
    """
    self.am_setOption( "PollingTime", 86400 )
    
    self.swtc = SoftwareTagClient()
    self.submitjobs = self.am_getOption( 'SubmitJobs', False )
    
    return S_OK()
  
  def execute(self):
    """ Get all New tags, mark them as Probing. 
    """
    res = self.swtc.getTagsWithStatus("New")
    if not res['OK']:
      return res
    
    for tag, ces in res['Value'].items():
      for ce in ces:
        res = self.swtc.updateCEStatus(tag, ce, 'Probing')
        if not res['OK']:
          self.log.error(res['Message'])
          continue
        res = None
      
        if self.submitjobs:
          res = self.submitProbeJobs(ce)
        else:
          res = self.swtc.updateStatus(tag, ces, 'Valid')
        
        if not res['OK']:
          self.log.error(res['Message'])
      
    return S_OK()
  
  def submitProbeJobs(self, ce):
    """ Submit some jobs to the CEs
    """
    
    from DIRAC.Interfaces.API.Dirac import Dirac
    d = Dirac()
    from DIRAC.Interfaces.API.Job import Job
    
    j = Job()
    j.setDestinationCE(ce)
    j.setCPUTime(1000)
    j.setName("Probe %s" % ce)
    j.setJobGroup("SoftwareProbe")
    j.setExecutable("$DIRAC/GlastDIRAC/SoftwareTagSystem/Client/ProbeSoftwareArea.py", logFile='SoftwareProbe.log')
    j.setOutputSandbox('*.log')
    res = d.submit(j)
    if not res['OK']:
      return res
      
    return S_OK()