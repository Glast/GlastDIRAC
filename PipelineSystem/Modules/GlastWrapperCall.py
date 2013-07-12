'''
Created on Jun 8, 2013

@author: stephane
'''
import DIRAC

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
import os, sys, shutil

class GlastWrapperCall(object):
    '''
    classdocs
    '''


    def __init__(self):
        '''
        Constructor
        '''
        self.log = gLogger.getSubLogger("WrapperCall")
        self.ops = Operations("glast.org")
        self.stdError = ''
        
    def getVariables(self):
      """ Resolve the step variables
      """ 
      if 'logFile' in self.step_commons:
          self.logFile = self.step_commons['logFile']
      else:
          return S_ERROR("Missing log file definition")
      return S_OK()
    
    def getWrapperLocation(self):
        """ Discover and check existence of the wrapper
        """
        bpath = DIRAC.rootPath
        fname = self.ops.getValue("Pipeline/Wrapper","GlastDIRAC/PipelineSystem/scripts/dirac-glast-pipeline-wrapper.sh")
        location = os.path.join(bpath, fname)
        if not os.path.isfile(location):
            return S_ERROR("Could not find wrapper at %s"%location)
        return S_OK(location)
      
    def execute(self):
        """ This is where magic happens
        """
        res = self.getVariables()
        if not res['OK']:
            return res
          
        #In the future, you'll want to put the entire wrapper here, but because I'm lazy, we will just call it
        
        res = self.getWrapperLocation()  
        if not res['OK']:
          return res

        loc = res['Value']
        exec_name = os.path.basename(loc)
        try:
          shutil.copy(loc, os.path.join(".",exec_name))
        except:
          return S_ERROR("Could not copy the executable to run directory")
        os.chmod(exec_name, 0755) #executable for all
        comm = 'bash "./%s"' % exec_name
        self.log.info("Will execute", comm)
        
        res = shellCall(0, comm, self.callBack)
        if not res['OK']:
            return res
        resultTuple = res['Value']
        status = resultTuple[0]
        if status:
          self.log.error("Command exited with status %s" % status)
          return S_ERROR("Failed with status %s" % status)
        return S_OK()
      
    def callBack(self, fd, message):
        sys.stdout.flush()
        print message
        if self.logFile:
          log = open(self.logFile, 'a')
          log.write(message+'\n')
          log.close()
        else:
          self.log.error("Application Log file not defined")
        if fd == 1:
          self.stdError += message
