'''
Created on Jun 8, 2013

@author: stephane
'''

from DIRAC.Interfaces.API.Job import Job
from DIRAC import S_OK
from DIRAC.Core.Workflow.Module import ModuleDefinition
from DIRAC.Core.Workflow.Parameter import Parameter
from DIRAC.Core.Workflow.Step import StepDefinition

class GlastJob(Job):
    '''
    classdocs
    '''


    def __init__(self,script = None, stdout = 'std.out', stderr = 'std.err'):
        '''
        Constructor
        '''
        super(GlastJob,self).__init__(script, stdout, stderr)
        
    def addWrapper(self, logFile = ''):
        """ Overload the DIRAC.Job.setExecutable
        """
        logFile = str(logFile)
        stepDefn = 'WrapperStep'
        stepName = 'RunWrapperStep'

        moduleName = 'GlastWrapperCall'
        module = ModuleDefinition( moduleName )
        module.setDescription( 'The utility that calls the pipeline_wrapper.' )
        body = 'from GlastDIRAC.PipelineSystem.Modules.GlastWrapperCall import GlastWrapperCall\n'
        module.setBody( body )
        # Create Step definition
        step = StepDefinition( stepDefn )
        step.addModule( module )
        moduleInstance = step.createModuleInstance( 'GlastWrapperCall', stepDefn )
        # Define step parameters
        step.addParameter( Parameter( "logFile", "", "string", "", "", False, False, 'Log file name' ) )
        self.addToOutputSandbox.append( logFile )
        self.workflow.addStep( step )

        # Define Step and its variables  
        stepInstance = self.workflow.createStepInstance( stepDefn, stepName )
        stepInstance.setValue( "logFile", logFile )

        return S_OK()
      