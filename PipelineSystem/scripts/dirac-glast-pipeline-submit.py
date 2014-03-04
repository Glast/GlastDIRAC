#!/usr/bin/env python

""" Pipeline Submission Script 

Created 10/2012
@author: S. Zimmer (OKC/SU)

"""

class options:
    def __init__(self,DICT,**kwargs):
        self.release = None
        self.cpu = 64000
        self.site = None
        self.stagein = None
        self.mailDebug = False
        self.name = None
        self.group = "user"
        self.debug = False
        self.env = None
        self.bannedSites = None
        self.__dict__.update(DICT)
        self.__dict__.update(kwargs)

def setSpecialOption( optVal ):
    from DIRAC import S_OK
    global specialOptions
    option,value = optVal.split('=')
    specialOptions[option] = value
    return S_OK()

def extract_inputfiles(fname):
    file_list = []
    lines = open(fname,'read').readlines()
    for line in lines:
        thisLine = line.replace("\n","")
        file_list.append(thisLine)
    return file_list

if __name__ == "__main__":
    import sys, os, shutil, glob
    from DIRAC.Core.Base import Script
    from DIRAC import gLogger, exit as dexit
    specialOptions = {}
    Script.registerSwitch( "p:", "parameter=", "Special option (currently supported: release, cpu, site, stagein, name, debug, mailDebug, env, bannedSites, group) ", setSpecialOption )
    # thanks to Stephane for suggesting this fix!
    Script.addDefaultOptionValue('/DIRAC/Security/UseServerCertificate','y')
    Script.parseCommandLine()
    args = Script.getPositionalArgs() 
    #from DIRAC.Interfaces.API.Job import Job
    from GlastDIRAC.PipelineSystem.Interface.GlastJob import GlastJob as Job
    from DIRAC.Interfaces.API.Dirac import Dirac
    from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
    from GlastDIRAC.ResourceStatusSystem.Client.SoftwareTagClient import SoftwareTagClient
    from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
    opts = options(specialOptions) # converts the "DIRAC registerSwitch()" to something similar to OptionParser
    pipeline = False
    pipeline_dict = None
    if not opts.env is None:
        import json
        f = open(specialOptions["env"],"r")
        pipeline_dict = json.load(f)
        pipeline = True
        
    if pipeline:
        proxy = None
        # check whether critical email information is available, if any or all of those are not there, raise an exception and fail to submit.
        required_variables = ["PIPELINE_FROMADDRESS","PIPELINE_TOADDRESS","PIPELINE_PROCESSINSTANCE","PIPELINE_ERRORADDRESS","JOBCONTROL_LOGFILE"]
        res = [i for i in required_variables if i not in pipeline_dict]
        if len(res)!=0:
            gLogger.error("Could not find critical variables for submission:",str(res))
            dexit(1)            
        op = Operations("glast.org")
        #TODO: replace glast.org with VO-agnostic statement
        shifter = op.getValue("Pipeline/Shifter","/DC=org/DC=doegrids/OU=People/CN=Stephan Zimmer 799865")
        shifter_group = op.getValue("Pipeline/ShifterGroup","glast_user")
        result = gProxyManager.downloadProxyToFile(shifter,shifter_group,requiredTimeLeft=10000)
        if not result['OK']:
            gLogger.error("No valid proxy found; ",result['Message'])
            dexit(1)
        proxy = result[ 'Value' ]
        os.environ['X509_USER_PROXY'] = proxy
        gLogger.info("using proxy %s"%proxy)
    
    j = Job(stdout="DIRAC_wrapper.txt",stderr="DIRAC_wrapper.txt") # specifies the logfile
    
    input_sandbox_files = []
    output_sandbox_files = ["*.log","*.txt", "jobmeta.inf"]
    if pipeline:
        if opts.mailDebug:
            pipeline_dict["PIPELINE_DEBUG_ADDRESS"]=op.getValue("Pipeline/DebugMailAddress","glastpro@in2p3.fr")
        j.setExecutionEnv(pipeline_dict) # that sets the env vars
        if pipeline_dict.has_key("GPL_CONFIGDIR"):
            GPL_CONFIGDIR = pipeline_dict['GPL_CONFIGDIR']
            files = []
            if os.path.isdir(GPL_CONFIGDIR):
                files_to_copy = glob.glob("%s/*"%GPL_CONFIGDIR)
                for f in files_to_copy:
                    if os.path.isfile(f):
                        input_sandbox_files.append(os.path.abspath(f))
                    else:
                        input_sandbox_files.append(f)
        if pipeline_dict.has_key("DIRAC_OSB"):
            DIRAC_OUTPUTSANDBOX = pipeline_dict["DIRAC_OSB"]
            files = DIRAC_OUTPUTSANDBOX.split(",")
            for f in files:
                output_sandbox_files.append(f)
    #print input_sandbox_files #DEBUG
    executable = None
    if len(args)>0:
        # BUG: pipeline.process instance creates pipeline_wrapper --> sets automatically 'bash pipeline_wrapper' as cmd
        log = "logFile.txt"
        if pipeline:
            input_sandbox_files.append("jobmeta.inf") # that one is generated with every single job (or at least should be)
            if os.path.isfile(os.path.join(pipeline_dict["PIPELINE_WORKDIR"],"script")):
                script = os.path.join(pipeline_dict["PIPELINE_WORKDIR"],"script")
                os.chmod(script,0755) # to make sure it's executable.
                input_sandbox_files.append(script)
            # here we branch out...
            pipeline_wrapper = os.path.join(os.environ["DIRAC"],"GlastDIRAC/PipelineSystem/scripts/dirac-glast-pipeline-wrapper.sh")
            if opts.mailDebug:
                pipeline_wrapper = os.path.join(os.environ["DIRAC"],"GlastDIRAC/PipelineSystem/test/dirac-glast-pipeline-wrapper-debug.sh")
            input_sandbox_files.append(pipeline_wrapper)
            executable = pipeline_wrapper
            log = pipeline_dict["PIPELINE_LOGFILE"]    
        else:
            executable = args[0].replace("bash ","").replace("./","")
            if not os.path.isfile(executable):
                gLogger.error("file %s not found."%executable)
                dexit(1)
            os.chmod(executable,0755) # make file executable
            input_sandbox_files.append(executable)
        
        #j.setExecutable(str(executable),logFile=str(log))
        j.addWrapper(logFile=log)
    else:
        gLogger.error("No executable defined.")
        dexit(1)
        
    j.setName("MC job")
    if not opts.name is None:
        j.setName(opts.name)
    if not opts.group is "user":
        j.setJobGroup(str(opts.group))
    j.setInputSandbox(input_sandbox_files) # all input files in the sandbox
    j.setOutputSandbox(output_sandbox_files)

    j.setCPUTime(opts.cpu)
    if not opts.site is None:
        j.setDestination(opts.site.split(","))#can also be a list
        
    if not opts.bannedSites is None:
        j.setBannedSites(opts.bannedSites.split(","))

    if not opts.release is None:
        tag = opts.release
        cl = SoftwareTagClient()
        result = cl.getSitesForTag(tag,'Valid') # keyword doesn't work there.
        if not result['OK']:
            gLogger.error("*ERROR* Could not get sites for Tag %s"%tag,result['Message'])
            dexit(1)
        sites = result[ 'Value' ]
        j.setDestination(sites)
    # new feature: add xrootd-keytab file to input list. this one resides on SE
    
    input_stage_files = []
    if pipeline:
        xrd_keytab = op.getValue("Pipeline/XrdKey",None)    
        if not xrd_keytab:
            gLogger.notice("*DEBUG* adding XrdKey file %s to input"%xrd_keytab)
            input_stage_files.append(xrd_keytab)
    if not opts.stagein is None:
        # we do add. input staging
        files = opts.stagein.split(",")
        for f in files:
                input_stage_files.append(f)
    if len(input_stage_files):
        if len(input_stage_files)==1:
            j.setInputData(input_stage_files[-1])
        else:
            j.setInputData(input_stage_files)
    if opts.debug:
        gLogger.notice('*DEBUG* just showing the JDL of the job to be submitted')
        gLogger.notice(j._toJDL())
    
    d = Dirac(True,"myRepo.rep")
    res = d.submit(j)
    if not res['OK']:
        gLogger.error("Error during Job Submission ",res['Message'])
        dexit(1)
    JobID = res['Value']
    gLogger.notice("Your job %s (\"%s\") has been submitted."%(str(JobID),executable))
    