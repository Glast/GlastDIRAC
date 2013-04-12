#!/usr/bin/env python
# S.Zimmer 10/2012 The Oskar Klein Center for Cosmoparticle Physics
#TODO: add correct X509_USER_PROXY path to submission script
#TODO: update DB query



class options:
    def __init__(self,DICT,**kwargs):
        self.release = None
        self.cpu = 64000
        self.site = None
        self.stagein = None
        self.name = None
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
    specialOptions = {}
    Script.registerSwitch( "p:", "parameter=", "Special option (currently supported: release, cpu, site, stagein, name, debug, env, bannedSites) ", setSpecialOption )
    # thanks to Stephane for suggesting this fix!
    Script.addDefaultOptionValue('/DIRAC/Security/UseServerCertificate','y')
    Script.parseCommandLine()
    args = Script.getPositionalArgs() 
    from DIRAC.Interfaces.API.Job import Job
    from DIRAC.Interfaces.API.Dirac import Dirac
    from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
    from GlastDIRAC.SoftwareTagSystem.SoftwareTagClient import SoftwareTagClient
    proxy = None
    opts = options(specialOptions) # converts the "DIRAC registerSwitch()" to something similar to OptionParser
    #print opts.__dict__
    #sys.exit()
    # use stored certificates
    result = gProxyManager.downloadProxyToFile('/DC=org/DC=doegrids/OU=People/CN=Stephan Zimmer 799865','glast_user',requiredTimeLeft=10000)
    if not result['OK']:
        raise Exception(result)
    proxy = result[ 'Value' ]
    os.environ['X509_USER_PROXY'] = proxy
    print("*INFO* using proxy %s"%proxy)
    j = Job(stdout="logFile.txt",stderr="logFile.txt") # specifies the logfile
    
    input_sandbox_files = []
    output_sandbox_files = ["logFile.txt", "jobmeta.inf"]
    pipeline_dict = None
    if not opts.env is None:
        import json
        f = open(specialOptions["env"],"r")
        pipeline_dict = json.load(f)
        j.setExecutionEnv(pipeline_dict) # that sets the env vars
    if not pipeline_dict is None:
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

    if len(args)>0:
        executable = args[0]
        input_sandbox_files.append(executable) # add executable to input sandbox
        input_sandbox_files.append("jobmeta.inf") # that one is generated with every single job (or at least should be)
        j.setExecutable(executable)

    j.setName("MC job")
    if not opts.name is None:
        j.setName(opts.name)

    j.setInputSandbox(input_sandbox_files) # all input files in the sandbox
    j.setOutputSandbox(output_sandbox_files)

    j.setCPUTime(opts.cpu)
    if not opts.site is None:
        j.setDestination(opts.site.split(","))#can also be a list
        
    if not opts.bannedSites is None:
        j.setBannedSites(opts.bannedSites.split(","))

    if not opts.release is None:
        release = opts.release
        cl = SoftwareTagClient()
        result = cl.getSitesForTag(tag)
        if not result['OK']:
            raise Exception(result)
        sites = result[ 'Value' ]
        j.setDestination(sites)

    if not opts.stagein is None:
        input_stage_files = []
        # we do add. input staging
        files = opts.stagein.split(",")
        for f in files:
            if f.startswith("LFN"):
                input_stage_files.append(f)
            else:
                input_stage_files+=extract_file(f)
        for f in input_stage_files:
            if not f.startswith("LFN"):
                raise Exception("*ERROR* required inputfiles to be defined through LFN, could not find LFN in %s"%f)
        j.setInputData(input_stage_files)

    if opts.debug:
        print('*DEBUG* just showing the JDL of the job to be submitted')
        print(j._toJDL())
    else:
        try:
            d = Dirac()
        except AttributeError:
            raise Exception("Error loading Dirac monitor")

        print("Your job %s (\"%s\") has been submitted."%(str(d.submit(j)['Value']),executable))
                                                         
