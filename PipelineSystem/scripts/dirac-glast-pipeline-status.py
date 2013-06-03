#!/usr/bin/env python
# S.Zimmer 10/2012 The Oskar Klein Center for Cosmoparticle Physics
import xml.dom.minidom as xdom
import sys, getopt, os, StringIO

class logging:
    def __init__(self,ntuple):
        self.main_status = ntuple[0]
        self.major_status = ntuple[1]
        self.minor_status = ntuple[2]
        self.time = ntuple[3]
        self.name = ntuple[4]

class internalstatus:
    def __init__(self,job_id,my_dict,**kwargs):
        self.id = job_id
        self.status = None
        self.started = None
        self.submitted = None
        self.ended = None
        self.cputime = None
        self.mem = None
        self.hostname = None
        if my_dict.has_key("Status"):
            self.status = my_dict['Status']
        if my_dict.has_key("Started"):
            self.started = my_dict['Started']
        if my_dict.has_key("Submitted"):
            self.submitted = my_dict['Submitted']
        if my_dict.has_key("Ended"):
            self.ended = my_dict['Ended']
        if my_dict.has_key('NormCPUTime(s)'):
            self.cputime = my_dict['NormCPUTime(s)']
        if my_dict.has_key('CacheSize(kB)'):
            self.mem = my_dict['CacheSize(kB)']
        if my_dict.has_key("Site"):
            if self.hostname is None:
                self.hostname = ""
            self.hostname+=my_dict['Site']
        self.mydict = my_dict
        self.__dict__.update(kwargs)
        #print my_dict

    def __call__(self):
        old_dict = self.__dict__
        for key in self.__dict__.keys():
            if self.__dict__[key] is None:
                self.__dict__[key]="-"
        k = '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s'%(self.id,self.hostname,self.status,self.submitted,self.started,self.ended,self.cputime,self.mem)
        self.__dict__ = old_dict
        return k

    def _toxml(self):
        xf = xdom.parse(StringIO.StringIO('<?xml version="1.0" ?><some_tag/>'))
        job = xf.createElement("job")
        job.setAttribute("JobID",self.id)
        for key in self.mydict.keys():
            xmlkey = key.replace("(","").replace(")","").replace(" ","")
            new_node = xf.createElement(xmlkey)
            dict_value = self.mydict[key]
            if not dict_value is None:
                if "\n" in dict_value:
                    textnode = xf.createTextNode(dict_value)
                    new_node.appendChild(textnode)
                    job.appendChild(new_node)
                else:
                    job.setAttribute(xmlkey,str(dict_value))
        return job
    
def setSpecialOption( optVal ):
    from DIRAC import S_OK
    global specialOptions
    option,value = optVal.split('=')
    specialOptions[option] = value
    return S_OK()

if __name__ == "__main__":

    from DIRAC.Core.Base import Script
    from DIRAC import gLogger, exit as dexit
    specialOptions = {}
    Script.registerSwitch( "p:", "parameter=", "Special option (currently supported: user, xml, dayspassed, logging) ", setSpecialOption )
    # thanks to Stephane for suggesting this fix!
    Script.addDefaultOptionValue('/DIRAC/Security/UseServerCertificate','y')
    Script.parseCommandLine()
    #print specialOptions
    from DIRAC.Core.DISET.RPCClient import RPCClient
    from DIRAC.Interfaces.API.Dirac import Dirac
    import DIRAC.Core.Utilities.Time as Time
    # use stored certificates
    from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
    from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
    do_xml = False
    
    user = os.getenv("USER")
    doLogging = False
    if specialOptions.has_key("xml"):
        do_xml = specialOptions["xml"]
    if specialOptions.has_key("user"):
        user = specialOptions["user"]
    if specialOptions.has_key("logging"):
        doLogging = specialOptions["logging"]
    if do_xml:
        xmlfile = xdom.parse(StringIO.StringIO('<?xml version="1.0" ?><joblist/>'))
        firstChild = xmlfile.firstChild
    
    d = Dirac()
    w = RPCClient("WorkloadManagement/JobMonitoring")
    my_dict = {}
    my_dict['Status']=['Matched','Staging','Completed','Done','Failed','Rescheduled','Stalled','Waiting','Running','Checking'] # monitor all states
    my_dict['Owner']=[user]
    res = w.getJobs(my_dict)
    
    if not res['OK']:
        gLogger.error("Could not get list of running jobs.",res['Message'])
        dexit(1)

    job_list = res['Value']

    #for j in job_list:
    res = d.status(job_list)   
    
    if not res['OK']:
        gLogger.error("Could not get status of job_list,",res['Message'])
        dexit(1)
    
    status = res['Value']
    statuses = []
    if not do_xml:
        print('# ID\thostname\tStatus\tSubmitted\tStarted\tEnded\tCPUtime\tMemory')
    for j in job_list:
        status_j=status[int(j)]
        if doLogging:
            res = w.getJobParameters(int(j))
            if not res['OK']:
                gLogger.error("Could not get Job Parameters;",res["Message"])
                dexit(1)
            status_j.update(res['Value'])
            res = w.getJobLoggingInfo(int(j))
            #print res
            if not res['OK']:
                gLogger.error("Could not get JobLoggingInfo;",res['Message'])
                dexit(1)
            logs = res['Value']
            logging_obj = []
            for l in logs:
                logging_obj.append(logging(l))
            logging_info = {'Submitted': logging_obj[0].time, 'Started': None, 'Ended': None, 'JobID': j}
            for l in logging_obj:
                if l.major_status == 'Application':
                    logging_info['Started']=l.time
            if status_j['Status'] == 'Done':
                logging_info['Ended']=logging_obj[-1].time
            status_j.update(logging_info)
        new_stat = internalstatus(j,status_j)
        #print new_stat._toxml().toprettyxml()
        if do_xml:
            firstChild.appendChild(new_stat._toxml())
        else:
            print(new_stat())
    # TODO:
        # pretty print & parse in java
    if do_xml:
        print(xmlfile.toprettyxml())
