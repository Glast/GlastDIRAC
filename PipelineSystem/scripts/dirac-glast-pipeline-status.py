#!/usr/bin/env python
""" Pipeline Submission Script 

Created 10/2012
@author: S. Zimmer (OKC/SU)

"""

import xml.dom.minidom as xdom
import sys, getopt, os, StringIO, datetime

class LoggingRecord:
    def __init__(self,ntuple):
        self.main_status = ntuple[0]
        self.major_status = ntuple[1]
        self.minor_status = ntuple[2]
        self.time = ntuple[3]
        self.name = ntuple[4]

class InternalJobStatus:
    SERIALIZABLE = ("StandardOutput", "CPUMHz", "CPUNormalizationFactor", "CPUScalingFactor", "CacheSizekB", 
                    "Ended", "HostName", "JobID", "JobPath", "JobSanityCheck", "JobWrapperPID", "LocalAccount", 
                    "LocalBatchID", "LocalJobID", "MemorykB", "MinorStatus", "ModelName", "NormCPUTimes", 
                    "OK", "OutputSandboxMissingFiles", "PayloadPID", "PilotAgent", "Pilot_Reference", 
                    "ScaledCPUTime", "Site", "Started", "Status", "Submitted", "TotalCPUTimes")

    def __init__(self,job_id,my_dict,**kwargs):
        translateJobSummary(my_dict)
        self.id = str(job_id)
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
        if my_dict['Status'] in ('Done','Failed') and 'LastUpdateTime' in my_dict:
            self.ended = my_dict['LastUpdateTime']
        self.mydict = {}
        for key in my_dict:
            if key in self.SERIALIZABLE:
                self.mydict[key] = my_dict[key]
        #self.mydict = my_dict
        self.__dict__.update(kwargs)

    def setSite(self,site):
        self.hostname = site

    def setEndTime(self,deltatimeseconds=86400):
        #86400 is 1 day!
        local_time = datetime.datetime.utcnow()
        failed_time_stamp = local_time-datetime.timedelta(seconds=deltatimeseconds)
        str_timestamp = failed_time_stamp.strftime("%Y-%m-%d %H:%M:S")
        self.ended = str_timestamp

    def getStatus(self):
        return self.status

    def getEndTime(self):
        return self.ended

    def getStartTime(self):
        return self.started

    def __str__(self):
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
    

def translateJobSummary(job_dict):
    if 'SubmissionTime' in job_dict:
        job_dict['Submitted'] = job_dict['SubmissionTime']
    if job_dict['Status'] in ('Done','Failed') and 'LastUpdateTime' in job_dict:
        job_dict['Ended'] = job_dict['LastUpdateTime']
    if job_dict['Status'] is 'Running' and 'LastUpdateTime' in job_dict:
        job_dict['Started'] = job_dict['LastUpdateTime']


def setSpecialOption( optVal ):
    from DIRAC import S_OK
    global specialOptions
    option,value = optVal.split('=')
    specialOptions[option] = value
    return S_OK()

if __name__ == "__main__":
    stdout = sys.stdout
    sys.stdout = sys.stderr
    from DIRAC.Core.Base import Script
    from DIRAC import gLogger, exit as dexit
    specialOptions = {}
    Script.registerSwitch( "p:", "parameter=", "Special option (currently supported: user, xml, dayspassed, logging, JobID) ", setSpecialOption )
    # thanks to Stephane for suggesting this fix!
    Script.addDefaultOptionValue('/DIRAC/Security/UseServerCertificate','y')
    Script.parseCommandLine()
    #print specialOptions
    from DIRAC.Core.DISET.RPCClient import RPCClient
    from DIRAC.Interfaces.API.Dirac import Dirac
    import DIRAC.Core.Utilities.Time as Time
    # use stored certificates
    from DIRAC.Core.Utilities.List import breakListIntoChunks
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

    if not specialOptions.has_key("JobID"):
        my_dict = {}
        #my_dict['Status']=['Matched','Staging','Completed','Done','Failed','Rescheduled','Stalled','Waiting','Running','Checking'] # monitor all states
        my_dict['Owner']=[user]
        local_time = datetime.datetime.utcnow()
        timedelta = local_time-datetime.timedelta(seconds=86400)
        if specialOptions.has_key("dayspassed"):
            timedelta = local_time-datetime.timedelta(seconds=float(specialOptions["dayspassed"])*3600)
        res = w.getJobs(my_dict,timedelta.strftime( '%Y-%m-%d %H:%M:%S' ))

        if not res['OK']:
            gLogger.error("Could not get list of running jobs.",res['Message'])
            dexit(1)
        job_list = res['Value']
    else:
        job_list = specialOptions["JobID"].split(",")
        doLogging = True
    status = {}
    sites = {} 

    for chunk in breakListIntoChunks(job_list,1000):
        res = d.getJobSummary(chunk)   
        if not res['OK']:
            gLogger.error("Could not get status of job list chunk,",res['Message'])
            if do_xml:
                d.exit(1)            
            continue
        status.update(res['Value'])
    # get sites info
        res = w.getJobsSites(chunk)
        if not res['OK']:
            gLogger.error("Could not get sites;",res['Message'])
        sites.update(res['Value'])
    
    if not do_xml:
        print('# ID\thostname\tStatus\tSubmitted\tStarted\tEnded\tCPUtime\tMemory')

    job_list = [int(i) for i in job_list]
    for job in job_list:
        status_j=status[job]
        if doLogging:
            res = w.getJobParameters(job)
            if not res['OK']:
                gLogger.error("Could not get Job Parameters;",res["Message"])
                dexit(1)
            status_j.update(res['Value'])
            res = w.getJobLoggingInfo(job)
            #print res
            if not res['OK']:
                gLogger.error("Could not get JobLoggingInfo;",res['Message'])
                dexit(1)
            logs = res['Value']
            logging_obj = []
            for l in logs:
                logging_obj.append( LoggingRecord(l) )
            logging_info = {'Submitted': logging_obj[0].time, 'Started': None, 'Ended': None, 'JobID': str(job)}
            for record in logging_obj:
                if record.major_status == 'Application':
                    logging_info['Started'] = record.time
            if status_j['Status'] == 'Done':
                logging_info['Ended']=logging_obj[-1].time 
            status_j.update(logging_info)
        new_stat = InternalJobStatus(job,status_j)
        sys.stdout = stdout
        if new_stat.getStatus()=="Failed":
            if not new_stat.getEndTime():
                gLogger.info("Time stamp for ended job %i not provided, setting it to 1 day in the past!" %job)
                new_stat.setEndTime()
                  # addresses LPG-35
#                 gLogger.info("Requesting to kill job %i" %job)
#                 d.kill(job)
        if job in sites:
            new_stat.setSite(sites[job]['Site'])
        #print new_stat._toxml().toprettyxml()
        if do_xml:
            firstChild.appendChild(new_stat._toxml())
        else:
            print(new_stat)
    # TODO:
        # pretty print & parse in java
    sys.stdout = stdout
    if do_xml:
        print(xmlfile.toprettyxml())

