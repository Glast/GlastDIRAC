#!/usr/bin/env python
""" Get LogFile from Dirac Sandboxes
@author: V. Rolland (LUPM/IN2P3)
@author: S. Zimmer (UniGE/CERN)
"""
import sys, os, shutil, time

if __name__ == "__main__":

    from DIRAC.Core.Base import Script
    Script.addDefaultOptionValue('/DIRAC/Security/UseServerCertificate','y')
    Script.parseCommandLine()

    from DIRAC.Core.DISET.RPCClient import RPCClient
    from DIRAC.Interfaces.API.Dirac import Dirac
    import DIRAC.Core.Utilities.Time as Time

    delay_job_handled = 3
    status_to_handle = ['Done','Failed','Killed','Deleted']
    status_to_ignore = ['Running','Waiting','Checking','Stalled','Received']
    dir_temp = '/tmp'
    
    from DIRAC.ConfigurationSystem.Client.Helpers.Operations    import Operations
    op = Operations("glast.org")
    LogfileRetrievalSummaryPath = op.getValue("Pipeline/LogfileRetrievalSummaryPath", "/glast_data/Pipeline2/grid-service" )
    filename_jobhandled = LogfileRetrievalSummaryPath + '/LogfileRetrievalSummaryIDjob'
    #filename_jobhandled = '/afs/in2p3.fr/home/g/glastpro/vrolland/logFile/jobidhandled.list'

    from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
    from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

    proxy = None
    op = Operations("glast.org")

    shifter = op.getValue("Pipeline/Shifter","/DC=org/DC=doegrids/OU=People/CN=Stephan Zimmer 799865")
    shifter_group = op.getValue("Pipeline/ShifterGroup","glast_user")
    
    result = gProxyManager.downloadProxyToFile(shifter,shifter_group,requiredTimeLeft=10000)
    print result    
    if not result['OK']:
        sys.stderr.write(result['Message']+"\n")
        sys.stderr.write("No valid proxy found.\n")
        exit(1)

    proxy = result[ 'Value' ]
    os.environ['X509_USER_PROXY'] = proxy
    print("*INFO* using proxy %s"%proxy)
    
    
    print "*********************************************************************************************************"
    print "Execution at : '"+time.strftime('%d/%m/%y %H:%M',time.localtime())+"'"

    try:
        d = Dirac()
    except AttributeError:
        sys.stderr.write(time.strftime('%d/%m/%y %H:%M',time.localtime())+" => Error loading Dirac monitor\n")
        raise Exception("Error loading Dirac monitor")

    w = RPCClient("WorkloadManagement/JobMonitoring")

    delTime = str( Time.dateTime() - delay_job_handled * Time.day )

    jobid_handled = []

    file_jobhandled = open(filename_jobhandled, "r")
    for line in file_jobhandled:
        try:
            jobid_handled.append(str(int(line)));
        except ValueError:
            print "WARNING : the file '"+filename_jobhandled+"' contains a NaN line : '"+line+"'"
    file_jobhandled.close()
    
    my_dict = {}
    my_dict['OwnerDN']=[shifter]    # Waiting for glast_prod : my_dict['OwnerGroup']=[user]
    res = w.getJobs(my_dict,delTime)

    if not res['OK']:
        print res['Message']
        print "\n\n"    
        print res
        print "\n\n"
        sys.stderr.write(time.strftime('%d/%m/%y %H:%M',time.localtime())+" => "+res['Message']+"\n")
        exit(1)

    job_list_to_handle = res['Value']

    for j in job_list_to_handle:
        print "\t"+j+" => ",
        if j not in jobid_handled:
        
            res = w.getJobSummary(int(j))
            if not res['OK']:
                print res['Message']
                sys.stderr.write(time.strftime('%d/%m/%y %H:%M',time.localtime())+" => "+j+" => "+res['Message']+"\n")
                break
            summary = res['Value']    
            status_j = summary['Status']

            # Job we want to handle
            if status_j in status_to_handle:
              
                # retrieve the INPUT sandbox
                res = d.getInputSandbox(j,dir_temp)
                if not res['OK']:
                    print res['Message']
                    if "No Input sandbox registered for job" in res['Message']:
                        jobid_handled.append(j); # notify the job as "already handled"
                    else:
                        sys.stderr.write(time.strftime('%d/%m/%y %H:%M',time.localtime())+" => "+j+" => "+res['Message']+"\n")
                        
                else:

                    # check if 'jobmeta.inf' is present (if not it's not a PIPELINE job )
                    if not os.path.isfile(dir_temp+"/InputSandbox"+j+"/jobmeta.inf"):
                        print "WARNING : not a pipeline task"
                        # notify the job as "already handled"
                        jobid_handled.append(j);
                    else:
                  
                        # Get the working dir of the task from 'jobmeta.inf'
                        file_jobmeta = open( dir_temp+"/InputSandbox"+j+"/jobmeta.inf" , "r")
                        workdir = file_jobmeta.readline().splitlines()[0]
                        # TEST workdir = dir_temp+"/"+file_jobmeta.readline().splitlines()[0]
                        file_jobmeta.close()
                
                        # retrieve the OUTPUT sandbox
                        res = d.getOutputSandbox(j,dir_temp)
                        if not res['OK']:
                            print res['Message']
                            sys.stderr.write(time.strftime('%d/%m/%y %H:%M',time.localtime())+" => "+j+" => "+res['Message']+"\n")
                        elif not os.path.isfile(dir_temp+"/"+j+"/jobmeta.inf"):
                            print "ERROR : no jobmeta.inf file in the outpusandbox"
                            sys.stderr.write(time.strftime('%d/%m/%y %H:%M',time.localtime())+" => "+j+" => "+"ERROR : no jobmeta.inf file in the outpusandbox\n")

                        else: # everything is right about the outpusandbox
                  
                            # if the working directory don't exist, create it
                            if not os.path.isdir(workdir):
                                print "ERROR : The workir '"+workdir+"' has not been created during the submission"
                                sys.stderr.write(time.strftime('%d/%m/%y %H:%M',time.localtime())+" => "+j+" => "+"ERROR : The workir has not been created during the submission\n")
                            else:
                    
                                if not os.path.isfile(workdir+"/jobmeta.inf"):
                                    print "ERROR : the file 'jobmeta.inf' don't exist in the workdir : '"+workdir+"'"
                                    sys.stderr.write(time.strftime('%d/%m/%y %H:%M',time.localtime())+" => "+j+" => "+"ERROR : the file 'jobmeta.inf' don't exist in the workdir : '"+workdir+"'\n")
                        
                                else:
                        
                                    try:
                                        try:  # try to get the JobId of the previous job from its 'jobmeta.inf'
                                            file_jobmeta = open(workdir+"/jobmeta.inf", "r")
                                            file_jobmeta.readline() # ignore the line of the working directory
                                            str_last_jobid = file_jobmeta.readline()
                                            if str_last_jobid != '':
                                                # It's a rollback
                                                last_jobid = str(int(str_last_jobid))
                                            else:
                                                # First run
                                                last_jobid = "FIRST_RUN"
                                                            
                                            file_jobmeta.close()
                                        except IOError as e:
                                            print "WARNING : Impossible to open '"+workdir+"/jobmeta.inf' ({0}): {1}".format(e.errno, e.strerror)
                                            raise
                                        except ValueError:
                                            print "WARNING : Previous id job is not a number '"+str_last_jobid+"'"
                                            raise
                                    except Exception as e: # if we don't find the previous id we fix it like "UnknownIDX" 
                                        print "WARNING: %s"%str(e)
                                        #last_jobid = 'UnknownID' 
                                        suffix = 1
                                        while os.path.isdir(workdir+"/archive/"+last_jobid+str(suffix)):
                                            suffix+=1 
                                        last_jobid = last_jobid+str(suffix)
                                
                            
                        
                                    if last_jobid != "FIRST_RUN": # It's a rollback
                                        # start the copy to a sub directory in working directory "archive"
                                        print "archive the last run in '"+workdir+"/archive/"+last_jobid+"'",
                                        if not os.path.isdir(workdir+"/archive/"+last_jobid):
                                            os.makedirs(workdir+"/archive/"+last_jobid)
                                            for f in os.listdir(workdir+"/"):
                                                if not os.path.isdir(workdir+"/"+f):
                                                    shutil.move( workdir+"/"+f , workdir+"/archive/"+last_jobid+"/"+f )
                                            print "done"
                                            # we copy the InputSandbox
                                            for f in os.listdir(dir_temp+"/InputSandbox"+j):
                                                shutil.copy( dir_temp+"/InputSandbox"+j+"/"+f , workdir)
                                        else:
                                            print "ERROR : '"+workdir+"/archive/"+last_jobid+"' already exists."
                                            sys.stderr.write(time.strftime('%d/%m/%y %H:%M',time.localtime())+" => "+j+" => "+"ERROR : '"+workdir+"/archive/"+last_jobid+"' already exists.\n")

                                
                            
                                    # copy the job outpusandbox in the working directory
                                    print "move output sandbox to '"+workdir+"'... ",
                                    for f in os.listdir(dir_temp+"/"+j):
                                        shutil.copy( dir_temp+"/"+j+"/"+f , workdir )
                                    print "done"
                
                                    # notify the job as "already handled"
                                    jobid_handled.append(j);
                        # Suppress the outputSandbox in the tmp directory
                        shutil.rmtree(dir_temp+"/"+j)
                        
                # Suppress the inputSandbox in the tmp directory
                shutil.rmtree(dir_temp+"/InputSandbox"+j)
                
      
            # Job we want to ignore
            elif status_j in status_to_ignore: 
                print status_j+" ignored"
        
            # Status not anticipateds !
            else:
                print "ERROR : The job '"+j+"' has an unknown status : '"+status_j+"'"
                sys.stderr.write(time.strftime('%d/%m/%y %H:%M',time.localtime())+" => "+j+" => "+"ERROR : The job '"+j+"' has an unknown status : '"+status_j+"'\n")
 
        else:
            print "already handled"
    
    # ###################################################################################################################################################
    # update the file listing the jobs handled
    file_jobhandled = open(filename_jobhandled, "w")

    for jc in job_list_to_handle:
        if jc in jobid_handled:
            file_jobhandled.write(jc)
            file_jobhandled.write('\n')

    file_jobhandled.close()  