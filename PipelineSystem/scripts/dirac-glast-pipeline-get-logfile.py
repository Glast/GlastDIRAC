#!/usr/bin/env python
# V.ROLLAND 11/2012 Laboratoire Univers et Particules de Montpellier
import sys, getopt, os, shutil, commands, time

if __name__ == "__main__":

	from DIRAC.Core.Base import Script
	Script.addDefaultOptionValue('/DIRAC/Security/UseServerCertificate','y')
	Script.parseCommandLine()

	from DIRAC.Core.DISET.RPCClient import RPCClient
	from DIRAC.Interfaces.API.Dirac import Dirac
	import DIRAC.Core.Utilities.Time as Time

	user = 'zimmer'
	delay_job_handled = 3
	status_to_handle = ['Done','Failed','Killed','Deleted']
	status_to_ignore = ['Running','Waiting','Checking','Stalled','Received']
	dir_temp = '/tmp'
	filename_jobhandled = '/glast_data/Pipeline2/grid-service/jobidhandled'

	from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
	from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

	proxy = None
	op = Operations()

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
		jobid_handled.append(str(int(line)));
	file_jobhandled.close()

	my_dict = {}
	my_dict['Owner']=[user]
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
		if not j in jobid_handled:

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
				workdir = file_jobmeta.readline()
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
					os.makedirs(workdir)
				
				# If the working directory is not empty, The job was a ROLLBACK
				if os.listdir(workdir)!=[]:  
					# It's a rollback !
				      
					# Create the archive directory in the workdir if it don't exist
					if not os.path.isdir(workdir+"/archive"):
						os.mkdir(workdir+"/archive")

					try:
						try:  # try to get the JobId of the previous job from its 'jobmeta.inf'
							file_jobmeta = open(workdir+"/jobmeta.inf", "r")
							file_jobmeta.readline() # ignore the line of the working directory
							str_last_jobid = file_jobmeta.readline()
							last_jobid = str(int(str_last_jobid));
							file_jobmeta.close()
						except IOError as e:
							print "WARNING : Impossible to open '"+workdir+"/jobmeta.inf' ({0}): {1}".format(e.errno, e.strerror)
							raise
						except ValueError:
							print "WARNING : Previous id job is not a number '"+str_last_jobid+"'"
							raise
						except:
							print "WARNING : the Previous id job can't be retrieved for an obscur reason"
							raise
					except: # if we don't find the previous id we fix it like "UnknownIDX" 
						last_jobid = 'UnknownID' 
						suffix = 1
						while os.path.isdir(workdir+"/archive/"+last_jobid+str(suffix)):
							suffix+=1 
						last_jobid = last_jobid+str(suffix)

					# start the copy to a sub directory in working directory "archive"
					print "archive the last run in '"+workdir+"/archive/"+last_jobid+"'",
					if not os.path.isdir(workdir+"/archive/"+last_jobid):
						os.mkdir(workdir+"/archive/"+last_jobid)
						for f in os.listdir(workdir+"/"):
							if not os.path.isdir(workdir+"/"+f):
							shutil.move( workdir+"/"+f , workdir+"/archive/"+last_jobid+"/"+f )
						print "done"
					else:
						print "ERROR : '"+workdir+"/archive/"+last_jobid+"' already exists."
						sys.stderr.write(time.strftime('%d/%m/%y %H:%M',time.localtime())+" => "+j+" => "+"ERROR : '"+workdir+"/archive/"+last_jobid+"' already exists.\n")

				# copy the job outpusandbox in the working directory
				print "move output sandbox to '"+workdir+"'... ",
				for f in os.listdir(dir_temp+"/"+j):
					shutil.move( dir_temp+"/"+j+"/"+f , workdir )
				print "done"
				
				# notify the job as "already handled"
				jobid_handled.append(j);
				
				# Suppress the sandboxes in the tmp directory
				shutil.rmtree(dir_temp+"/InputSandbox"+j)
				shutil.rmtree(dir_temp+"/"+j)
	  
		# Job we want to ignore
		elif status_j in status_to_ignore: 
			print status_j+" ignored"
		    
		# Status not anticipated !
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

