#!/bin/env python
""" Software Tag Interface System 

Created 06/04/2012
@author: S. Zimmer (OKC/SU)
@author: V. Rolland (IN2P3/LUPM)

"""

import cmd, sys, os
from GlastDIRAC.ResourceStatusSystem.Client import SoftwareTagClient
from DIRAC.Core.Utilities import PromptUser

class SoftwareTagCli(cmd.Cmd):
    def __init__(self):
        self.client = SoftwareTagClient.SoftwareTagClient()
        cmd.Cmd.__init__(self)
        self.prompt = 'SoftwareTagClient:/>'

    def do_add(self,args):
        """ add something 
            add tag <tag> site1,site2,site3
        """
        errorcount = 0
        argss = args.split()
        if (len(argss)==0):
            print self.do_add.__doc__
            return
        option = argss[0]
        del argss[0]
        if option == "tag":
           tag = argss[0]
           sites = argss[1].split(",") 
           for site in sites:
               res = self.client.addTagAtSite(tag,site)
               if not res['OK']:
                   print "Could not register tag %s at site %s, message %s"%(tag,site,res['Message'])
                   errorcount+=1
                   continue
               print "Added %s to %i CEs"%(tag,len(res['Value'][tag]))
        else:
            print "ERROR parsing command"
            print self.do_add.__doc__
            return
        if errorcount!=0:
            print "Found errors, cannot continue"
            return
    
    def do_remove(self,args):
        """ remove something 
            remove tag <tag> site1,site2,site3
        """
        errorcount = 0
        argss = args.split()
        if (len(argss)==0):
            print self.do_remove.__doc__
            return
        option = argss[0]
        del argss[0]
        if option == "tag":
           tag = argss[0]
           sites = argss[1].split(",") 
           for site in sites:
               res = self.client.removeTagAtSite(tag,site)
               if not res['OK']:
                   print "Could not remove tag %s at site %s, message %s"%(tag,site,res['Message'])
                   errorcount+=1
                   continue
        else:
            print "ERROR parsing command"
            print self.do_remove.__doc__
            return
        if errorcount!=0:
            print "Found errors, cannot continue"
            return
        
    def do_get(self,args):
        """ get something
        get tag <site1,site2,site3> [<status>]- lists tags at site 1 -3
        get site <tag> [<status>]- lists sites supporting tag
        """
        errorcount = 0
        argss = args.split()
        if (len(argss)==0):
            print self.do_get.__doc__
            return
        option = argss[0]
        del argss[0]
        status = 'Valid'
        if len(argss)>1:
            status = argss[-1]
        if option == "tag":
            tags = []
            # expect site as input
            sites = argss[0].split(",")
            tags = []
            for site in sites:
                res = self.client.getTagsAtSite(site,status)
                if not res['OK']:
                    print "Failed to get tags for site %s; %s"%(site,res['Message'])
                    errorcount+=1
                else:
                    tags+=res['Value']
            print "Found the following tags:"
            for tag in tags:
                print tag
        
        elif option == "site":
            tag = argss[0]
            res = self.client.getSitesForTag(tag,status)
            if not res['OK']:
                print "Failed to get sites for tag %s; %s"%(tag,res['Message'])
            else:
                sites = res['Value']
                print "Found the following sites:"
                for site in sites:
                    print site
        else:
            print "ERROR parsing command"
            print self.do_get.__doc__
            return
        if errorcount!=0:
            print "Found errors, cannot continue"
            return
    
    def do_listpreinstalled(self,argss):
        """ List softwares pre-install on ccglast02.in2p3.fr. thoses software are available to be install on sites
            listpreinstalled
        """
   
        cmd_listpreinstall = "cat /glast/applicat/preinstallGlastRelease/listsoftware.list"
        cmd_ssh = 'ssh glastpro@ccage.in2p3.fr "ssh ccglast02.in2p3.fr \\"'+cmd_preinstall+'\\" \" '
   
        print " - List of softwares pre installed on ccglast02.in2p3.fr available to be install on sites :"

        if os.system(cmd_ssh) != 0 :
            print "Error during the command '"+cmd_ssh+"'"
            return

    
    def do_reset(self,args):
        """ 
            to reset a tag system-wide use:
            >> forcestatus tag <tag> New ALL
            to reset all tags at a site use:
            >> forcestatus site <site> New
        """
        print self.do_reset.__doc__ 
        return
        
    def do_forcestatus(self,args):
        """ update status of tag, site or both
            *** USE WITH ABSOLUTE CARE!!! ***
            forcestatus tag <tag> <status> [<site>] : the entire tag is set for all sites, site is optional.
            forcestatus site <site> <status> : all tags at the site are updated 
            status can only be Valid, Bad, and New
        """
        argss = args.split()
        if (len(argss)==0):
            print self.do_forcestatus.__doc__
            return
        option = argss[0]
        del argss[0]
        res = tag = site = status = None
        if option == "site":
            if len(argss)<1:
                print self.do_forcestatus.__doc__
            tag = ""
            site = argss[0]
            status = argss[1]
        elif option == "tag":
            if len(argss)<1:
                print self.do_forcestatus.__doc__
            site = "ALL"
            tag = argss[0]
            status = argss[1]
            if len(argss)==3:
                site = argss[-1]
        else:
            print "ERROR parsing command"
            print self.do_forcestatus.__doc__
            return
        if status not in ['Bad','New','Valid']:
            print "ERROR invalid status"
            print self.do_forcestatus.__doc__
            return
        if status == 'New' and tag == "":
            if site == "ALL":
                print "ERROR bad boy, you should never reset the entire DB!!"
                return
            else:
                print 'Resetting all tags at site %s'%site
        print "Force update to status=%s:\nsites=%s\ntags=%s "%(status,site,tag)
        res = PromptUser.promptUser("Are you sure?!", choices=['y','n'], default='n')
        if not res['OK']:
            return
        elif res['Value'] != 'y':
            return
        res = self.client.updateStatus(tag,site,status)
        if not res['OK']:
            print 'Message: %s'%res['Message']
        elif res['Value']['Failed']:
            print 'Failed to update %s'%res['Value']['Failed']
        else:
            print 'Successfully updated %i CEs'%len(res['Value']['Successful'])
        return
    
    def do_list(self,argss):
        """ list tags/sites known to the system 
            list tags [<status>] :  lists all tags known to the system
            list sites [<status>]:  lists all sites known to the system 
                                    where tags with status [<status>] are registered 
        """
        args = argss.split()
        if len(args)<1:
            print self.do_list().__doc__
            return
        option = args[0]
        status = 'Valid'
        del args[0]
        if len(args)>0:
            status = args[0]
        res = self.client.getTagsAtSite("ALL",status)
        if not res['OK']:
            print 'ERROR retrieving tags: %s'%res['Message']
        else:
            tags = res['Value']
            if option == "tags":
                print "Found the following tags:"
                for tag in tags:
                    print tag
                
            elif option == "sites":
                sites = []
                for tag in tags:
                    res = self.client.getSitesForTag(tag,status)
                    # returns list of CEs
                    if not res['OK']:
                        print 'ERROR retrieving sites for tag %s'%tag
                    else:
                        sites+=res['Value']
                # done, need to re-parse CEs to sites
                if not res['OK']:
                    print 'ERROR mapping sites to CEs'
                    return
                else:
                    print 'Found the following sites'
                    for site in sites:
                        print site
            else:
                print "ERROR parsing command"
                print self.do_get.__doc__
                return
             
    def do_quit(self,args):
        """ quit """
        sys.exit(0)
        
    def do_exit(self,args):
        """ quit """
        sys.exit(0)      
    
    def do_removetagfromdb(self,argss):
        """ forget tag (drop in DB)
            *** USE WITH ABSOLUTE CARE!!! ***
            *** only used by OPERATORS ***
            cleantag tag
        """
        args = argss.split()
        if len(args)<1:
            print self.do_removetagfromdb.__doc__
            return
        tag = args[0]
        completed = False
        ces_removed = 0
        print "Removing tag %s from DB"%tag
        for i in range(3):
            # ask 3 times!
            res = PromptUser.promptUser("Are you REALLY sure?!", choices=['y','n'], default='n')
            if not res['OK']:
                return
            elif res['Value'] != 'y':
                return
        while not completed:
            res = self.client.cleanTagAtSite(tag,"ALL")
            if not res['OK']:
                print "Failed to remove tag %s: %s"%(tag,res['Message'])
            elif res['OK'] and len(res['Value']['Failed'])!=0:
                print 'Failed to remove tag %s in %i CEs'%(tag,len(res['Value']['Failed']))
            else:
                completed = True
                ces_removed+=len(res['Value']['Successful'])
        print "Removed %s from %i CEs"%(tag,ces_removed)



    def do_installonsite(self,argss):
        """ Install a release on a grid site : 
            installonsite tag site
        """
        args = argss.split()
        if len(args)<2:
            print self.do_installonsite.__doc__
            return
        tag = args[0]
        site = args[1]
        
        #print "Check if the software with the tag '"+tag+"' exists on the rsync server..." 
        #res = self.client.getSitesForTag(tag)
        #if not res['OK']:
          #print res['Message']
          #return 
        #print "tag found !"  

        from DIRAC.Interfaces.API.Dirac import Dirac
        d = Dirac()
        from DIRAC.Interfaces.API.Job import Job
        
        from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
        import os
        
        ops = Operations()
        scriptname = "InstallSoftware.py"
        j = Job()
        j.setDestination(site)
        j.setCPUTime(1000)
        j.setName("Installation "+tag)
        j.setExecutable(os.environ['DIRAC']+"/GlastDIRAC/ResourceStatusSystem/Client/"+scriptname +" "+tag , logFile='SoftwareInstallation.log')
        j.setOutputSandbox('*.log')
        res = d.submit(j)
        if not res['OK']:
          print "Could not submit the installation at site %s, message %s"%(site,res['Message'])
          return
        
        print "Job submitted, id = "+str(res['Value'])
        
        print "Add tag :"
        res = self.client.addTagAtSite(tag,site)
        if not res['OK']:
            print "Could not register tag %s at site %s, message %s"%(tag,site,res['Message'])
            return
        print "Added %s to %i CEs"%(tag,len(res['Value'][tag]))
        

    def do_preinstall(self,argss):
        """ Pre-Install a release on the rsync server :
            preinstall tag 
        """
        args = argss.split() 
        if len(args)<1:
            print self.do_preinstall.__doc__
            return
        tag = args[0].replace("VO-glast.org-","")
        items = tag.split("/")
        if len(items)!=4:
            print "Bad tag structure"
            return

        tag_os = items[0]
        variant = items[1]
        package = items[2]
        version = items[3]       
        
        cmd_preinstall = "/glast/applicat/preinstallGlastRelease/GlastReleasePreinstaller -o "+tag_os+" -v "+version+" -a "+variant
        cmd_ssh = 'ssh glastpro@ccage.in2p3.fr "ssh ccglast02.in2p3.fr \\"'+cmd_preinstall+'\\" \" '
        
        print "Submission of the preinstallation will beginning after you give the password of the glastpro account."
        print " NB : the installation takes several minutes... be patient !"
        if os.system(cmd_ssh) != 0 :
            print "Error during the command '"+cmd_ssh+"'"
            return
        print "'"+args[0]+"' pre-installed on the rsync server"
              
        
if __name__=="__main__":
    from DIRAC.Core.Base import Script
    Script.parseCommandLine()
    cli = SoftwareTagCli()
    cli.cmdloop()  

