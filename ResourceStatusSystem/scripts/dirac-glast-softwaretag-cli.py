#!/bin/env python
import cmd, sys
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
        get tag <site1,site2,site3> - lists tags at site 1 -3
        get site <tag> - lists sites supporting tag
        get all - lists everything.
        """
        errorcount = 0
        argss = args.split()
        if (len(argss)==0):
            print self.do_get.__doc__
            return
        option = argss[0]
        del argss[0]
        if option == "tag":
            tags = []
            status = 'Valid'
            if len(argss)>1:
                status = argss[1]
            # expect site as input
            sites = argss[0].split(",")
            tags = []
            for site in sites:
                res = self.client.getTagsAtSite(site,status=status)
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
            status = 'Valid'
            if len(argss)>1:
                status = argss[1]
            res = self.client.getSitesForTag(tag,status=status)
            if not res['OK']:
                print "Failed to get sites for tag %s; %s"%(tag,res['Message'])
            else:
                sites = res['Value']
                print "Found the following sites:"
                for site in sites:
                    print site
        
        elif option == "all":
            raise NotImplementedError
            
        else:
            print "ERROR parsing command"
            print self.do_get.__doc__
            return
        if errorcount!=0:
            print "Found errors, cannot continue"
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
        
    def do_quit(self,args):
        """ quit """
        sys.exit(0)
        
    def do_exit(self,args):
        """ quit """
        sys.exit(0)
    
    def do_resettag(self,argss):
        """ forget tag (drop in DB)
            *** USE WITH ABSOLUTE CARE!!! ***
            *** only used by OPERATORS ***
            resettag tag
        """
        args = argss.split()
        if len(args)<1:
            print self.do_resettag().__doc__
            return
        tag = args[0]
        completed = False
        ces_removed = 0
        while not completed:
            res = self.client.cleanTagAtSite(tag,"ALL")
            if not res['OK']:
                print "Failed to reset %s: %s"%(tag,res['Message'])
            elif res['OK'] and len(res['Value']['Failed'])!=0:
                print 'Failed to reset tag %s in %i CEs'%(tag,len(res['Value']['Failed']))
            else:
                completed = True
                ces_removed+=len(res['Value']['Successful'])
        print "Cleaned %s from %i CEs"%(tag,ces_removed)
        
if __name__=="__main__":
    from DIRAC.Core.Base import Script
    Script.parseCommandLine()
    cli = SoftwareTagCli()
    cli.cmdloop()  