#!/bin/env python
import cmd, sys
from GlastDIRAC.SoftwareTagSystem.Client import SoftwareTagClient

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
        else:
            print "ERROR parsing command"
            print self.do_get.__doc__
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
            print self.do_get.__doc__
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
            status = 'OK'
            if len(argss)>1:
                status = argss[1]
            # expect site as input
            sites = args[0].split(",")
            tags = []
            for site in sites:
                res = self.client.getTagsAtSite(site,status=status)
                if not res['OK']:
                    print "Failed to get tags for site %s; %s"%(site,res['Message'])
                    errorcount+=1
                else:
                    tags+=res['Value']
            return tags
        
        elif option == "site":
            tag = args[0]
            status = 'OK'
            if len(argss)>1:
                status = argss[1]
            res = self.client.getSitesForTag(tag,status='OK')
            if not res['OK']:
                print "Failed to get tags for site %s; %s"%(site,res['Message'])
            else:
                return res['Value']
        
        elif option == "all":
            raise NotImplementedError
            
        else:
            print "ERROR parsing command"
            print self.do_get.__doc__
            return
        if errorcount!=0:
            print "Found errors, cannot continue"
            return
    def do_quit(self,args):
        """ quit """
        sys.exit(0)
    def do_exit(self,args):
        """ quit """
        sys.exit(0)
    
if __name__=="__main__":
    cli = SoftwareTagCli()
    cli.cmdloop()  
