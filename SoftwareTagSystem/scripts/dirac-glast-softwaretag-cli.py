#!/bin/env python
import cmd
from GlastDIRAC.SoftwareTagSystem.Client import SoftwareTagClient

class SoftwareTagCli(cmd.Cmd):
    def __init__(self):
        self.client = SoftwareTagClient.SoftwareTagClient()
        cmd.Cmd.__init__()
    
    def do_add(self,args):
        """ add something 
            add tag <tag> site1,site2,site3
        """
        errorcount = 0
        argss = args.split()
        if (len(argss)==0):
            print self.do_meta.__doc__
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
            print self.do_meta.__doc__
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
        if errorcount!=0:
            print "Found errors, cannot continue"
            return

if __name__=="__main__":
    cli = SoftwareTagCli()
    cli.cmdloop()  
