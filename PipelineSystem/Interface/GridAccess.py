""" Grid Storage Access Library to be used instead of GPL.staging.

@author: V. Rolland (LUPM/IN2P3)
@author: S. Zimmer (OKC/SU)

"""
from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = False )
import DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
import os, time
from DIRAC.ConfigurationSystem.Client.Helpers.Operations    import Operations
from DIRAC.Core.Security.ProxyInfo                          import getProxyInfo
from DIRAC.DataManagementSystem.Client.ReplicaManager       import ReplicaManager
from DIRAC.DataManagementSystem.Client.ReplicaManager       import CatalogFile
from DIRAC.Core.Utilities.List                              import sortList

# Set up message logging

class stageGrid(object):
    """
    provides a container for all the storage elements. the base class any user needs to call
    implements a standard set of functions any user can call directly from pipeline code;
    """
    
    def __init__(self,dataDir):
        op = Operations("glast.org")
        self.stagingArea = dataDir
        self.listFileStaged = []    
        self.nbofSEtried =0
        self.userprefix = None
        self.prefixDest = None
        self.stagingDest = None
        SEtemporaryStaging = op.getValue( 'Pipeline/StorageElementTemporaryStaging', "" )
        self.listSEs = SEtemporaryStaging.split(',')
    
        self.SE = None
        self.log = gLogger.getSubLogger("GPL Staging")
        if self.__getPrefix()!=0:
            self.log.error("Failed to initialize staging.")
            raise Exception("Failed to initialize!")
        self.__pickRandomSE()
    
    def __getPrefix(self):
        op = Operations("glast.org")
        self.userprefix = None
        res = getProxyInfo()
        if res['OK']:
            if 'username' in res['Value']:
                user=res['Value']['username']
                self.userprefix="/glast.org/user/%s/%s/"%(user[0],user)
        else:
            self.log.error("Proxy could not be found")
            return 1
        task_category = os.environ["GPL_TASKCATEGORY"]
        if not task_category:
            task_category = op.getValue("Pipeline/TaskCategory",None)
        if not task_category:
            self.log.error("Could not find task category")
            return 1

        self.prefixDest = op.getValue("Pipeline/StorageElementBasePath",self.userprefix)
        self.stagingDest = self.prefixDest+"/"+task_category+"/"+os.environ['PIPELINE_TASK']+"/"+os.environ['PIPELINE_STREAM']
        return 0
   
    def getDestinationDir(self):
        return self.stagingDest
 
    def getStageDir(self):
        """
        @return: string - stage directory
        """
        return self.stagingArea
        
      
    def __pickRandomSE(self):
        """
        intended to be private
        pick SE from list of storage elements
        """
        from random import choice
        try:
            self.SE = choice(self.listSEs)
        except IndexError:
            self.SE = None
        self.log.info('__pickRandomSE -> self.SE :')
        self.log.info(self.SE) 
            
    def stageOut(self,outfile):
        """
        adds a file to list of files to be written to SE
        @param outfile:
        @return: None
        """
        self.listFileStaged.append( [self.stagingArea+"/"+outfile , self.stagingDest+"/"+outfile] )
        return self.stagingArea+"/"+outfile

    def reset(self):
        """
        resets all internal lists/dicts
        @return:
        """
        self.listFileStaged = [] ;

    def finish(self):
        """
        after having set all the files, this one does all the job
        @return:
        """
        rc = 0
        rm = ReplicaManager()
        for item in self.listFileStaged:   
            #print("SE '"+self.SE+"' == : '"+str(self.SE == "False")+"'")
            if not self.SE:
                self.log.info("No SE available for '"+item[0]+"'")
                rc+=1
                continue
            else:
                self.log.info("Trying to store '"+item[0]+"' in SE : '"+self.SE+"' ...")
                result = rm.putAndRegister( item[1], item[0], self.SE)
                if not result['OK']:
                    self.log.info('ERROR %s' % ( result['Message'] ))

                    self.log.info("Wait 5sec before trying again...")
                    time.sleep(5)
                    result = rm.putAndRegister( item[1], item[0], self.SE)
                    if not result['OK']:
                        self.log.info('ERROR %s' % ( result['Message'] ))
                        while   not result['OK'] :
                            self.listSEs.remove(self.SE) # make sure not to pick the same SE again.    
                            self.__pickRandomSE()
                            if not self.SE:
                                break
                            self.log.info("Trying with another SE : '"+self.SE+"' . In 5sec...")
                            time.sleep(5)
                            result = rm.putAndRegister( item[1], item[0], self.SE)
                            if result['OK']:
                                self.log.info("file stored : '"+item[1]+"' in '"+self.SE+"'")
                            else:
                                self.log.error("ERROR : failed to store the file '"+item[1]+"' ...")
                                rc += 1        

        return rc

    def dumpStagedFiles(self):
        """
        dump list of files along with their intended SEs
        @return:
        """
        for item in self.listFileStaged:
            self.log.info("* "+item[0]+" - "+item[1])

    def listStageDir(self):
        """
        list content of current staging dir
        @return:
        """
        for item in self.listFileStaged:
            self.log.info("* "+item[0])

    def getChecksums(self,printflag=None):
        """@brief Return a dictionary of: [stagedOut file name,[length,checksum] ].  Call this after creating file(s), but before finish(), if at all.  If the printflag is set to 1, a brief report will be sent to stdout."""
        cksums = {}
        # Compute checksums for all stagedOut files
        
        print("Calculating 32-bit CRC checksums for stagedOut files")
        #print "Calculating 32-bit CRC checksums for stagedOut files"
        
        for stagee in self.listFileStaged:
            File = stagee[0]
            if os.access(File,os.R_OK):
                cksum = "cksum "+File
                fd = os.popen(cksum,'r')    # Capture output from unix command
                foo = fd.read()             # Calculate checksum
                rc = fd.close()
                if rc != None:
                    self.log.error("Checksum error: return code =  "+str(rc)+" for file "+File)
                    #print "Checksum error: return code =  "+str(rc)+" for file "+file
                else:
                    cksumout = foo.split()
                    cksums[cksumout[2]] = [cksumout[0],cksumout[1]]
            else:
                self.log.error("Checksum error: file does not exist, "+File)
        return cksums

        
        
        
def getOutputData(baseDir):
    exitCode = 0    
    listOutputData = []
    res = getProxyInfo( False, False )
    if not res['OK']:
        gLogger.error( "Failed to get client proxy information.", res['Message'] )
        DIRAC.exit( 2 )
        

    print  'Will search for files in %s' % baseDir
    activeDirs = [baseDir]
    import fnmatch
    # ######################################################################################################## #
    # before is from dirac-dms-user-lfns
    rm = ReplicaManager()
    allFiles = []
    while len( activeDirs ) > 0:
        currentDir = activeDirs[0]
        res = rm.getCatalogListDirectory( currentDir, False )
        activeDirs.remove( currentDir )
        if not res['OK']:
            gLogger.error( "Error retrieving directory contents", "%s %s" % ( currentDir, res['Message'] ) )
        elif res['Value']['Failed'].has_key( currentDir ):
            gLogger.error( "Error retrieving directory contents", "%s %s" % ( currentDir, res['Value']['Failed'][currentDir] ) )
        else:
            dirContents = res['Value']['Successful'][currentDir]
            subdirs = dirContents['SubDirs']    
            for subdir, metadata in subdirs.items():
                activeDirs.append( subdir )
            for filename, fileInfo in dirContents['Files'].items():
                metadata = fileInfo['MetaData']
                if fnmatch.fnmatch( filename, "*"):
                    allFiles.append( filename )
            files = dirContents['Files'].keys()
    # ######################################################################################################## #
    # get file
    ntries = 3
    for lfn in sortList( allFiles ):
        success = False
        while ntries:
            print " - getting '"+lfn+"'... ",
            result = rm.getFile( lfn )
            if not result['OK']:
                print 'ERROR %s' % ( result['Message'] )
                print 'sleep for 10s and try again.'
                time.sleep(10)
            else:
                success = True
                listOutputData.append(result['Value']['Successful'][lfn])
                print "OK"
                break
            ntries -= 1
        if not success:
            print 'ERROR could not get file after %i trials. Giving up :('%ntries
            exitCode = 2
    # ######################################################################################################## #
    if exitCode:
        return S_ERROR("Failed to finish operations.")
    return S_OK(listOutputData);


    
    
def removeOutputData(baseDir):
    res = getProxyInfo( False, False )
    if not res['OK']:
        gLogger.error( "Failed to get client proxy information.", res['Message'] )
        return S_ERROR("Failed to get client proxy information.", res['Message'])

    # ######################################################################################################## #
    rm = ReplicaManager()
    
    result = rm.cleanLogicalDirectory(baseDir);
    print "Ignore the message about the file '"+baseDir+"dirac_directory'"
      
    if not result['OK']:
        print 'ERROR: %s' % (result['Message'] )
        return S_ERROR("Failed to Suppress the directory : '"+baseDir+"'")

    return S_OK(baseDir + " has been supressed")

    
    
    
def cleanOldOutputData(baseDir):
    res = getProxyInfo( False, False )
    if not res['OK']:
        gLogger.error( "Failed to get client proxy information.", res['Message'] )
        return S_ERROR("Failed to get client proxy information.", res['Message'])

    cf = CatalogFile()
    result = cf.getCatalogExists(baseDir) 
    
    if not result['OK']:
        print 'ERROR: %s' % (result['Message'] )
        return S_ERROR("Failed to reach the directory : '"+baseDir+"'")
        
    if result['Value']['Successful'][baseDir]:
        res = removeOutputData(baseDir)
        if not result['OK']:
            return S_ERROR(result['Message'])
        else:
            return S_OK(baseDir + " has been supressed")
        
    return S_OK("No previous outpudata found.")
    

