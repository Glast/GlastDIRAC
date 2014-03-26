# helper script to get pilot info on jobs that get stalled...
from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = True )
import DIRAC
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin
from DIRAC import gConfig, gLogger, exit as dexit

def getPilotLoggingInfo(gridID):
    output = ""
    diracAdmin = DiracAdmin()
    result = diracAdmin.getPilotLoggingInfo( gridID )
    if not result['OK']:
        output = 'ERROR retrieving pilot logging info, %s'%str(result['Message'])
        gLogger.error(output)
    else:
        output = result['Value']
    return output

def removeStr(string,filter):
    out = ""
    while filter in string:
        out = string.replace(filter,"")
        string = out
    return out
            
def ingestPilot(info,key):
    out = None
    outD = {}
    lines = info.split("\n")
    for line in lines:
        tLine = removeStr(line,"\t")
        if not tLine.startswith("**") or tLine.startswith("--"):
            if "=" in tLine:
                fields = tLine.split(" = ")
                tkey = fields[0]
                tvalue = fields[1]
                theKey = tkey
                if " " in tkey:
                    theKey = removeStr(tkey," ")
                if theKey in outD: outD[theKey].append(tvalue)
                else: outD[theKey]=[str(tvalue)]
    myKey = key
    if " " in key:
        myKey = removeStr(key," ")
    if myKey in outD:
        out = str(outD[myKey])
    return out

exitCode = 0
iKey = "FailureReason"
args = Script.getPositionalArgs()
if len(args):
    iKey = args[0]
print '*INFO* looking for key %s'%iKey
# first, get all jobs which are marked as stalled
status = "Failed"
minor_stat = "Job stalled: pilot not running"
owner = "zimmer"
dirac = Dirac()
jobs = []
conditions = {"Status":status,"MinorStatus":minor_stat,"Owner":owner}
res = dirac.selectJobs( status = status,
                       minorStatus = minor_stat,
                       owner = owner)
if not res['OK']:
    gLogger("ERROR retrieving jobs")
    gLogger(res["Message"])
    exitCode = 2
else:
    conds = []
    for n, v in conditions.items():
        if v:
            conds.append( '%s = %s' % ( n, v ) )
    jobs = res['Value']
pilot_refs = {}
for job in jobs:
    # next get pilot refs
    key = job
    res = dirac.parameters( job, printOutput = False )
    if not res['OK']:
        gLogger.error("ERROR retrieving job parameters")
        gLogger.error(res['Message'])
        exitCode = 3
    else:
        val = res['Value']['Pilot_Reference']
        pilot_refs[key]=val
# next is to look through the pilots
pilot_info = {}
for i,job in enumerate(pilot_refs):
    gLogger.info("Pilot for %s: %s"%(str(job),str(pilot_refs[job])))
    pilotInfo = getPilotLoggingInfo(pilot_refs[job])
    #gLogger.info("%s,%s"%(str(job),str(pilotInfo)))
    val = ingestPilot(pilotInfo,iKey)
    print "%s\t:\t%s"%(str(job),str(val))
    if not val is None:
        pilot_info[job]=val
    
DIRAC.exit( exitCode )