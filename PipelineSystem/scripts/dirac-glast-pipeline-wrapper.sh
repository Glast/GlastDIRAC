#!/bin/bash
# implement coredump limit early on. / 1MB max -- setting it here should propagate everywhere
#ulimit -c 1M
 
trap gotINT INT
trap gotTERM TERM
trap gotQUIT QUIT
trap gotUSR1 USR1
trap gotUSR2 USR2
trap gotXCPU XCPU
 
gotINT()
{
    echo "INT signal received."
    pipeline_signals="${pipeline_signals} INT"
}
gotTERM()
{
    echo "TERM signal received."
    pipeline_signals="${pipeline_signals} TERM"
    # I hate to do this, but we need time to clean up!
    pidToKill=`pstree -p $$ | sed -n -r "s/.*time\([0-9]+\)---\w*\(([0-9]+)\)/\1/p"`
    if [ -n "$pidToKill" ]; then
      echo "Pipeline killing $pidToKill because time SIGTERM received"
      kill -9 $pidToKill
    fi
}
gotUSR2()
{
    echo "USR2 signal received."
    pipeline_signals="${pipeline_signals} USR2"
}
gotUSR1()
{
    echo "USR1 signal received."
    pipeline_signals="${pipeline_signals} USR1"
}
gotQUIT()
{
    echo "QUIT signal received."
    pipeline_signals="${pipeline_signals} QUIT"
}
gotXCPU()
{
    echo "XCPU signal received."
    pipeline_signals="${pipeline_signals} XCPU"
}
 
if [ "$1" = "rerun" ] ; then . pipeline_env; fi
 
# create the beginning of pipeline summary file
 
MSG="${MSG}ProcessInstance: ${PIPELINE_PROCESSINSTANCE} \n"
MSG="${MSG}Host: DIRAC-WORKER\n"
MSG="${MSG}StartTime: `date`\n"
MSG="${MSG}WorkDir: `pwd`\n"
MSG="${MSG}LogFile: ${JOBCONTROL_LOGFILE}\n"
# Tell the server we are done!
gotEXIT()
{
    END="EndTime: `date`\n"
    END="${END}Signals: ${pipeline_signals}\n"
    END="${END}Status: Ended"
    if [ -e ${PIPELINE_SUMMARY} ] ; then
       echo -e "${END}" >> ${PIPELINE_SUMMARY}
       if [ "$1" != "rerun" ] ; then
           dirac-glast-pipeline-sendmail -T ${PIPELINE_TOADDRESS} -F ${PIPELINE_FROMADDRESS} -S ${PIPELINE_PROCESSINSTANCE} -f "${PIPELINE_SUMMARY}"
           fi
    else
       if [ "$1" != "rerun" ] ; then
           dirac-glast-pipeline-sendmail -T ${PIPELINE_TOADDRESS} -F ${PIPELINE_FROMADDRESS} -S ${PIPELINE_PROCESSINSTANCE} -B "${MSG}\n${END}"
           fi
    fi
}
trap gotEXIT EXIT
 
# Tell the server we have started!
if [ "$1" != "rerun" ] ; then
    dirac-glast-pipeline-sendmail -T ${PIPELINE_TOADDRESS} -F ${PIPELINE_FROMADDRESS} -S ${PIPELINE_PROCESSINSTANCE} -B "${MSG}\nStatus: Started"
fi
echo "WE STARTED: \n ${MSG}"
 
# Note this is the first attempt to write to the working dir. Will fail if no disk space.
export PIPELINE_SUMMARY=`pwd`/pipeline_summary 
echo -e "${MSG}" > ${PIPELINE_SUMMARY}

 
# Support for allowing user to re-run interactively
if [ "$1" != "rerun" ] ; then export -p > pipeline_env; fi
 
env | sort
 
# Some functions for use by users
function pipelineSet { echo "Pipeline.$1: $2" >> ${PIPELINE_SUMMARY}; }
function pipelineCreateStream { echo "PipelineCreateStream.$1.$2: $3" >> ${PIPELINE_SUMMARY}; }
export -f pipelineSet
export -f pipelineCreateStream
 
# run the user code
if [ -e script ] ; then chmod +x script; fi
/usr/bin/time -f "Elapsed: %e\nUser: %U\nSystem: %S" -a -o ${PIPELINE_SUMMARY} ${PIPELINE_COMMAND}
 
# store the exit code
RC=$?
 
# Complete the summary file
echo "ExitCode: ${RC}" >> ${PIPELINE_SUMMARY}
exit ${RC}