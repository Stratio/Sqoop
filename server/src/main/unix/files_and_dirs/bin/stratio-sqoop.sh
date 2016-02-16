#!/bin/bash
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

# Get options
while getopts "p:l:" option; do
  case $option in
     p)      PIDFILE=$OPTARG ;;
     l)      LOGFILE=$OPTARG ;;
     *)      echo "Unknown option" ; exit 1 ;;
  esac
done

# Set default values
LOGFILE=${LOGFILE:-"/var/log/sds/sqoop-server/sqoop.log"}
PIDFILE=${PIDFILE:-"/var/run/sds/sqoop-server.pid"}

$DIR/run.sh >>$LOGFILE 2>&1 & echo $! >$PIDFILE