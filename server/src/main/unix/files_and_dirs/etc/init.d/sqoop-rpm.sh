#!/bin/bash
#!/bin/bash
#
# Licensed to STRATIO (C) under one or more contributor license agreements.
# See the NOTICE file distributed with this work for additional information
# regarding copyright ownership.  The STRATIO (C) licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

#
#   /etc/rc.d/init.d/sqoop
#
#   Sqoop service
#
### BEGIN INIT INFO
# Provides:          Sqoop
# Required-Start:    $network $remote_fs $named
# Required-Stop:     $network $remote_fs $named
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Starts Sqoop
# Description:       Starts Sqoop sso using start-stop-daemon
### END INIT INFO

# Source function library.
. /etc/init.d/functions

# Only configuration needed here
NAME="sqoop"
DESC="Sqoop single sign on service"

PATH=/bin:/usr/bin:/sbin:/usr/sbin
VARIABLES="/etc/default/$NAME-variables"
FUNCTIONS="/etc/default/$NAME-functions"

# Source function library.
. /etc/rc.d/init.d/functions

# Source default configuration
if [ -r $VARIABLES ]; then
    . $VARIABLES
fi

# Source check functions & validate service configuration
. $FUNCTIONS && validateService

start() {
    validateStart
    echo -n "Starting $NAME: "
    daemon --user $USER --pidfile $PIDFILE "$DAEMON start $DAEMON_OPTS"
    echo
    touch $LOCKFILE
}

stop() {
    validateStop
    echo -n "Shutting down $NAME: "
    killproc -p $PIDFILE $NAME
    local exit_status=$?
    echo
    rm -f $LOCKFILE
    return $exit_status
}

restart(){
    validateRestart
    stop
    sleep 1
    start
}

condrestart(){
    if [ -f /var/lock/subsys/$NAME ]; then
        restart || { echo "won't restart: $NAME is stopped" ; exit 1 ; }
    fi
}

getStatus(){
    status -p $PIDFILE $NAME -l $LOCKFILE
}

usage(){
    echo "Usage: $NAME {start|stop|status|restart|reload|force-reload|condrestart}"
    exit 1
}

case "$1" in
    start)                          start ;;
    stop)                           stop ;;
    status)                         getStatus ;;
    restart|reload|force-reload)    restart ;;
    condrestart)                    condrestart ;;
    *)                              usage ;;
esac
exit $?