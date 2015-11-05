#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Starts a server instance on each machine specified in the conf/servers file.
sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

. "$sbin/snappy-config.sh"
. "$sbin/spark-config.sh"

. "$SPARK_PREFIX/bin/load-snappy-env.sh"
. "$SPARK_PREFIX/bin/load-spark-env.sh"

# Launch the slaves
if [[ $@ == *"start"* ]] ; then
  "$sbin/snappy-nodes.sh" server cd "$SPARK_HOME" \; "$sbin/snappy-server.sh"  $@ $SERVER_STARTUP_OPTIONS
else
  "$sbin/snappy-nodes.sh" server cd "$SPARK_HOME" \; "$sbin/snappy-server.sh"  $@
fi