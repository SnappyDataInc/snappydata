#!/usr/bin/env bash

#
# Copyright (c) 2016 SnappyData, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License. You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License. See accompanying
# LICENSE file.
#

# Start all snappy daemons - locator, lead and server on the nodes specified in the
# conf/locators, conf/leads and conf/servers files respectively

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

MEMBERS_FILE=members.txt

rm -rf $MEMBERS_FILE 2> /dev/null
touch $MEMBERS_FILE

# Load the Spark configuration
. "$sbin/spark-config.sh"
. "$sbin/snappy-config.sh"

# Start Locators
"$sbin"/snappy-locators.sh start "$@"

# Start Servers
"$sbin"/snappy-servers.sh start "$@"

# Start Leads
if [ "$1" != "rowstore" ]; then
  "$sbin"/snappy-leads.sh start
fi
