#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
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
# Graph the contents of the benchmark database using gnuplot
#

#set -x
GNUPLOT=$(type -p gnuplot)
if [[ !(-x $GNUPLOT) ]] ; then
    echo >&2 "'gnuplot' command not available, cannot graph results"
    exit 0
fi

database=".msgr-db"
while getopts "d:h" opt; do
  case $opt in
    d)
      database=$OPTARG
      ;;
    h | \?)
      echo "Usage: $0 [-d <database>]" >&2
      echo "  <database> defaults to '$database'" >&2
      exit 1
      ;;
  esac
done

for f in `find $database -name "*.csv"`; do
    gnuplot -p <<-EOF
        set bars 4
        set datafile separator ','
        plot '$f' using 0:3:2:4:xticlabels(1) with yerrorlines
EOF
done
