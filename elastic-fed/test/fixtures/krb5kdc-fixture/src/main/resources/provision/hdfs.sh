#!/bin/bash
#
# Copyright (c) 2021, Alibaba Group;
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Modifications Copyright Havenask Contributors. See
# GitHub history for details.
#

set -e

addprinc.sh "havenask"
#TODO(Havenask): fix username
addprinc.sh "hdfs/hdfs.build.havenask.org"

# Use this as a signal that setup is complete
python3 -m http.server 4444 &

sleep infinity
