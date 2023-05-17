#!/bin/env python

import re
import sys
import os
import socket
from optparse import OptionParser
import tempfile
import subprocess
import json
import time
import general_search_starter
import re

class GeneralSearchUpdateCmd(general_search_starter.GeneralSearchStartCmd):
    '''
local_search_update.py
    {-i index_dir           | --index=index_dir}
    {-c config_dir          | --config=config_dir}
    {-p port_start          | --prot=prot_start}
    {-z zone_name           | --zone=zone_name}
    {-b binary_path         | --binary=binary_path}

options:
    -i index_dir,     --index=index_dir              : required, query string
    -c config_dir,    --config=config_dir            : required, qrs http/tcp address,
    -p port_start,    --prot=port_start              : optional, http port, arpc port is start +1 (total port may use start + n*3 ) [default 12000].
    -p zone_name,     --zone=zone_name               : optional, special zone to start
    -b binary_path,   --binary=binary_path           : optional, special binary path to load

examples:
    ./local_search_update.py -i /path/to/index -c path/to/config 
    ./local_search_update.py -i /path/to/index -c path/to/config -p 12345
    '''

    def __init__(self):
        super(GeneralSearchUpdateCmd, self).__init__()

    def usage(self):
        print self.__doc__ % {'prog' : sys.argv[0]}

    def addOptions(self):
        super(GeneralSearchUpdateCmd, self).addOptions()

    def parseParams(self, optionList):
        return super(GeneralSearchUpdateCmd, self).parseParams(optionList)

    def checkOptionsValidity(self, options):
        return super(GeneralSearchUpdateCmd, self).checkOptionsValidity(options)

    def initMember(self, options):
        return super(GeneralSearchUpdateCmd, self).initMember(options)

    def run(self):
        if not self._getPortListArray():
            errmsg= "get port listArray failed"
            print errmsg
            return ("", errmsg, -1)
        if self.role == "all" or self.role == "searcher":
            if not self._updateSearchConfig():
                errmsg= "update search failed"
                print errmsg
                return ("", errmsg, -1)
        if self.role == "all" or self.role == "qrs":
            if not self._updateQrsConfig():
                errmsg= "update qrs failed"
                print errmsg
                return ("", errmsg, -1)
        print "update success"
        return ("", "", 0)

    def _getPortListArray(self):
        print(self.pidFile)
        if not os.path.exists(self.portFile):
            return False
        self.portListArray = []
        for line in open(self.portFile).readlines():
            line = line.strip()
            ports = []
            for port in line.split(" "):
                match = re.search("[0-9]+", port)
                if match:
                    ports.append(int(match.group(0)))
                    self.portList.append(int(match.group(0)))
            print(ports)
            if len(ports) != 2:
                return False
            self.portListArray.append((ports[0], ports[1], ports[1]))
        if self.role == "searcher" or self.role == "all":
            self.searcher_port_list = self.portListArray
        if self.role == "qrs" or self.role == "all":
            self.qrs_port_list = ports
        return True
        pass
    
    def _updateSearchConfig(self):
        zoneNames = self._getNeedStartZoneName()
        tableInfos = self._genTargetInfos(zoneNames)
        if not self._loadSearcherTarget(tableInfos):
            return False
        return True

    def _updateQrsConfig(self):
        if not self._loadQrsTarget():
            return False
        return True
    

    def _loadQrsTarget(self, timeout = 300):
        with open(os.path.join(self.workdir, "readyZones.json"), "r") as f:
            self.readyZones = json.load(f)
        # print(self.readyZones.values())
        target = {
            "service_info" : {
                "cm2_config" : {
                    "local" : self.readyZones.values()
                },
                "part_count" : 0,
                "part_id" : 0,
                "zone_name": "qrs"
            },
            "biz_info" : {
                "default" : {
                    "config_path" : self.createConfigLink('qrs', 'biz', 'default', self.onlineConfigPath)
                }
            },
            "table_info" : {
            },
            "clean_disk" : False
        }
        targetStr = json.dumps(target)
        requestSig = targetStr
        globalInfo = {"customInfo":targetStr}
        targetRequest = { "signature" : requestSig,
                          "customInfo" : targetStr,
                          "globalCustomInfo": json.dumps(globalInfo)
        }
        portList = self._getQrsPortList()
        httpArpcPort = portList[0]
        arpcPort = portList[1]
        address = "%s:%d" %(self.ip, httpArpcPort)
        while timeout > 0:
            retCode, out, err, _ = self.curl(address, "/HeartbeatService/heartbeat", targetRequest)
            if retCode != 0:
                print "set qrs target %s failed." % targetStr
                time.sleep(5)
                timeout -= 5
                continue
            response = json.loads(out)
            if response["signature"] == requestSig:
                print "qrs is ready for search, http port %s, arpc port %s" % (httpArpcPort, arpcPort)
                return True
            time.sleep(5)
            timeout -= 5
        return timeout > 0
    
    def _loadSearcherTarget(self, targetInfos, timeout = 300):
        self.readyZones = {}
        while timeout > 0:
            count = 0
            for targetInfo in targetInfos:
                portList = self._getSearcherPortList(count)
                print("portList", portList)
                count += 1
                zoneName = targetInfo[0]
                partId = targetInfo[1]
                roleName = zoneName + "_" + str(partId)
                if self.readyZones.has_key(roleName) :
                    continue
                target = targetInfo[2]
                httpArpcPort = portList[0]
                arpcPort = portList[1]
                targetStr = json.dumps(target)
                requestSig = targetStr
                globalInfo = {"customInfo":targetStr}
                targetRequest = { "signature" : requestSig,
                                  "customInfo" :targetStr,
                                  "globalCustomInfo": json.dumps(globalInfo)
                }
                address = "%s:%d" %(self.ip, httpArpcPort)
                retCode, out, err, _ = self.curl(address, "/HeartbeatService/heartbeat", targetRequest)
                if retCode != 0:
                    print "set target %s failed." % targetStr
                    continue
                response = json.loads(out)
                infos = []
                if response["signature"] == requestSig:
                    serviceInfo = json.loads(response["serviceInfo"])
                    infos = serviceInfo["cm2"]["topo_info"].strip('|').split('|')
                    for info in infos:
                        splitInfo = info.split(':')
                        localConfig = {}
                        localConfig["biz_name"] = splitInfo[0]
                        localConfig["part_count"] = int(splitInfo[1])
                        localConfig["part_id"] = int(splitInfo[2])
                        localConfig["version"] = int(splitInfo[3])
                        localConfig["ip"] = self.ip
                        localConfig["tcp_port"] = arpcPort
                        if splitInfo[0] == 'default':
                            self.readyZones[roleName] = localConfig
                            print "searcher [%s] is ready for search, topo [%s]" % (roleName, json.dumps(localConfig))
                        else:
                            zoneName = splitInfo[0] + "_" + str(partId)
                            self.readyZones[zoneName] = localConfig
                            print "searcher [%s] is ready for search, topo [%s]" % (zoneName, json.dumps(localConfig))
                elif timeout <= 0:
                    print "searcher [%s] load target [%s] failed" % (roleName, targetStr)
                if len(infos) > 0 and len(targetInfos) == (len(self.readyZones) / len(infos)):
                    print "all searcher is ready."
                    with open(os.path.join(self.workdir, "readyZones.json"), "w") as f:
                        json.dump(self.readyZones, f)
                    return True

            time.sleep(5)
            timeout -= 5
        return timeout > 0

if __name__ == '__main__':
    cmd = GeneralSearchUpdateCmd()
    if len(sys.argv) < 3:
        cmd.usage()
        sys.exit(-1)
    if not cmd.parseParams(sys.argv):
        cmd.usage()
        sys.exit(-1)
    data, error, code = cmd.run()
    if code != 0:
        if error:
            print error
        sys.exit(code)
    sys.exit(0)
