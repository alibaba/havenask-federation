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
import random

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
            if len(ports) != 3:
                return False

            item = general_search_starter.PortListItem()
            item.ports = (ports[0], ports[1], ports[2])
            self.portListArray.append(item)
        if self.role == "searcher" or self.role == "all":
            self.searcher_port_list = self.portListArray
        if self.role == "qrs" or self.role == "all":
            self.qrs_port_list = ports
        return True
        pass

    def _updateSearchConfig(self):
        zoneNames = self._getNeedStartZoneName()
        tableInfos, createCatalogRequest = self._genTargetInfos(zoneNames, replica = self.searcherReplica)
        if 0 != self._loadSearcherTarget(tableInfos):
            return False
        return True

    def _updateQrsConfig(self):
        if 0 != self._loadQrsTarget():
            return False
        return True


    def _loadQrsTarget(self, timeout = 300):
        with open(os.path.join(self.workdir, "readyZones.json"), "r") as f:
            self.gigInfos = json.load(f)

        terminator = general_search_starter.TimeoutTerminator(timeout)
        bizs = os.listdir(self.onlineConfigPath)
        bizInfo = {}
        if self.enableMultiBiz:
            for biz in bizs:
                onlineConfig = self.genOnlineConfigPath(self.onlineConfigPath, biz)
                bizInfo[biz] = {
                    "config_path" : self.createConfigLink('qrs', 'biz', biz, onlineConfig)
                }
                if biz in self.modelBiz:
                    bizInfo[biz]["custom_biz_info"] = {
                            "biz_type" : "model_biz"
                    }
        else:
            onlineConfig = self.genOnlineConfigPath(self.onlineConfigPath, bizs[0])
            bizInfo['default'] = {
                "config_path" : self.createConfigLink('qrs', 'biz', 'default', onlineConfig)
            }
        tableInfos = {}
        zoneName = "qrs"
        portList = self.getQrsPortList()
        httpArpcPort = portList[0]
        arpcPort = portList[1]
        grpcPort = portList[2]
        address = "%s:%d" %(self.ip, httpArpcPort)
        arpcAddress = "%s:%d" %(self.ip, arpcPort)

        if self.enableLocalAccess:
            zoneNames = self._getNeedStartZoneName()
            targetInfos, createCatalogRequest = self._genTargetInfos(zoneNames, 1, True)
            tableInfos = targetInfos[0][3]["table_info"]
            zoneName = zoneNames[0]
            ret = self.createCatalog(createCatalogRequest)
            if ret != 0:
                print "create catalog %s failed." % createCatalogRequest
                return -1

        gigInfos = self.gigInfos

        target = {
            "service_info" : {
                "cm2_config" : {
                    "local" : gigInfos.values()
                },
                "part_count" : 1,
                "part_id" : 0,
                "zone_name": zoneName
            },
            "biz_info" : bizInfo,
            "table_info" : tableInfos,
            "clean_disk" : False,
            "catalog_address" : arpcAddress,
            "target_version" : self.targetVersion
        }
        targetStr = ''
        targetStr = json.dumps(target)
        requestSig = targetStr
        globalInfo = {"customInfo":targetStr}
        targetRequest = { "signature" : requestSig,
                          "customInfo" : targetStr,
                          "globalCustomInfo": json.dumps(globalInfo)
        }
        lastRespSignature = ""
        while True:
            timeout = terminator.left_time()
            if timeout <= 0:
                break
            log_file = os.path.join(self.localSearchDir, "qrs", 'logs/ha3.log')
            log_state = self.check_log_file(log_file)
            if log_state != 0:
                return log_state

            retCode, out, err, status = self.curl(address, "/HeartbeatService/heartbeat", targetRequest, timeout=timeout)
            if retCode != 0: #qrs core
                print "set qrs target [{}] failed, address[{}] ret[{}] out[{}] err[{}] status[{}] left[{}]s".format(targetStr, address, retCode, out, err, status, terminator.left_time())
                return -1
            response = json.loads(out)
            if "signature" not in response:
                print "set qrs target response invalid [{}], continue...".format(out)
                continue
            lastRespSig = response["signature"]
            if lastRespSig == requestSig:
                print "start local search success\nqrs is ready for search, http arpc grpc port: %s %s %s" % (httpArpcPort, arpcPort, grpcPort)
                return 0
            time.sleep(5)
        print 'load qrs target timeout [{}]s left[{}]s resp[{}] request[{}]'.format(terminator.raw_timeout(), terminator.left_time(), lastRespSig, requestSig)
        return -1

    def _loadSearcherTarget(self, targetInfos, timeout = 300):
        self.gigInfos = {}
        terminator = general_search_starter.TimeoutTerminator(timeout)
        readyTarget = set()
        while True:
            timeout = terminator.left_time()
            if timeout <= 0:
                break
            count = 0
            for targetInfo in targetInfos:
                portList = self._getSearcherPortList(count)
                count += 1
                zoneName = targetInfo[0]
                partId = targetInfo[1]
                replicaId = targetInfo[2]
                roleName = self.genRoleName(targetInfo)
                if roleName in readyTarget:
                    continue

                target = targetInfo[3]
                if self.options.searcherSubscribeConfig:
                    target['service_info']['cm2_config'] = json.loads(self.options.searcherSubscribeConfig)
                httpArpcPort = portList[0]
                arpcPort = portList[1]
                grpcPort = portList[2]
                target["target_version"] = self.targetVersion
                targetStr = json.dumps(target)
                requestSig = targetStr
                globalInfo = {"customInfo":targetStr}
                targetRequest = { "signature" : requestSig,
                                  "customInfo" :targetStr,
                                  "globalCustomInfo": json.dumps(globalInfo)
                }
                log_file = os.path.join(self.localSearchDir, roleName, 'logs/ha3.log')
                log_state = self.check_log_file(log_file)
                if log_state != 0:
                    return log_state
                address = "%s:%d" %(self.ip, httpArpcPort)
                retCode, out, err, status = self.curl(address, "/HeartbeatService/heartbeat",
                                                      targetRequest, timeout=timeout)

                if retCode != 0:  # binary core
                    print "set searcher target [{}] failed. role[{}] address[{}] ret[{}] out[{}] err[{}] status[{}] left[{}]s".format(
                        targetRequest, roleName, address, retCode, out, err, status, terminator.left_time())
                    return -1
                response = json.loads(out)
                infos = []
                if "signature" not in response:
                    print "set searcher target response invalid [{}] role [{}], continue...".format(out, roleName)
                    continue
                if response["signature"] == requestSig:
                    serviceInfo = json.loads(response["serviceInfo"])
                    infos = serviceInfo["cm2"]["topo_info"].strip('|').split('|')
                    random_version = random.randint(1,100000)
                    for info in infos:
                        splitInfo = info.split(':')
                        localConfig = {}
                        localConfig["biz_name"] = splitInfo[0]
                        localConfig["part_count"] = int(splitInfo[1])
                        localConfig["part_id"] = int(splitInfo[2])
                        localConfig["version"] = int(splitInfo[3]) + random_version
                        localConfig["ip"] = self.ip
                        localConfig["tcp_port"] = arpcPort
                        if grpcPort != 0:
                            localConfig["grpc_port"] = grpcPort
                            localConfig["support_heartbeat"] = True
                        gigKey = roleName + "_" + splitInfo[0] + "_" + str(splitInfo[2])
                        self.gigInfos[gigKey] = localConfig
                    readyTarget.add(roleName)
                    print "searcher [%s] is ready for search, topo [%s]" % (roleName, json.dumps(localConfig))
                if len(targetInfos) == len(readyTarget):
                    print "all searcher is ready."
                    with open(os.path.join(self.workdir, "readyZones.json"), "w") as f:
                        json.dump(self.gigInfos, f)
                    return 0
            time.sleep(5)
        print 'load searcher [{}] target [{}] timeout [{}]s left [{}]s readyTarget[{}]'.format(
            zoneName,
            targetStr,
            terminator.raw_timeout(),
            terminator.left_time(),
            readyTarget)
        return -1

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
