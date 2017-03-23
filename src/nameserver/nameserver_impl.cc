// Auther: zackliu1995@hotmail.com

#include "nameserver_impl.h"

#include <set>
#include <map>
#include <sstream>
#include <cstdlib>

#include <gflags/gflags.h>
#include <sofa/pbrpc/pbrpc.h>

#include <common/counter.h>
#include <common/logging.h>
#include <common/string_util.h>

#include "block_mapping.h"
#include "chunkserver_manager.h"
#include "namespace.h"

#include "../proto/status_code.pb.h"

DECLARE_bool(bfsWebKickEnable);
DECLARE_int32(nameserverStartRecoverTimeout);
DECLARE_int32(chunkserverMaxPendingBuffers);
DECLARE_int32(nameserverReportThreadNum);
DECLARE_int32(nameserverWorkThreadNum);
DECLARE_int32(nameserverReadThreadNum);
DECLARE_int32(nameserverHeartbeatThreadNum);
//DECLARE_int32(blockmappingBucketNum);
DECLARE_int32(hiRecoverTimeout);
DECLARE_int32(loRecoverTimeout);
DECLARE_int32(blockReportTimeout);
DECLARE_bool(cleanRedundancy);


namespace zfs
{

baidu::common::Counter gGetLocation;
baidu::common::Counter gAddBlock;
baidu::common::Counter gHeartBeat;
baidu::common::Counter gBlockReport;
baidu::common::Counter gUnlink;
baidu::common::Counter gCreateFile;
baidu::common::Counter gListDir;
baidu::common::Counter gReportBlocks;
extern baidu::common::Counter gBlocksNum;

NameServerImpl::NameServerImpl(): readonly(true), recoverTimeout(FLAGS_nameserverStartRecoverTimeout),
                recoverMode(kStopRecover)
{
    blockMappingManager = new BlockMappingManager();
    reportThreadPool = new baidu::common::ThreadPool(FLAGS_nameserverReportThreadNum);
    readThreadPool = new baidu::common::ThreadPool(FLAGS_nameserverReadThreadNum);
    workThreadPool = new baidu::common::ThreadPool(FLAGS_nameserverWorkThreadNum);
    heartbeatThreadPool = new baidu::common::ThreadPool(FLAGS_nameserverHeartbeatThreadNum);
    chunkserverManager = new ChunkServerManager(workThreadPool, blockMappingManager);
    _namespace = new NameSpace();
    checkLeader();
    startTime = baidu::common::timer::get_micros();
    readThreadPool->AddTask(std::bind(&NameServerImpl::logStatus, this));
}

void NameServerImpl::checkLeader()
{
    LOG(INFO, "Leader nameserver, rebuild block map.");
    NameServerLog log;
    std::function<void (const FileInfo&) > task = std::bind(&NameServerImpl::rebuildBlockMapCallback, this, std::placeholders::_1);
    _namespace->activateDb(task, &log);
    if(!logRemote(log, std::funciton<void (bool)>()))
    {
        LOG(FATAL, "logremote namespace update fail");
    }
    recoverTimeout = FLAGS_nameserverStartRecoverTimeout;
    startTIme = baidu::common::timer::get_micros();
    workThreadPool->DelayTask(1000, std::bind(&NameServerImpl::checkRecoverMode, this));
    isLeader = true;
}

void NameServerImpl::rebuildBlockMapCallback(const FileInfo &fileInfo)
{
    for(int i = 0; i < fileInfo.blockSize(); i++)
    {
        int64_t blockId = fileInfo.blocks(i);
        int64_t version = fileInfo.version();
        blockMappingManager->rebuildBlock(blockId, fileInfo.replicas(), version, fileInfo.size());
    }
}

void NameServerImpl::checkRecoverMode()
{
    int nowTime = (baidu::common::timer::get_micros() - startTime) / 1000000;
    int recoverTimeoutTemp = recoverTimeout;
    if(recoverTimeoutTemp == 0)
    {
        return;
    }
    int newRecoverTimeout = FLAGS_nameserverStartRecoverTimeout - nowTime;
    if(newRecoverTimeout <= 0)
    {
        LOG(INFO, "Now time %s", nowTime);
        recoverMode = kRecoverAll;
        return;
    }
    baidu::common::atomic_comp_swap(&recoverTimeout, newRecoverTimeout, recoverTimeout);
    workThreadPool->DelayTask(1000, std::bind(&NameServerImpl::checkRecoverMode, this));
}

void createFile(::google::protobuf::RpcController* controller,
                       const CreateFileRequest* request,
                       CreateFileResponse* response,
                       ::google::protobuf::Closure* done)
{
    gCreateFile.Inc();
    
}                       

}