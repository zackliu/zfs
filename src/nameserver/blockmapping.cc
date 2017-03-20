// Auther: zackliu1995@hotmail.com

#include "blockmapping.h"

#include <vector>
#include <gflags/gflags.h>

#include <common/counter.h>
#include <common/logging.h>
#include <common/string_util.h>

DECLARE_int32(recoverSpeed);
DECLARE_int32(hiRecoverTimeout);
DECLARE_int32(loRecoverTimeout);
DECLARE_bool(zfsBugTolerant);
DECLARE_bool(cleanRedundancy);
DECLARE_int32(webRecoverListSize);

namespace zfs
{

extern common::Counter gBlocksNum;

NSBlock::NSBlock() : id(-1), version(-1), blockSize(-1), expectReplicaNum(0), 
                     recoverStatus(kNotInRecover) {}

NSBlock::NSBlock(int64_t blockId, int32_t replica, int64_t blockVersion, int64_t blockSize)
         : id(blockId), version(blockVersion), blockSize(blockSize), expectReplicaNum(replica),
           recoverStatus(blockVersion < 0 ? kBlockWriting : kNotInRecover) {}

BlockMapping::BlockMapping(ThreadPool *threadPool) : threadPool(threadPool) {}

bool BlockMapping::getBlock(int64_t blockId, NSBlock *block)
{
    MutexLock lock(&mu, "BlockMapping::getBlock", 1000);
    NSBlock *nsblock = NULL;
    if(!getBlockPtr(blockId, &nsblock))
    {
        LOG(WARNING, "getBlockPtr can not find block #%ld", blockId);
        return false;
    }
    if(block)
    {
        *block = *nsblock;
    }
    return true;
}

bool BlockMapping::getBlockPtr(int64_t blockId, NSBlock **block)
{
    mu.AssertHeld();
    NSBlockMap::iterator it = blockMap.find(blockId);
    if(it == blockMap.end())
    {
        return false;
    }
    if(block)
    {
        *block = it -> second;
    }
    return true;
}

bool BlockMapping::getLocatedBlock(int64_t blockId, std::vector<int32_t> *replica, 
                                   int64_t *blockSize, RecoverStat *status)
{
    MutexLock lock(&mu);
    NSBlock *nsblock = NULL;
    if(!getBlockPtr(blockId, &nsblock))
    {
        LOG(WARNING, "GetReplicaLocation can not find block: #%ld ", blockId);
        return false;
    }

    replica->assign(nsblock->replica.begin(), nsblock->replica.end());
    if(nsblock->recoverStatus == kBlockWriting || nsblock->recoverStatus == kIncomplete)
    {
        LOG(DEBUG, "getLocatedBlock return writing block #%ld", blockId);
        replica->insert(replica->end(), nsblock->incompleteReplica.begin(), nsblock->incompleteReplica.end());
    }

    if(replica->empty())
    {
        LOG(DEBUG, "Block #%ld lost all the replicas", blockId);
    }

    *blockSize = nsblock->blockSize;
    *status = nsblock->recoverStatus;
    return true;
}                         

bool BlockMapping::changeReplicaNum(int64_t blockId, int32_t replicaNum)
{
    MutexLock lock(&mu);
    NSBlock *nsblock = NULL;
    if(!getBlockPtr(blockId, &nsblock))
    {
        LOG(WARNING, "changeReplicaNum can not find block: #%ld ", blockId);
        return false;
    }
    nsblock->expectReplicaNum = replicaNum;
    return true;
}

void BlockMapping::addBlock(int64_t blockId, int32_t replica, 
                            const std::vector<int32_t> &initReplicas)
{
    NSBlock* nsblock = new NSBlock(blockId, replica, -1, 0);
    if(nsblock -> recoverStatus == kNotInRecover)
    {
        nsblock->replica.insert(initReplicas.begin(), initReplicas.end());
    }
    else
    {
        nsblock->incompleteReplica.insert(initReplicas.begin(), initReplicas.end());
    }
    LOG(DEBUG, "init block info: #%ld", blockId);
    gBlocksNum.Ink(); //原子自增1
    MutexLock lock(&mu);
    common::timer::TimeChecker insertTime;
    std::pair<NSBlockMap::iterator, bool> ret = blockMap.insert(std::make_pair(blockId, nsblock));
    assert(ret.second == true);
    insertTime.Check(10*1000, "[AddNewBlock] InsertToBlockMapping");
}                          

void BlockMapping::rebuildBlock(int64_t blockId, int32_t replica, 
                                int64_t version, int64_t size)
{
    NSBlock *nsblock = new NSBlock(blockId, replica, version, size);
    if(size)//??
    {
        nsblock->recoverStatus = kLost;
        lostBlocks.insert(blockId);
    }
    else
    {
        nsblock->recoverStatus = kBlockWriting;
    }

    if(version < 0)//??
    {
        LOG(INFO, "Rebuild writing block #%ld V%ld %ld", blockId, version, size);
    }
    else
    {
        LOG(DEBUG, "Rebuild block #%ld V%ld %ld", blockId, version, size);
    }

    gBlocksNum.Inc();
    MutexLock lock(&mu);
    common::timer::TimeChecker insertTime;
    std::pair<NSBlockMap::iterator, bool> ret = blockMap.insert(std::make_pair(blockId, nsblock));
    assert(ret.second == true);
    insertTime.Check(10*1000, "[RebuildBlock] InsertToBlockMapping");
}

bool BlockMapping::updateBlockInfo(int64_t blockId, int32_t serverId, int64_t blockSize,
                                   int64_t blockVersion);
{
    MutexLock lock(mu);
    common::timer::TimeChecker updateBlockTimer;
    NSBlock *nsblock = NULL;
    if(!getBlockPtr(blockId, &nsblock))
    {
        LOG(DEBUG, "UpdateBlockInfo C%d #%ld has been removed", serverId, blockId);
        return false;
    }
    updateBlockTimer.Check(10 * 1000, "[UpdateBlockInfo] GetBlockPtr");
    
    bool ret = true;
    switch (nsblock -> recoverStatus)
    {
        case kBlockWriting:
            ret = updateWritingBlock(nsblock, serverId, blockSize, blockVersion);
            updateBlockTimer.Check(10*1000, "[UpdateBlockInfo] UpdateWritingBlock");
            return ret;
        case kIncomplete:
            ret = updateIncompleteBlock(nsblock, serverId, blockSize, blockVersion);
            updateBlockTimer.Check(10*1000, "[UpdateBlockInfo] UpdateIncomleteBlock");
            return ret;
        case kLost:
            if(nsblock->version < 0)
            {
                bool ret = updateWritingBlock(nsblock, serverId, blockSize, blockVersion);
                if(nsblock->recoverStatus == kLost)
                {
                    lostBlocks.erase(blockId);
                    if(nsblock->version < 0)
                    {
                        nsblock->recoverStatus = kBlockWriting;
                    }
                    else
                    {
                        LOG(WARNING, "Update lost block #%ld V%ld", blockId, nsblock->version);
                    }
                }
                return ret;
            }
            else
            {
                ret = updateNormalBlock(nsblock, serverId, blockSize, blockVersion);
                updateBlockTimer.Check(10*1000, "[UpdateBlockInfo] UpdateNormalBlock");
                return ret;
            }
        default:
            ret = updateNormalBlock(nsblock, serverId, blockSize, blockVersion);
                updateBlockTimer.Check(10*1000, "[UpdateBlockInfo] UpdateNormalBlock");
                return ret;
    }
}                                   

}