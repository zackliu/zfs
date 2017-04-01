// Auther: zackliu1995@hotmail.com

#include "block_mapping.h"

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
DECLARE_int32(blockmappingThreadNum);

using baidu::common::LogLevel::INFO;
using baidu::common::LogLevel::DEBUG;
using baidu::common::LogLevel::ERROR;
using baidu::common::LogLevel::FATAL;
using baidu::common::LogLevel::WARNING;

namespace zfs
{

extern baidu::common::Counter gBlocksNum;

NSBlock::NSBlock() : id(-1), version(-1), blockSize(-1), expectReplicaNum(0), 
                     recoverStatus(kNotInRecover) {}

NSBlock::NSBlock(int64_t blockId, int32_t replica, int64_t blockVersion, int64_t blockSize)
         : id(blockId), version(blockVersion), blockSize(blockSize), expectReplicaNum(replica),
           recoverStatus(blockVersion < 0 ? kBlockWriting : kNotInRecover) {}

BlockMapping::BlockMapping()
{
    _threadPool = new baidu::common::ThreadPool(FLAGS_blockmappingThreadNum);
}

bool BlockMapping::getBlock(int64_t blockId, NSBlock *block)
{
    baidu::MutexLock lock(&_mu, "BlockMapping::getBlock", 1000);
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
    _mu.AssertHeld();
    NSBlockMap::iterator it = _blockMap.find(blockId);
    if(it == _blockMap.end())
    {
        return false;
    }
    if(block)
    {
        *block = it -> second;
    }
    return true;
}

//bool BlockMapping::getLocatedBlock(int64_t blockId, std::vector<int32_t> *replica,
//                                   int64_t *blockSize, RecoverStat *status)
//{
//    MutexLock lock(&_mu);
//    NSBlock *nsblock = NULL;
//    if(!getBlockPtr(blockId, &nsblock))
//    {
//        LOG(WARNING, "GetReplicaLocation can not find block: #%ld ", blockId);
//        return false;
//    }
//
//    replica->assign(nsblock->replica.begin(), nsblock->replica.end());
//    if(nsblock->recoverStatus == kBlockWriting || nsblock->recoverStatus == kIncomplete)
//    {
//        LOG(DEBUG, "getLocatedBlock return writing block #%ld", blockId);
//        replica->insert(replica->end(), nsblock->incompleteReplica.begin(), nsblock->incompleteReplica.end());
//    }
//
//    if(replica->empty())
//    {
//        LOG(DEBUG, "Block #%ld lost all the replicas", blockId);
//    }
//
//    *blockSize = nsblock->blockSize;
//    *status = nsblock->recoverStatus;
//    return true;
//}
//
//bool BlockMapping::changeReplicaNum(int64_t blockId, int32_t replicaNum)
//{
//    MutexLock lock(&_mu);
//    NSBlock *nsblock = NULL;
//    if(!getBlockPtr(blockId, &nsblock))
//    {
//        LOG(WARNING, "changeReplicaNum can not find block: #%ld ", blockId);
//        return false;
//    }
//    nsblock->expectReplicaNum = replicaNum;
//    return true;
//}
//
//void BlockMapping::addBlock(int64_t blockId, int32_t replica,
//                            const std::vector<int32_t> &initReplicas)
//{
//    NSBlock* nsblock = new NSBlock(blockId, replica, -1, 0);
//    if(nsblock -> recoverStatus == kNotInRecover)
//    {
//        nsblock->replica.insert(initReplicas.begin(), initReplicas.end());
//    }
//    else
//    {
//        nsblock->incompleteReplica.insert(initReplicas.begin(), initReplicas.end());
//    }
//    LOG(DEBUG, "init block info: #%ld", blockId);
//    gBlocksNum.Ink(); //原子自增1
//    MutexLock lock(&_mu);
//    common::timer::TimeChecker insertTime;
//    std::pair<NSBlockMap::iterator, bool> ret = _blockMap.insert(std::make_pair(blockId, nsblock));
//    assert(ret.second == true);
//    insertTime.Check(10*1000, "[AddNewBlock] InsertToBlockMapping");
//}
//
//void BlockMapping::rebuildBlock(int64_t blockId, int32_t replica,
//                                int64_t version, int64_t size)
//{
//    NSBlock *nsblock = new NSBlock(blockId, replica, version, size);
//    if(size)//??
//    {
//        nsblock->recoverStatus = kLost;
//        _lostBlocks.insert(blockId);
//    }
//    else
//    {
//        nsblock->recoverStatus = kBlockWriting;
//    }
//
//    if(version < 0)//??
//    {
//        LOG(INFO, "Rebuild writing block #%ld V%ld %ld", blockId, version, size);
//    }
//    else
//    {
//        LOG(DEBUG, "Rebuild block #%ld V%ld %ld", blockId, version, size);
//    }
//
//    gBlocksNum.Inc();
//    MutexLock lock(&_mu);
//    common::timer::TimeChecker insertTime;
//    std::pair<NSBlockMap::iterator, bool> ret = _blockMap.insert(std::make_pair(blockId, nsblock));
//    assert(ret.second == true);
//    insertTime.Check(10*1000, "[RebuildBlock] InsertToBlockMapping");
//}
//
//bool BlockMapping::updateBlockInfo(int64_t blockId, int32_t serverId, int64_t blockSize,
//                                   int64_t blockVersion)
//{
//    MutexLock lock(&_mu);
//    common::timer::TimeChecker updateBlockTimer;
//    NSBlock *nsblock = NULL;
//    if(!getBlockPtr(blockId, &nsblock))
//    {
//        LOG(DEBUG, "UpdateBlockInfo C%d #%ld has been removed", serverId, blockId);
//        return false;
//    }
//    updateBlockTimer.Check(10 * 1000, "[UpdateBlockInfo] GetBlockPtr");
//
//    bool ret = true;
//    switch (nsblock -> recoverStatus)
//    {
//        case kBlockWriting:
//            ret = updateWritingBlock(nsblock, serverId, blockSize, blockVersion);
//            updateBlockTimer.Check(10*1000, "[UpdateBlockInfo] UpdateWritingBlock");
//            return ret;
//        case kIncomplete:
//            ret = updateIncompleteBlock(nsblock, serverId, blockSize, blockVersion);
//            updateBlockTimer.Check(10*1000, "[UpdateBlockInfo] UpdateIncomleteBlock");
//            return ret;
//        case kLost:
//            if(nsblock->version < 0)
//            {
//                bool ret = updateWritingBlock(nsblock, serverId, blockSize, blockVersion);
//                if(nsblock->recoverStatus == kLost)
//                {
//                    _lostBlocks.erase(blockId);
//                    if(nsblock->version < 0)
//                    {
//                        nsblock->recoverStatus = kBlockWriting;
//                    }
//                    else
//                    {
//                        LOG(WARNING, "Update lost block #%ld V%ld", blockId, nsblock->version);
//                    }
//                }
//                return ret;
//            }
//            else
//            {
//                ret = updateNormalBlock(nsblock, serverId, blockSize, blockVersion);
//                updateBlockTimer.Check(10*1000, "[UpdateBlockInfo] UpdateNormalBlock");
//                return ret;
//            }
//        default:
//            ret = updateNormalBlock(nsblock, serverId, blockSize, blockVersion);
//                updateBlockTimer.Check(10*1000, "[UpdateBlockInfo] UpdateNormalBlock");
//                return ret;
//    }
//}

	void BlockMapping::dealWithDeadNode(int32_t csId, const std::set<int64_t> &blocks)
	{
		for(auto it = blocks.begin(); it != blocks.end(); ++it)
		{
			baidu::MutexLock lock(&_mu);
			dealWithDeadBlockInternal(csId, *it);
		}
		baidu::MutexLock lock(&_mu);
		NSBlock *block = NULL;
		for(auto it = _hiRecoverCheck[csId].begin(); it != _hiRecoverCheck[csId].end(); it++)
		{
			if(!getBlockPtr(*it, &block))
			{
				LOG(DEBUG, "deal with dead node for C%d cannot find block #%ld", csId, *it);
			}
			else
			{
				block->recoverStatus = kNotInRecover;
			}
		}
		_hiRecoverCheck.erase(csId);

		for(auto it = _loRecoverCheck[csId].begin(); it != _loRecoverCheck[csId].end(); it++)
		{
			if(!getBlockPtr(*it, &block))
			{
				LOG(DEBUG, "deal with dead node for C%d cannot find block #%ld", csId, *it);
			}
			else
			{
				block->recoverStatus = kNotInRecover;
			}
		}
		_loRecoverCheck.erase(csId);
	}

	void BlockMapping::dealWithDeadBlockInternal(int32_t csId, int64_t blockId)
	{
		_mu.AssertHeld();
		NSBlock *block = NULL;
		if(!getBlockPtr(blockId, &block))
		{
			LOG(DEBUG, "dealWithDeadBlockInternal for C%d cannot find block #%ld", csId, blockId);
			return;
		}

		std::set<int32_t> &incompleteReplica = block->incompleteReplica;
		std::set<int32_t> &replica = block->replica;

		if(incompleteReplica.erase(csId))
		{
			if(block->recoverStatus == kIncomplete)
			{
				removeFromIncomplete(blockId, csId);
			}
		}
		else
		{
			if(!replica.erase(csId))
			{
				LOG(INFO, "Dead replica C%d #%ld not in blockMapping, ignore it R%lu IR%lu",
					csId, blockId, replica.size(), incompleteReplica.size());
				return;
			}
		}

		if(block->recoverStatus == kIncomplete)
		{
			LOG(INFO, "Incomplete block C%d #%ld dead R%lu",
				csId, blockId, replica.size());
			if(incompleteReplica.empty())
			{
				setState(block, kNotInRecover);
			}
		}
		else if(block->recoverStatus == kBlockWriting)
		{
			LOG(INFO, "Writing block C%d #%ld dead R%lu IR%lu",
			    csId, blockId, replica.size(), incompleteReplica.size());
			if(incompleteReplica.size() > 0)
			{
				setState(block, kIncomplete);
				insertToIncomplete(blockId, incompleteReplica);
			}
			else
			{
				setState(block, kNotInRecover);
			}
		}

		LOG(DEBUG, "Dead replica at C%d add #%ld R%lu try recover", csId, blockId, replica.size());
		tryRecover(block);
	}

	void BlockMapping::removeFromIncomplete(int64_t blockId, int32_t csId)
	{
		_mu.AssertHeld();
		bool error = false;
		auto it = _incomplete.find(csId);
		if(it != _incomplete.end())
		{
			std::set<int64_t> &incompleteSet = it -> second;
			if(!incompleteSet.erase(blockId)) error = true;
			if(incompleteSet.empty()) _incomplete.erase(it);
		}
		else
		{
			error = true;
		}

		if(error)
		{
			LOG(WARNING, "RemoveFromIncomplete not find C%d #%ld", csId, blockId);
			abort();
		}
		else
		{
			LOG(INFO, "RemoveFromIncomplete C%d #%ld ", csId, blockId);
		}
	}

	void BlockMapping::setState(NSBlock *block, RecoverStat stat)
	{
		_mu.AssertHeld();
		LOG(INFO, "setState #ld %s -> %", block->id, RecoverStat_Name(block->recoverStatus).c_str(),
			RecoverStat_Name(stat).c_str());
		block->recoverStatus = stat;
	}

	void BlockMapping::insertToIncomplete(int64_t blockId, const std::set<int32_t> &incompleteReplica)
	{
		_mu.AssertHeld();
		auto it = incompleteReplica.begin();
		for(; it != incompleteReplica.end(); it++)
		{
			_incomplete[*it].insert(blockId);
			LOG(INFO, "Insert C%d #%ld to _incomplete", *it, blockId);
		}
	}

	void BlockMapping::tryRecover(NSBlock *block)
	{
		_mu.AssertHeld();
		if(block->recoverStatus == kCheck || block->recoverStatus == kIncomplete ||
			block->recoverStatus == kBlockWriting)
		{
			return;
		}

		int64_t blockId = block->id;
		if(block->replica.size() < block->expectReplicaNum)
		{
			if(block->replica.size() == 0)
			{
				if(block->blockSize && block->recoverStatus != kLost)
				{
					LOG(INFO, "[TryRecover] lost block #%ld ", blockId);
					_lostBlocks.insert(blockId);
					setState(block, kLost);

					_loPriRecover.erase(blockId);
					_hiPriRecover.erase(blockId);
				}
				else if(block->blockSize == 0 && block->recoverStatus == kLost)
				{
					_lostBlocks.erase(blockId);
					LOG(WARNING, "[TryRecover] empty block #%ld remove from lost", blockId);
				}
			}
			else if(block->replica.size() == 1 && block->recoverStatus != kHiRecover)
			{
				_hiPriRecover.insert(blockId);
				LOG(INFO, "[TryRecover] need more recover: #%ld %s->kHiRecover", blockId, RecoverStat_Name(block->recoverStatus).c_str());
				setState(block, kHiRecover);
				_lostBlocks.erase(blockId);
				_loPriRecover.erase(blockId);
			}
			else if(block->replica.size() > 1 && block->recoverStatus != kLoRecover)
			{
				_loPriRecover.insert(blockId);
				LOG(INFO, "[TryRecover] need more recover: #%ld %s->kLoRecover",
				    blockId, RecoverStat_Name(block->recoverStatus).c_str());
				setState(block, kLoRecover);
				_lostBlocks.erase(blockId);
				_hiPriRecover.erase(blockId);
			}
			return;
		}

		if(block->recoverStatus != kNotInRecover)
		{
			LOG(INFO, "Block #%ld V%ld %ld R%lu back to normal from %s",
			    blockId, block->version, block->blockSize, block->replica.size(), RecoverStat_Name(block->recoverStatus).c_str());
			setState(block, kNotInRecover);
			_lostBlocks.erase(blockId);
			_hiPriRecover.erase(blockId);
			_lostBlocks.erase(blockId);
		}
	}

	void BlockMapping::removeBlock(int64_t blockId, std::map<int64_t, std::set<int32_t> > *blocks)
	{
		baidu::MutexLock lock(&_mu);
		NSBlock *block = NULL;
		if(!getBlockPtr(blockId, &block))
		{
			LOG(WARNING, "Remove block #%ld is not found", blockId);
			return ;
		}

		if(blocks)
		{
			std::set<int32_t> &blockCs = (*blocks)[blockId];
			blockCs.insert(block->incompleteReplica.begin(), block->incompleteReplica.end());
			blockCs.insert(block->replica.begin(), block->replica.end());
		}

		if(block->recoverStatus == kIncomplete)
		{
			for(auto it = block->incompleteReplica.begin(); it != block->incompleteReplica.end(); it++)
			{
				removeFromIncomplete(blockId, *it);
			}
		}
		else if(block->recoverStatus == kLost)
		{
			_lostBlocks.erase(blockId);
		}
		else if(block->recoverStatus == kHiRecover)
		{
			_hiPriRecover.erase(blockId);
		}
		else if(block->recoverStatus == kLoRecover)
		{
			_loPriRecover.erase(blockId);
		}

		delete block;
		_blockMap.erase(blockId);
		gBlocksNum.Dec();
	}

	void BlockMapping::removeBlocksForFile(const FileInfo &fileInfo, std::map<int64_t, std::set<int32_t> > *blocks)
	{
		for(int i = 0; i < fileInfo.blocks_size(); i++)
		{
			removeBlock(fileInfo.blocks(i), blocks);
			LOG(INFO, "Remove block #%ld, for %s", fileInfo.blocks(i), fileInfo.name().c_str());
		}
	}

	void BlockMapping::addBlock(int64_t blockId, int32_t replicaNum, const std::vector<int32_t> &initReplica)
	{
		NSBlock *nsBlock = new NSBlock(blockId, replicaNum, -1, 0);
		if(nsBlock->recoverStatus == kNotInRecover)
		{
			nsBlock->replica.insert(initReplica.begin(), initReplica.end());
		}
		else
		{
			nsBlock->incompleteReplica.insert(initReplica.begin(), initReplica.end());
		}

		LOG(DEBUG, "Init block info: #%ld", blockId);

		gBlocksNum.Inc();
		baidu::MutexLock lock(&_mu);
		baidu::common::timer::TimeChecker insertTime;
		auto result = _blockMap.insert(std::make_pair(blockId, nsBlock));
		assert(result.second);
		insertTime.Check(10*1000, "[AddBlock] Insert into blockMapping");
	}
}