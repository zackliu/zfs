//
// Created by zackliu on 3/23/17.
//

#include "chunkserver_manager.h"

#include <algorithm>

#include <gflags/gflags.h>
#include <common/logging.h>;
#include <common/string_util.h>
#include <common/util.h>
#include "block_mapping.h"

using baidu::common::LogLevel::INFO;
using baidu::common::LogLevel::DEBUG;
using baidu::common::LogLevel::ERROR;
using baidu::common::LogLevel::FATAL;
using baidu::common::LogLevel::WARNING;

DECLARE_int32(keepalive_timeout);
DECLARE_int32(chunkserver_max_pending_buffers);
DECLARE_int32(recover_speed);
DECLARE_int32(recover_dest_limit);
DECLARE_int32(heartbeat_interval);
DECLARE_bool(select_chunkserver_by_zone);
DECLARE_bool(select_chunkserver_by_tag);
DECLARE_double(select_chunkserver_local_factor);
DECLARE_int32(blockreport_interval);
DECLARE_int32(blockreport_size);
DECLARE_int32(expect_chunkserver_num);


namespace zfs
{
	void add()
	{}

	int64_t Blocks::getReportId()
	{
		return _reportId;
	}

	void Blocks::insert(int64_t blockId)
	{
		baidu::common::MutexLock lock(&_newMu);
		_newBlocks.insert(blockId);
	}

	void Blocks::remove(int64_t blockId)
	{
		baidu::common::MutexLock lock(&_mu);
		_blocks.erase(blockId);

		baidu::common::MutexLock newLock(&_newMu);
		_newBlocks.erase(blockId);
	}

	void Blocks::cleanUp(std::set<int64_t> *blocks)
	{
		assert(blocks->empty());
		std::set<int64_t > temp;

		baidu::common::MutexLock lock(&_mu);
		std::swap(*blocks, _blocks);

		baidu::common::MutexLock newLock(&_newMu);
		std::swap(temp, _newBlocks);

		blocks->insert(temp.begin(), temp.end());
	}

	void Blocks::moveNew()
	{
		baidu::common::MutexLock lock(&_mu), newLock(&_newMu);
		std::set<int64_t> temp;
		_blocks.insert(_newBlocks.begin(), _newBlocks.end());
		std::swap(temp, _newBlocks);
	}

	//TODO: checkLost

	ChunkServerManager::ChunkServerManager(baidu::common::ThreadPool *threadPool, BlockMapping *blockMapping)
		: _threadPool(threadPool), _blockMapping(blockMapping), _chunkServerNum(0), _nextChunkServerId(1)
	{
		memset(&_status, 0, sizeof(_status));
		_threadPool->AddTask(std::bind(&ChunkServerManager::deadCheck, this));
		_threadPool->AddTask(std::bind(&ChunkServerManager::logStatus, this));
		_localhostName = baidu::common::util::GetLocalHostName();
		_params.set_reportinterval(FLAGS_blockreport_interval);
		_params.set_reportsize(FLAGS_blockreport_size);
		_params.set_recoversize(FLAGS_recover_speed);
		_params.set_keepalivetimeout(FLAGS_keepalive_timeout);
		LOG(INFO, "Localhost: %s", _localhostName.c_str());
	}

	void ChunkServerManager::deadCheck()
	{
		int32_t nowTime = baidu::common::timer::now_time();

		baidu::common::MutexLock lock(&_mu, "DeadCheck", 10);
		auto it = _heartbeatList.begin();
		//对于当时check时间+timeout限制已经小于现在时间的 即为已经超时丢失认为dead
		while (it != _heartbeatList.end() && it -> first + _params.keepalivetimeout() <= nowTime)
		{
			auto node = it->second.begin();
			while(node != it->second.end())
			{
				ChunkServerInfo *cs = *node;
				it -> second.erase(node++);
				LOG(INFO, "[DeadCheck] ChunkServer dead C%d %s, csNum = %d", cs->id(), cs->address().c_str(), _chunkServerNum);
				cs->set_isdead(true);
				if(cs->status() == kCsActive || cs->status() == kCsReadonly)
				{
					cs->set_status(kCsWaitClean);
					std::function<void ()> task = std::bind(&ChunkServerManager::cleanChunkServer, this, cs, std::string("Dead"));
					_threadPool->AddTask(task);
				}
				else
				{
					LOG(INFO, "[DeadCheck] ChunkServer C%d, %s is being clean", cs->id(), cs->address().c_str());
				}
			}
			assert(it->second.empty());
			_heartbeatList.erase(it);
			it = _heartbeatList.begin();
		}

		int idleTime = 5;
		if(it != _heartbeatList.end())
		{
			idleTime = it->first + _params.keepalivetimeout() - nowTime;
			if(idleTime > 5)
			{
				idleTime = 5;
			}
		}
		_threadPool->DelayTask(idleTime*1000, std::bind(&ChunkServerManager::deadCheck, this));
	}

	void ChunkServerManager::cleanChunkServer(ChunkServerInfo *cs, const std::string reason)
	{
		int32_t id = cs->id();
		baidu::MutexLock lock(&_mu, "CleanChunkServer", 10);
		_chunkServerNum--;
		auto it = _blockMap.find(id);
		assert(it != _blockMap.end());
		std::set<int64_t> blocks;
		it->second->cleanUp(&blocks); //原有block清空并且换出到blocks中
		LOG(INFO, "Remove CHunkServer C%d %s %s, csNum = %d", id, cs->address().c_str(), reason.c_str(), _chunkServerNum);
		cs->set_status(kCsCleaning);
		_mu.Unlock();

		_blockMapping->dealWithDeadNode(id, blocks);
		_mu.Lock("CleanChunkserver Relock", 10);
		cs->set_wqps(0);
		cs->set_wspeed(0);
		cs->set_rqps(0);
		cs->set_rspeed(0);
		cs->set_recoverspeed(0);

		if(std::find(_chunkServersToOffline.begin(), _chunkServersToOffline.end(), cs->address())
		   == _chunkServersToOffline.end())
		{
			if(cs->isdead())
			{
				cs->set_status(kCsOffLine);
			}
			else
			{
				cs->set_status(kCsStandby);
			}
		}
		else
		{
			cs->set_status(kCsReadonly);
		}
	}

	void ChunkServerManager::addBlock(int32_t id, int64_t blockId)
	{
		Blocks *blocks = getBlockMap(id);
		if(!blocks)
		{
			LOG(WARNING, "Cannot get blocks in C%d", id);
			return;
		}
		blocks->insert(blockId);
	}

	Blocks* ChunkServerManager::getBlockMap(int32_t csId)
	{
		baidu::MutexLock lock(&_mu);
		auto it = _blockMap.find(csId);
		if(it == _blockMap.end()) return NULL;
		return it->second;
	}

	void ChunkServerManager::removeBlock(int32_t id, int64_t blockId)
	{
		Blocks* blocks = getBlockMap(id);
		if(!blocks)
		{
			LOG(WARNING, "Cannot get blocks in C%d", id);
			return;
		}
		blocks->remove(blockId);
	}

	bool ChunkServerManager::getChunkServerChains(int num, std::vector<std::pair<int32_t, std::string> > *chains,
	                                              const std::string &clientAddress)
	{
		baidu::MutexLock lock(&_mu, "getChunkServerChains", 10);
		if(num > _chunkServerNum)
		{
			LOG(INFO, "not enough alive chunkservers [%ld] for GetChunkServerChains [%d]\n",
			    _chunkServerNum, num);
			return false;
		}

		ChunkServerInfo *localCs = NULL;
		auto clientIt = _addressMap.lower_bound(clientAddress); //查找是否有chunkserver恰好是client
		if(clientIt != _addressMap.end())
		{
			std::string tempAddress(clientIt->first, 0, clientIt->first.find_last_of(':'));
			if(tempAddress == clientAddress)
			{
				ChunkServerInfo *cs = NULL;
				if(getChunkServerPtr(clientIt->second, &cs) && !cs->isdead() && !(cs->status() == kCsReadonly))
				{
					localCs = cs;
				}
			}
		}

		auto it = _heartbeatList.begin();
		std::vector<std::pair<double, ChunkServerInfo*> > loads;

		for(; it != _heartbeatList.end(); it++)
		{
			std::set<ChunkServerInfo*> &csSet = it->second;
			for(auto setIt = csSet.begin(); it != csSet.end(); setIt++)
			{
				ChunkServerInfo *cs = *setIt;
				if(cs->status() == kCsReadonly)
				{
					LOG(INFO, "Alloc ignore Chunkserver %s: is in offline progress", cs->address().c_str());
					continue;
				}
				double load = cs->load();
				if(load <= kChunkServerLoadMax)
				{
					double localFactor = (cs == localCs ? FLAGS_select_chunkserver_local_factor : 0);
					loads.push_back(std::make_pair(load-localFactor, cs));//对于本地情况，可以看作负载更低
				}
				else
				{
					LOG(DEBUG, "Alloc ignore: ChunkServer %s data %ld/%ld buffer %d",
					    cs->address().c_str(), cs->datasize(),
					    cs->diskquota(), cs->buffers());
				}
			}
		}

		if((int)loads.size() < num)
		{
			LOG(DEBUG, "Only %ld chunkserver of %d is not over overladen, GetChunkServerChains(%d) return false",
			    loads.size(), _chunkServerNum, num);
			return false;
		}

		randomSelect(&loads, num);

		for(int i = 0; i < num; i++)
		{
			chains->push_back(std::make_pair(loads[i].second->id(), loads[i].second->address()));
		}

		return true;
	}

	bool ChunkServerManager::getChunkServerPtr(int32_t csId, ChunkServerInfo **info)
	{
		_mu.AssertHeld();
		auto it = _chunkServers.find(csId);
		if(it == _chunkServers.end()) return false;
		if(info) *info = it->second;
		return true;
	}

	void ChunkServerManager::randomSelect(std::vector<std::pair<double, ChunkServerInfo *> > *loads, int num)
	{
		std::sort(loads->begin(), loads->end());

		int scope = loads->size() - (loads->size() % num);
		for (int32_t i = num; i < scope; i++) {
			int round =  i / num + 1;
			double base_load = (*loads)[i % num].first;
			int ratio = static_cast<int>((base_load + 0.0001) * 100.0 / ((*loads)[i].first + 0.0001));
			if (rand() % 100 < (ratio / round)) {
				std::swap((*loads)[i % num], (*loads)[i]);
			}
		}
	}
}
