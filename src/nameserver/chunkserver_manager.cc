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

	}

	void ChunkServerManager::deadCheck()
	{
		int32_t newTime = baidu::common::timer::now_time();

		baidu::common::MutexLock lock(&_mu, "DeadCheck", 10);
		auto it = _heartbeatList.begin();

		while (it != _heartbeatList.end())
		{

		}


	}


}
