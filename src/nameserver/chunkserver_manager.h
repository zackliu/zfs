//
// Created by zackliu on 3/23/17.
//

#ifndef CLOUDSTORAGE_CHUNKSERVER_MANAGER_H
#define CLOUDSTORAGE_CHUNKSERVER_MANAGER_H

#include <set>
#include <map>
#include <functional>

#include <common/thread_pool.h>
#include "../proto/nameserver.pb.h"
#include "../proto/status_code.pb.h"

namespace zfs
{
	const double kChunkServerLoadMax = 0.999999;

	class BlockMapping;
	typedef std::vector<std::pair<int64_t, std::vector<std::string> > > RecoverVec;

	class Blocks
	{
	public:
		Blocks(int32_t csId) : _reportId(-1), _csId(csId) {}
		int64_t getReportId();
		void insert(int64_t blockId);
		void remove(int64_t blockId);
		void cleanUp(std::set<int64_t > *blocks);
		void moveNew();
		int64_t checkLost(int64_t reportId, const std::set<int64_t> &blocks,
						  int64_t start, int64_t end, std::vector<int64_t> *lost);

	private:
		baidu::common::Mutex _mu, _newMu;
		std::set<int64_t> _blocks, _newBlocks;
		int64_t _reportId;
		int32_t _csId;
	};


	class ChunkServerManager
	{
	public:
		struct Status
		{
			int32_t  writeQps, readQps;
			int64_t  writeSpeed, readSpeed, recoverSpeed;
		};

		ChunkServerManager(baidu::common::ThreadPool *threadPool, BlockMapping *blockMapping);

	private:
		void deadCheck();
		void logStatus();

	private:
		Status _status;
		baidu::common::ThreadPool *_threadPool;
		BlockMapping *_blockMapping;
		baidu::common::Mutex _mu;
		int32_t _chunkServerNum;
		int32_t _nextChunkServerId;
		typedef std::map<int32_t , ChunkServerInfo*> ServerMap;
		ServerMap _chunkServics;
		std::map<std::string, int32_t> _addressMap;
		std::map<int32_t, Blocks*> _blockMap;
		std::map<int32_t, std::set<ChunkServerInfo*> > _heartbeatList;

		std::string _localhostName, _localZone;
	};
}


#endif //CLOUDSTORAGE_CHUNKSERVER_MANAGER_H
