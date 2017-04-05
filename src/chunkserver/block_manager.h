//
// Created by zackliu on 4/4/17.
//

#ifndef CLOUDSTORAGE_BLOCK_MANAGER_H
#define CLOUDSTORAGE_BLOCK_MANAGER_H

#include "../proto/chunkserver.pb.h"
#include "../proto/nameserver.pb.h"
#include "../proto/status_code.pb.h"

#include <common/thread_pool.h>

#include "counter_manager.h"
#include "../proto/block.pb.h"

namespace zfs
{
	class BlockMeta;
	class Block;
	class FileCache;
	class Disk;
	typedef  DiskCounterManager::DiskStat DiskStat;

	class BlockManager
	{
	public:
		BlockManager(const std::string &storePath);
		~BlockManager();
		bool loadStorage();
		bool closeBlock(Block *block, bool sync);
		int64_t diskQuota();
		int64_t namespaceVersion() const;
		bool setNamespaceVersion(int64_t version);
		bool addBlock(int64_t blockId, Disk *disk, BlockMeta meta);
		int64_t listBlock(std::vector<BlockMeta> *blocks, int64_t offset, int32_t num);

	private:
		void checkStorePath(const std::string &storePath);
		void logStatus();


	private:
		baidu::ThreadPool *_threadPool;
		std::vector<std::pair<DiskStat, Disk*> > _disks;
		FileCache *_fileCache;
		baidu::Mutex _mu;
		std::map<int64_t , Block*> _blockMap;
		int64_t _diskQuota;
		DiskStat _stat;
		DiskCounterManager *_counterManager;
	};
}

#endif //CLOUDSTORAGE_BLOCK_MANAGER_H
