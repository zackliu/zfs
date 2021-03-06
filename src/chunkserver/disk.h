//
// Created by zackliu on 4/4/17.
//

#ifndef CLOUDSTORAGE_DISK_H
#define CLOUDSTORAGE_DISK_H

#include <stdint.h>
#include <map>
#include <string>
#include <vector>

#include <common/thread_pool.h>
#include <leveldb/db.h>

#include "../proto/status_code.pb.h"
#include "counter_manager.h"

namespace zfs
{
	class BlockMeta;
	class Block;
	class FileCache;
	typedef DiskCounterManager::DiskCounters DCounters;
	typedef DiskCounterManager::DiskStat DiskStat;

	class Disk
	{
	public:
		Disk(const std::string &path, int64_t quota);
		~Disk();
		void seek(int64_t blockId, std::vector<leveldb::Iterator*> *iters);

		bool loadStorage(std::function<void (int64_t, Disk*, BlockMeta)> callback);
		int64_t namespaceVersion() const;
		bool setNamespaceVersion(int64_t version);
		int64_t getQuota();
		bool removeBlockMeta(int64_t blockId);

	private:
		std::string blockId2Str(int64_t blockId);

	private:
		friend class Block;
		DCounters _counters;
		std::string _path;
		baidu::ThreadPool *_threadPool;
		int64_t _quota;
		leveldb::DB *_metadb;
		baidu::Mutex _mu;
		int64_t _namespaceVersion;
		DiskCounterManager _counterManager;
	};
}



#endif //CLOUDSTORAGE_DISK_H
