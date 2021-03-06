//
// Created by zackliu on 4/4/17.
//

#include "block_manager.h"

#include <sys/stat.h>
#include <sys/vfs.h>
#include <climits>
#include <functional>
#include <algorithm>

#include <gflags/gflags.h>
#include <leveldb/db.h>
#include <leveldb/iterator.h>
#include <common/counter.h>
#include <common/logging.h>
#include <common/string_util.h>

#include "disk.h"
#include "data_block.h"
#include "file_cache.h"

DECLARE_int32(chunkserver_file_cache_size);
DECLARE_int32(chunkserver_use_root_partition);
DECLARE_bool(chunkserver_multi_path_on_one_disk);
DECLARE_int32(chunkserver_disk_buf_size);


using baidu::common::LogLevel::INFO;
using baidu::common::LogLevel::DEBUG;
using baidu::common::LogLevel::ERROR;
using baidu::common::LogLevel::FATAL;
using baidu::common::LogLevel::WARNING;

namespace zfs
{
	extern baidu::common::Counter g_find_ops;

	BlockManager::BlockManager(const std::string &storePath)
		:_threadPool(new baidu::ThreadPool(1)), _diskQuota(0), _counterManager(new DiskCounterManager)
	{
		checkStorePath(storePath);
		_fileCache = new FileCache(FLAGS_chunkserver_file_cache_size);
		logStatus();
	}

	BlockManager::~BlockManager()
	{
		_threadPool->Stop(true);
		delete _threadPool;
		delete _counterManager;
		baidu::MutexLock lock(&_mu);
		for(auto it = _blockMap.begin(); it != _blockMap.end(); it++)
		{
			Block *block = it->second;
			if(!block->isRecover())
			{
				closeBlock(block, true);
			}
			else
			{
				LOG(INFO, "[~BlockManager] Do not close recovering block #%ld", block->id());
			}
			block->decRef();
		}
		for(auto it = _disks.begin(); it != _disks.end(); it++)
		{
			delete it->second;
		}
		_blockMap.clear();
		delete _fileCache;
	}

	bool BlockManager::loadStorage()
	{
		bool result = true;
		for(auto it = _disks.begin(); it != _disks.end(); it++)
		{
			Disk *disk = it->second;
			result = result && disk->loadStorage(std::bind(&BlockManager::addBlock, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
			_diskQuota += disk->getQuota();
		}
		return result;
	}

	void BlockManager::checkStorePath(const std::string &storePath)
	{
		std::string fsIdStr, homeFs;
		struct statfs fsInfo;
		if(statfs("/home", &fsInfo) == 0) //对/home提取文件类型
		{
			homeFs.assign((const char*)&fsInfo.f_fsid, sizeof(fsInfo.f_fsid));
		}
		else if(statfs("/", &fsInfo) == 0)
		{
			LOG(WARNING, "statfs(\"/home\") fail: %s", strerror(errno));
			homeFs.assign((const char*)&fsInfo.f_fsid, sizeof(fsInfo.f_fsid));
		}
		else
		{
			LOG(FATAL, "statfs(\"/\") fail: %s", strerror(errno));
		}

		std::vector<std::string> storePathList;
		baidu::common::SplitString(storePath, ",", &storePathList);
		for(int i = 0; i < storePathList.size(); i++)
		{
			std::string &diskPath = storePathList[i];
			diskPath = baidu::common::TrimString(diskPath, " ");
			if(diskPath.empty() || diskPath[diskPath.size() - 1] != '/')
			{
				diskPath += "/";
			}
		}

		std::sort(storePathList.begin(), storePathList.end());
		auto it = std::unique(storePathList.begin(), storePathList.end());
		storePathList.resize(std::distance(storePathList.begin(), it));

		std::set<std::string> fsIds;
		int64_t  diskQuota = 0;
		for(int i = 0; i < storePathList.size(); i++)
		{
			std::string &diskPath = storePathList[i];
			int statResult = statfs(diskPath.c_str(), &fsInfo);
			std::string fsTmp((const char*)&fsInfo.f_fsid, sizeof(fsInfo.f_fsid));
			if(statResult != 0
					|| (!FLAGS_chunkserver_multi_path_on_one_disk && fsIds.find(fsTmp) != fsIds.end())
					|| (!FLAGS_chunkserver_use_root_partition && fsTmp == homeFs))
			{
				// statfs failed
				// do not allow multi data path on the same disk
				// do not allow using root as data path
				if(statResult != 0)
				{
					LOG(WARNING, "Stat store_path %s fail: %s, ignore it",
					    diskPath.c_str(), strerror(errno));
				}
				else
				{
					LOG(WARNING, "%s fsid has been used", diskPath.c_str());
				}
				storePathList[i] = storePathList[storePathList.size()-1];
				storePathList.resize(storePathList.size()-1);
				i--;
			}
			else
			{
				int64_t diskSize = fsInfo.f_blocks * fsInfo.f_bsize;
				int64_t  userQuota = fsInfo.f_bavail * fsInfo.f_bsize;
				int64_t superQuota = fsInfo.f_bfree * fsInfo.f_bsize;
				LOG(INFO, "Use store path: %s block: %ld disk: %s available %s quota: %s",
				    diskPath.c_str(), fsInfo.f_bsize,
				    baidu::common::HumanReadableString(diskSize).c_str(),
				    baidu::common::HumanReadableString(superQuota).c_str(),
				    baidu::common::HumanReadableString(userQuota).c_str());

				diskQuota += userQuota;
				Disk *disk = new Disk(storePathList[i], userQuota);
				_disks.push_back(std::make_pair(DiskStat(), disk));
				fsIds.insert(fsTmp);
			}
		}

		LOG(INFO, "%lu store path used.", storePathList.size());
		assert(storePathList.size() > 0);
		//checkChunkServerMeta(storePathList);
	}

	int64_t BlockManager::diskQuota()
	{
		return _diskQuota;
	}

	int64_t BlockManager::namespaceVersion() const
	{
		int64_t version = -1;
		for(auto it = _disks.begin(); it != _disks.end(); it++)
		{
			Disk *disk = it->second;
			if(version == -1) version = disk->namespaceVersion();
			if(version != disk->namespaceVersion()) return -1; //每个磁盘version必须相同
		}
		return version;
	}

	bool BlockManager::setNamespaceVersion(int64_t version)
	{
		for(auto it = _disks.begin(); it != _disks.end(); it++)
		{
			if(!it->second->setNamespaceVersion(version)) return false;
		}
		return true;
	}

	bool BlockManager::addBlock(int64_t blockId, Disk *disk, BlockMeta meta)
	{
		Block *block = new Block(meta, disk, _fileCache);
		block->addRef();
		baidu::MutexLock lock(&_mu);
		return _blockMap.insert(std::make_pair(blockId, block)).second;
	}

	//将blockid为offset的block取出
	int64_t BlockManager::listBlocks(std::vector<BlockMeta> *blocks, int64_t offset, int32_t num)
	{
		std::vector<leveldb::Iterator*> iters;
		for(auto it = _disks.begin(); it != _disks.end(); it++)
		{
			Disk *disk = it->second;
			disk->seek(offset, &iters);
		}

		if(iters.size() == 0) return 0;

		int32_t idx = -1;
		int64_t largestId = 0;
		while(blocks->size() < num && iters.size() != 0)
		{
			int64_t blockId = findSmallest(iters, &idx);
			if(blockId == -1) return largestId;
			auto it = iters[idx];
			BlockMeta meta;
			bool result = meta.ParseFromArray(it->value().data(), it->value().size());
			assert(result);
			assert(blockId == meta.block_id());
			blocks->push_back(meta);
			largestId = blockId;
			iters[idx]->Next();
		}
		for(int i = 0; i < iters.size(); i++) delete iters[i];

		return largestId;
	}

	int64_t BlockManager::findSmallest(std::vector<leveldb::Iterator *> &iters, int32_t *idx)
	{
		int64_t id = LLONG_MAX;
		for(int i = 0; i < iters.size(); i++)
		{
			auto it = iters[i];
			if(!it->Valid())
			{
				delete it;
				iters[i] = iters[iters.size()-1];
				iters.resize(iters.size()-1);
				i--;
				continue;
			}

			int64_t tmpId;
			if(1 != sscanf(it->key().data(), "%ld", &tmpId))
			{
				delete it;
				iters[i] = iters[iters.size()-1];
				iters.resize(iters.size()-1);
				i--;
				continue;
			}

			if(tmpId < id)
			{
				id = tmpId;
				*idx = i;
			}
		}

		if(id == LLONG_MAX)
		{
			return -1;
		}
		return id;
	}

	bool BlockManager::cleanUp(int64_t namespaceVersion)
	{
		for(auto it = _blockMap.begin(); it != _blockMap.end(); it++)
		{
			Block *block = it->second;
			if(block->cleanUp(namespaceVersion))
			{
				_fileCache->eraseFileCache(block->getFilePath());
				block->decRef();
				_blockMap.erase(it++);
			}
			else
			{
				it++;
			}
		}
		return true;
	}

}