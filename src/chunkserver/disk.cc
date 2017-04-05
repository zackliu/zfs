//
// Created by zackliu on 4/4/17.
//

#include "disk.h"

#include <functional>
#include <sys/stat.h>

#include <gflags/gflags.h>
#include <leveldb/db.h>
#include <leveldb/cache.h>
#include <common/logging.h>
#include <common/string_util.h>

#include "data_block.h"

DECLARE_int32(disk_io_thread_num);
DECLARE_int32(chunkserver_disk_buf_size);

using baidu::common::LogLevel::INFO;
using baidu::common::LogLevel::DEBUG;
using baidu::common::LogLevel::ERROR;
using baidu::common::LogLevel::FATAL;
using baidu::common::LogLevel::WARNING;


namespace zfs
{
	Disk::Disk(const std::string &path, int64_t quota)
		:_path(path), _quota(quota), _namespaceVersion(0)
	{
		_threadPool = new baidu::ThreadPool(FLAGS_disk_io_thread_num);
	}

	Disk::~Disk()
	{
		_threadPool->Stop(true);
		delete _threadPool;
		delete _metadb;
	}

	int64_t  Disk::namespaceVersion() const
	{
		return _namespaceVersion;
	}

	bool Disk::setNamespaceVersion(int64_t version)
	{
		baidu::MutexLock lock(&_mu);

		if(_namespaceVersion == version) return true;

		//存入数据库
		std::string versionKey(8, '\0');
		versionKey.append("version");
		std::string versionStr(8, '\0');
		*(reinterpret_cast<int64_t*>(&versionStr[0])) = version;
		leveldb::Status s = _metadb->Put(leveldb::WriteOptions(), versionKey, versionStr);
		if(!s.ok())
		{
			LOG(WARNING, "SetNameSpaceVersion fail: %s", s.ToString().c_str());
			return false;
		}
		_namespaceVersion = version;
		LOG(INFO, "Disk %s Set namespace version: %ld", _path.c_str(), _namespaceVersion);
		return true;
	}

	bool Disk::loadStorage(std::function<void(int64_t, Disk *, BlockMeta)> callback)
	{
		baidu::MutexLock lock(&_mu);

		//TODO
	}

	int64_t Disk::getQuota()
	{
		return _quota;
	}
}
