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
}
