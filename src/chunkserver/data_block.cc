//
// Created by zackliu on 4/4/17.
//

#include "data_block.h"

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <functional>

#include <gflags/gflags.h>
#include <common/counter.h>
#include <common/thread_pool.h>
#include <common/sliding_window.h>

#include <common/logging.h>

#include "file_cache.h"
#include "disk.h"

DECLARE_int32(write_buf_size);

namespace zfs
{
	extern baidu::common::Counter g_block_buffers;
	extern baidu::common::Counter g_buffers_new;
	extern baidu::common::Counter g_buffers_delete;

	Block::Block(const BlockMeta &meta, Disk *disk, FileCache *fileCache)
		:_disk(disk), _meta(meta),
	     _lastSeq(-1),
	     _sliceNum(-1),
	     _blockBuf(NULL),
	     _bufLen(0),
	     _bufDataLen(0),
	     _diskWriting(false),
	     _diskFileSize(meta.block_size()),
	     _fileDesc(kNotCreated),
	     _refs(0),
	     _closeCv(&_mu),
		 _isRocover(false),
	     _deleted(false),
	     _fileCache(fileCache)
	{
		_disk->_counters.data_size.Add(meta.block_size());
		_diskFile = meta.store_path() + buildFilePath(_meta.block_id());
		_disk->_counters.blocks.Inc();

		if(_meta.version() >= 0)
		{
			_finished = true;
			_recvWindow = NULL;
		}
		else
		{
			_finished = false;
			_recvWindow = new baidu::common::SlidingWindow<Buffer>(100, std::bind(&Block::writeCallback, this, std::placeholders::_1, std::placeholders::_2));
		}
	}

	Block::~Block()
	{
		//TODO
	}

	bool Block::cleanUp(int64_t namespaceVersion)
	{
		if(namespaceVersion != _disk->namespaceVersion())
		{
			setDeleted();
			return true;
		}
		return false;
	}

	StatusCode Block::setDeleted()
	{
		_disk->removeBlockMeta(_meta.block_id());
		int deleted = baidu::common::atomic_swap(&_deleted, 1);
		if(deleted)
		{
			return kCsNotFound;
		}
		return kOK;
	}
}