//
// Created by zackliu on 3/30/17.
//

#include "file_impl.h"

#include <functional>
#include <gflags/gflags.h>
#include <common/sliding_window.h>
#include <common/logging.h>

#include "../proto/status_code.pb.h"


DECLARE_int32(sdk_file_reada_len);
DECLARE_int32(sdk_createblock_retry);
DECLARE_int32(sdk_write_retry_times);

namespace zfs
{
	WriteBuffer::WriteBuffer(int32_t seq, int32_t bufSize, int64_t blockId, int64_t offset)
		: _bufSize(bufSize), _dataSize(0), _blockId(blockId), _offset(offset), _seqId(seq),
	      _isLast(false), _refs(0)
	{
		_buf = new char[_bufSize];
	}

	WriteBuffer::~WriteBuffer()
	{
		delete[] _buf;
		_buf = NULL;
	}

	int WriteBuffer::available()
	{
		return _bufSize - _dataSize;
	}

	int WriteBuffer::append(const char *buf, int len)
	{
		
	}
}