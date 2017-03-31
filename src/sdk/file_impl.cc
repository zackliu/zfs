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
		assert(_dataSize + len <= _bufSize);
		memcpy(_buf, buf, len);
		_dataSize+=len;
		return _dataSize;
	}

	const char* WriteBuffer::data()
	{
		return _buf;
	}

	int WriteBuffer::size() const
	{
		return _dataSize;
	}

	int WriteBuffer::sequence() const
	{
		return _seqId;
	}

	void WriteBuffer::clear()
	{
		_dataSize = 0;
	}

	void WriteBuffer::setLast()
	{
		_isLast = true;
	}

	bool WriteBuffer::isLast() const
	{
		return _isLast;
	}

	int64_t WriteBuffer::offset() const
	{
		return _offset;
	}

	int64_t WriteBuffer::blockId() const
	{
		return _blockId;
	}

	void WriteBuffer::addRefBy(int counter)
	{
		baidu::common::atomic_add(&_refs, counter);
	}

	void WriteBuffer::addRef()
	{
		baidu::common::atomic_inc(&_refs);
		assert(_refs > 0);
	}

	void WriteBuffer::decRef()
	{
		if(baidu::common::atomic_add(&_refs, -1) == 1)
		{
			assert(_refs == 0);
			delete this;
		}
	}



}