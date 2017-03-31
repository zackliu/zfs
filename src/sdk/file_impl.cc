//
// Created by zackliu on 3/30/17.
//

#include "file_impl.h"

#include <functional>
#include <gflags/gflags.h>
#include <common/sliding_window.h>
#include <common/logging.h>

#include "../proto/status_code.pb.h"
#include "fs_impl.h"


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


	FileImpl::FileImpl(FileSystemImpl *fs, RpcClient *rpcClient, const std::string name, int32_t flags,
	                   const WriteOptions &writeOptions)
		:_fs(fs), _rpcClient(rpcClient), _name(name), _openFlag(flags), _writeOffset(0),
	     _blockForWrite(NULL), _writeBuffer(NULL), _lastSeq(-1), _backWriting(0),
	     _writeOptions(writeOptions), _chunkServer(NULL), _lastChunkServerIndex(-1),
	     _readOffset(0), _readBuffer(NULL), _readBufferLen(0), _readBaseOffset(0),
	     _sequentialReadRatio(0), _lastReadOffset(-1), _readOptions(ReadOptions()),
	     _closed(false), _synced(false), _syncSignal(&_mu), _bgError(false)
	{
		_threadPool = fs->_threadPool;
	}

	FileImpl::FileImpl(FileSystemImpl *fs, RpcClient *rpcClient, const std::string name, int32_t flags,
	                   const ReadOptions &readOptions)
			:_fs(fs), _rpcClient(rpcClient), _name(name), _openFlag(flags), _writeOffset(0),
			 _blockForWrite(NULL), _writeBuffer(NULL), _lastSeq(-1), _backWriting(0),
			 _writeOptions(WriteOptions()), _chunkServer(NULL), _lastChunkServerIndex(-1),
			 _readOffset(0), _readBuffer(NULL), _readBufferLen(0), _readBaseOffset(0),
			 _sequentialReadRatio(0), _lastReadOffset(-1), _readOptions(readOptions),
			 _closed(false), _synced(false), _syncSignal(&_mu), _bgError(false)
	{
		_threadPool = fs->_threadPool;
	}

	FileImpl::~FileImpl() //TODO
	{

	}

	int32_t FileImpl::pread(char *buf, int32_t readSize, int64_t offset, bool readAhead)
	{

	}



	int32_t FileImpl::read(char *buf, int32_t readSize)
	{
		baidu::MutexLock lock(&_readMu);
		int32_t res = pread(buf, readSize, _readOffset, true);
		if(res >= 0)
		{
			_readOffset += res;
		}
		return res;
	}

}