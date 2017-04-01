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
#include "../rpc/rpc.h"
#include "../rpc/nameserver_client.h"


DECLARE_int32(sdk_file_reada_len);
DECLARE_int32(sdk_createblock_retry);
DECLARE_int32(sdk_write_retry_times);

using baidu::common::LogLevel::INFO;
using baidu::common::LogLevel::DEBUG;
using baidu::common::LogLevel::ERROR;
using baidu::common::LogLevel::FATAL;
using baidu::common::LogLevel::WARNING;


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
		memcpy(_buf + _dataSize, buf, len);
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


	FileImpl::FileImpl(FileSystemImpl *fs, Rpc *rpcClient, const std::string name, int32_t flags,
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

	FileImpl::FileImpl(FileSystemImpl *fs, Rpc *rpcClient, const std::string name, int32_t flags,
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

	int32_t FileImpl::write(const char *buf, int32_t writeSize)
	{
		baidu::common::timer::AutoTimer autoTimer(100, "write", _name.c_str());

		{
			baidu::MutexLock lock(&_mu, "write", 1000);
			if(!(_openFlag & O_WRONLY))
			{
				return BAD_PARAMETER;
			}
			else if(_bgError)
			{
				return TIMEOUT;
			}
			else if(_closed)
			{
				return BAD_PARAMETER;
			}
			baidu::common::atomic_inc(&_backWriting);
		}

		if(_openFlag & O_WRONLY)
		{
			baidu::MutexLock lock(&_mu, "write addblock", 1000);
			if(_chunkServers.empty())
			{
				int res = 0;
				for(int i = 0; i < FLAGS_sdk_createblock_retry; i++)
				{
					res = addBlock();
					if(res == kOK) break;
					sleep(10);
				}
				if(res != kOK)
				{
					LOG(WARNING, "AddBlock for %s failed", _name.c_str());
					baidu::common::atomic_dec(&_backWriting);
					return res;
				}
			}
		}

		int32_t  w = 0;
		while(w < writeSize)
		{
			baidu::MutexLock lock(&_mu, "writeInternal", 1000);
			if(_writeBuffer == NULL)
			{
				_writeBuffer = new WriteBuffer(++_lastSeq, 256*1024, _blockForWrite->blockid(), _blockForWrite->blocksize());
			}

			if((writeSize - w) < _writeBuffer->available()) //可以一次装入buf
			{
				_writeBuffer->append(buf+w, writeSize-w);
				w = writeSize;
				break;
			}
			else
			{
				int n = _writeBuffer->available();
				_writeBuffer -> append(buf+w, n);
			}

			if(_writeBuffer -> available() == 0) //将已有的buffer内容写入
			{
				startWrite();
			}
		}

		baidu::common::atomic_add64(&_writeOffset, w);
		baidu::common::atomic_dec(&_backWriting);

		return w;
	}

	int32_t FileImpl::addBlock()
	{
		AddBlockRequest request;
		AddBlockResponse response;

		request.set_sequenceid(0);
		request.set_filename(_name);
		request.set_clientaddress(_fs->_localhostName);
		bool result = _fs->_nameServerClient->sendRequest(&NameServer_Stub::addBlock, &request, &response, 15, 1);

		if(!result || !response.has_block())
		{
			LOG(WARNING, "NameServer AddBlock failed: %s", _name.c_str());
			if(!result) return TIMEOUT;
			else return -1;
		}

		_blockForWrite = new LocatedBlock(response.block());
		bool chanWrite = isChainWrite();
		int chunkServerSize = chanWrite ? 1 : _blockForWrite->chains_size();
		for(int i = 0; i < chunkServerSize; i++)
		{
			const std::string &address = _blockForWrite->chains(i).address();
			_rpcClient->getStub(address, &_chunkServers[address]);
			_writeWindows[address] = new baidu::common::SlidingWindow<int>(100, std::bind(&FileImpl::onWriteCommit, std::placeholders::_1, std::placeholders::_2));
			_csError[address] = false;

			WriteBlockRequest writeBlockRequest;
			WriteBlockResponse writeBlockResponse;
			int64_t seq = baidu::common::timer::get_micros();
			writeBlockRequest.set_sequence_id(seq);
			writeBlockRequest.set_block_id(_blockForWrite->blockid());
			writeBlockRequest.set_databuf("", 0);
			writeBlockRequest.set_offset(0);
			writeBlockRequest.set_is_last(false);
			writeBlockRequest.set_packet_seq(0);

			if(chanWrite)
			{
				for(int i = 0; i < _blockForWrite->chains_size(); i++)
				{
					writeBlockRequest.add_chunkservers(_blockForWrite->chains(i).address());
				}
			}

			bool result = _rpcClient->sendRequest(_chunkServers[address], &ChunkServer_Stub::writeBlock, &writeBlockRequest, &writeBlockResponse, 25, 1);
			if(!result || writeBlockResponse.status() != 0)
			{
				LOG(WARNING, "Chunkserver AddBlock failed: %s", _name.c_str());
				for(int j = 0; j <= i; j ++)
				{
					delete _writeWindows[_blockForWrite->chains(j).address()];
					delete _chunkServers[_blockForWrite->chains(j).address()];
				}
				_writeWindows.clear();
				_chunkServers.clear();
				delete _blockForWrite;
				if(!result)
				{
					return TIMEOUT;
				}
			}
			_writeWindows[address]->Add(0, 0);
		}
		_lastSeq = 0;
		return OK;
	}

	void FileImpl::startWrite()
	{
		baidu::common::timer::AutoTimer at(5, "StartWrite", _name.c_str());

		_mu.AssertHeld();

		_writeQueue.push(_writeBuffer);
		_blockForWrite->set_blocksize(_blockForWrite->blocksize() + _writeBuffer->size());
		_writeBuffer = NULL;
		baidu::common::atomic_inc(&_backWriting);
		_mu.Unlock();
		_threadPool->AddTask(std::bind(&FileImpl::backgroundWrite, std::weak_ptr<FileImpl>(shared_from_this())));
		_mu.Lock("StartWrite relock", 1000);
	}

}