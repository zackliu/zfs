//
// Created by zackliu on 3/30/17.
//

#ifndef CLOUDSTORAGE_FILE_IMPL_H
#define CLOUDSTORAGE_FILE_IMPL_H

#include <map>
#include <set>
#include <string>
#include <memory>

#include <common/atomic.h>
#include <common/mutex.h>
#include <common/timer.h>
#include <common/sliding_window.h>
#include <common/thread_pool.h>

#include "../proto/nameserver.pb.h"
#include "../proto/chunkserver.pb.h"

#include "zfs.h"

namespace zfs
{
	class WriteBuffer
	{
	public:
		WriteBuffer(int32_t seq, int32_t bufSize, int64_t blockId, int64_t offset);
		~WriteBuffer();
		int available();
		int append(const char *buf, int len);
		const char* data();
		int size() const;
		int sequence() const;
		void clear();
		void setLast();
		bool isLast() const;
		int64_t	offset() const;
		int64_t blockId() const;
		void addRefBy(int counter);
		void addRef();
		void decRef();

	private:
		int32_t _bufSize;
		int32_t _dataSize;
		char *_buf;
		int64_t _blockId;
		int64_t _offset;
		int32_t _seqId;
		bool _isLast;
		volatile int _refs;
	};


	struct LocatedBlocks
	{
		int64_t _filelength;
		std::vector<LocatedBlock> _blocks;
		void copyFrom(const ::google::protobuf::RepeatedPtrField<LocatedBlock> &blocks)
		{
			for(int i =0; i < blocks.size(); i++)
			{
				_blocks.push_back(blocks.Get(i));
			}
		}

	};


	class FileSystemImpl;
	class RpcClient;


	class FileImpl : public File, public std::enable_shared_from_this<FileImpl>
	{
	public:
		friend  class FileSystemImpl;
		FileImpl(FileSystemImpl *fs, RpcClient *rpcClient, const std::string name,
				 int32_t flags, const WriteOptions &writeOptions);
		FileImpl(FileSystemImpl *fs, RpcClient *rpcClient, const std::string name,
		         int32_t flags, const ReadOptions &readOptions);
		~FileImpl();

		int32_t pread(char *buf, int32_t readSize, int64_t offset, bool readAhead = false);
		int32_t read(char* buf, int32_t readSize);
		int32_t write(const char* buf, int32_t writeSize);
		int32_t close();


		struct WriteBufferCmp
		{
			bool operator()(const WriteBuffer *wba, const WriteBuffer *wbb)
			{
				return wba->sequence() > wbb->sequence();
			}
		};

	private:
		FileSystemImpl *_fs;
		RpcClient *_rpcClient;
		baidu::ThreadPool *_threadPool;
		std::string _name;
		int32_t _openFlag;

		//for write
		volatile int64_t _writeOffset;
		LocatedBlock *_blockForWrite;
		WriteBuffer *_writeBuffer;
		int32_t _lastSeq;
		std::map<std::string, baidu::common::SlidingWindow<int>*> _writeWindows;
		std::priority_queue<WriteBuffer*, std::vector<WriteBuffer*>, WriteBufferCmp> _writeQueue;
		volatile int _backWriting;
		const WriteOptions _writeOptions;

		//for read
		LocatedBlocks _locatedBlocks;
		ChunkServer_Stub *_chunkServer;
		std::map<std::string, ChunkServer_Stub*> _chunkServers;
		std::set<ChunkServer_Stub> _badChunkServers;
		int32_t _lastChunkServerIndex;
		int64_t  _readOffset;
		baidu::Mutex _readMu;
		char *_readBuffer;
		int32_t _readBufferLen;
		int64_t _readBaseOffset;
		int32_t _sequentialReadRatio;
		int64_t _lastReadOffset;
		const ReadOptions _readOptions;

		bool _closed;
		bool _synced;
		baidu::Mutex _mu;
		baidu::CondVar _syncSignal;
		bool _bgError;
		std::map<std::string, bool> _csError;


	};
}

#endif //CLOUDSTORAGE_FILE_IMPL_H
