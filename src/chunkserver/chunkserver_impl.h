//
// Created by zackliu on 4/4/17.
//

#ifndef CLOUDSTORAGE_CHUNKSERVER_IMPL_H
#define CLOUDSTORAGE_CHUNKSERVER_IMPL_H

#include "../proto/chunkserver.pb.h"
#include "../proto/nameserver.pb.h"
#include "../proto/status_code.pb.h"

#include <common/thread_pool.h>

#include "counter_manager.h"

namespace zfs
{
	class BlockManager;
	class Rpc;
	class NameServerClient;
	class ChunkServer_Stub;
	class Block;
	typedef ChunkserverCounterManager::ChunkserverStat chunkServerStat;

	class ChunkServerImpl : public ChunkServer
	{
	public:
		ChunkServerImpl();
		virtual ~ChunkServerImpl();

		virtual void writeBlock(::google::protobuf::RpcController* controller,
		                        const WriteBlockRequest* request,
		                        WriteBlockResponse* response,
		                        ::google::protobuf::Closure* done);


	private:
		void logStatus(bool routine);
		void doRegister();



	private:
		BlockManager *_blockManager;
		std::string _dataServerAddress;
		Rpc *_rpcClient;

		baidu::ThreadPool *_workThreadPool;
		baidu::ThreadPool *_readThreadPool;
		baidu::ThreadPool *_writeThreadPool;
		baidu::ThreadPool *_recoverThreadPool;
		baidu::ThreadPool *_heartbeatThreadPool;

		NameServerClient *_nameServerClient;
		int32_t _chunkServerId;
		ChunkserverCounterManager _counterManager;
		int64_t _heartbeatTaskId;
		volatile  int64_t  _blockreportTaskId;
		int64_t  _lastReportBlockId;
		int64_t  _reportId;
		bool _isFirstRound;
		int64_t  _firstRoundReportStart;
		volatile bool _serviceStop;

		Params _param;
	};
}


#endif //CLOUDSTORAGE_CHUNKSERVER_IMPL_H
