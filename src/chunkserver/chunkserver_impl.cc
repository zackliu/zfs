//
// Created by zackliu on 4/4/17.
//

#include "chunkserver_impl.h"

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <climits>
#include <functional>

#include <gflags/gflags.h>

#include <common/mutex.h>
#include <common/atomic.h>
#include <common/counter.h>
#include <common/mutex.h>
#include <common/thread_pool.h>
#include <common/util.h>
#include <common/timer.h>
#include <common/sliding_window.h>
#include <common/string_util.h>

#include "../proto/nameserver.pb.h"
#include "../rpc/nameserver_client.h"

#include "data_block.h"
#include "block_manager.h"

DECLARE_string(block_store_path);
DECLARE_string(nameserver_nodes);
DECLARE_string(chunkserver_port);
DECLARE_string(chunkserver_tag);
DECLARE_int32(heartbeat_interval);
DECLARE_int32(blockreport_interval);
DECLARE_int32(blockreport_size);
DECLARE_int32(write_buf_size);
DECLARE_int32(chunkserver_work_thread_num);
DECLARE_int32(chunkserver_read_thread_num);
DECLARE_int32(chunkserver_write_thread_num);
DECLARE_int32(chunkserver_recover_thread_num);
DECLARE_int32(chunkserver_max_pending_buffers);
DECLARE_int64(chunkserver_max_unfinished_bytes);
DECLARE_bool(chunkserver_auto_clean);
DECLARE_int32(block_report_timeout);

namespace zfs
{
	extern baidu::common::Counter g_block_buffers;
// number of buffers are created in a period of time (stat)
	extern baidu::common::Counter g_buffers_new;
// number of buffers are deleted in a period of time (stat)
	extern baidu::common::Counter g_buffers_delete;
	extern baidu::common::Counter g_unfinished_bytes;
	extern baidu::common::Counter g_find_ops;
	extern baidu::common::Counter g_read_ops;
	extern baidu::common::Counter g_read_bytes;
	extern baidu::common::Counter g_write_ops;
	extern baidu::common::Counter g_recover_count;
	extern baidu::common::Counter g_recover_bytes;
	extern baidu::common::Counter g_refuse_ops;
	extern baidu::common::Counter g_rpc_delay;
	extern baidu::common::Counter g_rpc_delay_all;
	extern baidu::common::Counter g_rpc_count;


	ChunkServerImpl::ChunkServerImpl()
		:_chunkServerId(-1),
	     _heartbeatTaskId(-1),
	     _blockreportTaskId(-1),
	     _lastReportBlockId(-1),
	     _reportId(0),
	     _isFirstRound(true),
	     _firstRoundReportStart(-1),
	     _serviceStop(false)
	{
		_dataServerAddress = baidu::common::util::GetLocalHostName() + ":" + FLAGS_chunkserver_port;
		_param.set_reportinterval(FLAGS_blockreport_interval);
		_param.set_reportsize(FLAGS_blockreport_size);
		_workThreadPool = new baidu::ThreadPool(FLAGS_chunkserver_work_thread_num);
		_readThreadPool = new baidu::ThreadPool(FLAGS_chunkserver_read_thread_num);
		_writeThreadPool = new baidu::ThreadPool(FLAGS_chunkserver_write_thread_num);
		_recoverThreadPool = new baidu::ThreadPool(FLAGS_chunkserver_recover_thread_num);
		_heartbeatThreadPool = new baidu::ThreadPool(1);
		_blockManager = new BlockManager(FLAGS_block_store_path);
		bool result = _blockManager->loadStorage();
		assert(result == true);
		_rpcClient = new Rpc();
		_nameServerClient = new NameServerClient(_rpcClient, FLAGS_nameserver_nodes);
		//_heartbeatThreadPool->AddTask(std::bind(&ChunkServerImpl::logStatus, this, true));
		_heartbeatThreadPool->AddTask(std::bind(&ChunkServerImpl::doRegister, this));
	}

	ChunkServerImpl::~ChunkServerImpl()
	{
		//
	}

	void ChunkServerImpl::doRegister()
	{
		RegisterRequest request;
		RegisterResponse response;
		request.set_chunkserveraddr(_dataServerAddress);
		request.set_diskquota(_blockManager->diskQuota());
		request.set_namespaceversion(_blockManager->namespaceVersion());
		request.set_tag(FLAGS_chunkserver_tag);

		//向nameserver注册
		if(!_nameServerClient->sendRequest(&NameServer_Stub::doRegister, &request, &response, 20))
		{
			_workThreadPool->DelayTask(5000, std::bind(&ChunkServerImpl::doRegister, this));
			return;
		}

		if(response.status() != kOK)
		{
			_workThreadPool->DelayTask(5000, std::bind(&ChunkServerImpl::doRegister, this));
			return;
		}

		if(response.reportinterval() != -1)
		{
			_param.set_reportinterval(response.reportinterval());
		}
		if(response.reportsize() != -1)
		{
			_param.set_recoversize(response.reportsize());
		}

		//判断当前版本是否最新
		int64_t newVersion = response.namespaceversion();
		if(_blockManager->namespaceVersion() != newVersion)
		{
			LOG(INFO, "Use new namespace version: %ld, clean local data", newVersion);
			if(!_blockManager->cleanUp(newVersion))
				LOG(FATAL, "Remove local blocks fail");
			if(!_blockManager->setNamespaceVersion(newVersion))
				LOG(FATAL, "SetNamespaceVersion fail");
			_workThreadPool->AddTask(std::bind(&ChunkServerImpl::doRegister, this));
			return;
		}

		_chunkServerId = response.chunkserverid();
		_reportId = response.reportid() + 1;
		_firstRoundReportStart = _lastReportBlockId;
		_isFirstRound = true;

		_workThreadPool->DelayTask(1, std::bind(&ChunkServerImpl::sendBlockReport, this));
		_workThreadPool->DelayTask(1, std::bind(&ChunkServerImpl::sendHeartbeat, this));


	}
}