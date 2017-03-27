// Auther: zackliu1995@hotmail.com

#include "nameserver_impl.h"

#include <set>
#include <map>
#include <sstream>
#include <cstdlib>

#include <gflags/gflags.h>
#include <sofa/pbrpc/pbrpc.h>

#include <common/counter.h>
#include <common/logging.h>
#include <common/string_util.h>

#include "block_mapping.h"
#include "chunkserver_manager.h"
#include "namespace.h"

#include "../proto/status_code.pb.h"

DECLARE_bool(bfs_web_kick_enable);
DECLARE_int32(nameserver_start_recover_timeout);
DECLARE_int32(chunkserver_max_pending_buffers);
DECLARE_int32(nameserver_report_thread_num);
DECLARE_int32(nameserver_work_thread_num);
DECLARE_int32(nameserver_read_thread_num);
DECLARE_int32(nameserver_heartbeat_thread_num);
DECLARE_int32(blockmapping_bucket_num);
DECLARE_int32(hi_recover_timeout);
DECLARE_int32(lo_recover_timeout);
DECLARE_int32(block_report_timeout);
DECLARE_bool(clean_redundancy);

using baidu::common::LogLevel::INFO;
using baidu::common::LogLevel::DEBUG;
using baidu::common::LogLevel::ERROR;
using baidu::common::LogLevel::FATAL;
using baidu::common::LogLevel::WARNING;


namespace zfs
{

	baidu::common::Counter gGetLocation;
	baidu::common::Counter gAddBlock;
	baidu::common::Counter gHeartBeat;
	baidu::common::Counter gBlockReport;
	baidu::common::Counter gUnlink;
	baidu::common::Counter gCreateFile;
	baidu::common::Counter gListDir;
	baidu::common::Counter gReportBlocks;
	extern baidu::common::Counter gBlocksNum;

	NameServerImpl::NameServerImpl(): _readonly(true), _recoverTimeout(FLAGS_nameserver_start_recover_timeout),
	                _recoverMode(kStopRecover)
	{
		_blockMapping = new BlockMapping();
	    _reportThreadPool = new baidu::common::ThreadPool(FLAGS_nameserver_report_thread_num);
	    _readThreadPool = new baidu::common::ThreadPool(FLAGS_nameserver_read_thread_num);
	    _workThreadPool = new baidu::common::ThreadPool(FLAGS_nameserver_work_thread_num);
	    _heartbeatThreadPool = new baidu::common::ThreadPool(FLAGS_nameserver_heartbeat_thread_num);
	    _chunkserverManager = new ChunkServerManager(_workThreadPool, _blockMapping);
	    _namespace = new NameSpace();
	    checkLeader();
	    _startTime = baidu::common::timer::get_micros();
//	    _readThreadPool->AddTask(std::bind(&NameServerImpl::logStatus, this));
	}

	NameServerImpl::~NameServerImpl()
	{

	}

	void NameServerImpl::checkLeader()
	{
	    LOG(INFO, "Leader nameserver, rebuild block map.");
	    NameServerLog log;
	    std::function<void (const FileInfo&) > task = std::bind(&NameServerImpl::rebuildBlockMapCallback, this, std::placeholders::_1);
	    _namespace->activateDb(task, &log);
	    if(!logRemote(log, std::function<void (bool)>()))
	    {
	        LOG(FATAL, "logremote namespace update fail");
	    }
	    _recoverTimeout = FLAGS_nameserver_start_recover_timeout;
	    _startTime = baidu::common::timer::get_micros();
	    _workThreadPool->DelayTask(1000, std::bind(&NameServerImpl::checkRecoverMode, this));
	    _isLeader = true;
	}

	void NameServerImpl::rebuildBlockMapCallback(const FileInfo &fileInfo)
	{
	    for(int i = 0; i < fileInfo.blocks_size(); i++)
	    {
	        int64_t blockId = fileInfo.blocks(i);
	        int64_t version = fileInfo.version();
	        _blockMapping->rebuildBlock(blockId, fileInfo.replicas(), version, fileInfo.size());
	    }
	}

	void NameServerImpl::checkRecoverMode()
	{
	    int nowTime = (baidu::common::timer::get_micros() - _startTime) / 1000000;
	    int recoverTimeoutTemp = _recoverTimeout;
	    if(recoverTimeoutTemp == 0)
	    {
	        return;
	    }
	    int newRecoverTimeout = FLAGS_nameserver_start_recover_timeout - nowTime;
	    if(newRecoverTimeout <= 0)
	    {
	        LOG(INFO, "Now time %s", nowTime);
	        _recoverMode = kRevocerAll;
	        return;
	    }
	    baidu::common::atomic_comp_swap(&_recoverTimeout, newRecoverTimeout, _recoverTimeout);
	    _workThreadPool->DelayTask(1000, std::bind(&NameServerImpl::checkRecoverMode, this));
	}

//	void NameServerImpl::logStatus()
//	{
//
//	}

	bool NameServerImpl::logRemote(const NameServerLog &log, std::function<void(bool)> callback)
	{
		_workThreadPool->AddTask(std::bind(callback, true));
		return true;
	}

	void NameServerImpl::createFile(::google::protobuf::RpcController* controller,
	                       const CreateFileRequest* request,
	                       CreateFileResponse* response,
	                       ::google::protobuf::Closure* done)
	{
	    gCreateFile.Inc();
		response->set_sequenceid(request->sequenceid());
		std::string path = NameSpace::normalizePath(request->filename());
		int flags = request->flags();
		int mode = request->mode();
		if(mode == 0)
		{
			mode = 0644;
		}
		int replicaNum = request->replicanum();
		NameServerLog log;

		std::vector<int64_t> blocksToRemove;
		StatusCode status = _namespace->createFile(path, flags, mode, replicaNum, blocksToRemove, &log);
		for(int i = 0; i < blocksToRemove.size(); i++)
		{
			_blockMapping->removeBlock(blocksToRemove[i]);
		}
		response->set_status(status);
		sofa::pbrpc::RpcController *ctl = reinterpret_cast<sofa::pbrpc::RpcController*>(controller);
		LOG(INFO, "Sdk %s create file %s returns %s",
		    ctl->RemoteAddress().c_str(), path.c_str(), StatusCode_Name(status).c_str());
		if(status != kOK)
		{
			done->Run();
			return;
		}

		done->Run();
	}

}