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

	void NameServerImpl::syncLogCallback(::google::protobuf::RpcController *controller,
	                                     const ::google::protobuf::Message *request,
	                                     ::google::protobuf::Message *response, ::google::protobuf::Closure *done,
	                                     std::vector<FileInfo> *removed, bool ret)
	{
		done->Run();
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
		StatusCode status = _namespace->createFile(path, flags, mode, replicaNum, &blocksToRemove, &log);
		for(int i = 0; i < blocksToRemove.size(); i++)
		{
			_blockMapping->removeBlock(blocksToRemove[i], NULL);
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

	void NameServerImpl::addBlock(::google::protobuf::RpcController *controller, const AddBlockRequest *request,
	                              AddBlockResponse *response, ::google::protobuf::Closure *done)
	{
		response->set_sequenceid(request->sequenceid());
		if(_readonly)
		{
			LOG(INFO, "AddBlock failed: %s, ReadOnly", request->filename().c_str());
			response->set_status(kSafeMode);
			done->Run();
			return;
		}

		gAddBlock.Inc();
		std::string path = NameSpace::normalizePath(request->filename());
		FileInfo fileInfo;
		if(!_namespace->getFileInfo(path, &fileInfo))
		{
			LOG(INFO, "AddBlock Failed: %s, cannot find file", request->filename().c_str());
			response->set_status(kNsNotFound);
			done->Run();
			return;
		}

		if(fileInfo.blocks_size() > 0) //删除原有的block
		{
			std::map<int64_t, std::set<int32_t> > blockToRemove;
			_blockMapping->removeBlocksForFile(fileInfo, &blockToRemove);
			for(auto it = blockToRemove.begin(); it != blockToRemove.end(); it++)
			{
				auto chunkServers = it->second;
				for(auto csIt = chunkServers.begin(); csIt != chunkServers.end(); csIt++)
				{
					_chunkserverManager->removeBlock(*csIt, it->first);
				}
			}
			fileInfo.clear_blocks();
		}

		int replicaNum = fileInfo.replicas();

		std::vector<std::pair<int32_t , std::string> > chains;
		baidu::common::timer::TimeChecker addBlockTimer;
		if(_chunkserverManager->getChunkServerChains(replicaNum, &chains, request->clientaddress()))
		{
			addBlockTimer.Check(50*1000, "GetChunkServerChains");
			int64_t newBlockId = _namespace->getNewBlockId();
			fileInfo.add_blocks(newBlockId);
			fileInfo.set_version(-1);
			for(int i = 0; i < replicaNum; i++)
			{
				fileInfo.add_csaddrs(_chunkserverManager->getChunkServerAddress(chains[i].first));
			}

			NameServerLog log;
			if(!_namespace->updateFileInfo(fileInfo, &log))
			{
				LOG(WARNING, "Update File info failed: %s", path.c_str());
				response->set_status(kUpdateError);
			}

			LocatedBlock *block = response->mutable_block();
			std::vector<int32_t > replicas;
			for(int i = 0; i < replicaNum; i++)
			{
				ChunkServerInfo *info = block->add_chains();
				int32_t csId = chains[i].first;
				info->set_address(chains[i].second);
				LOG(INFO, "Add C%d %s to #%ld, response", chains[i].first, chains[i].second.c_str(), newBlockId);
				replicas.push_back(csId);
				addBlockTimer.Reset();
				_chunkserverManager->addBlock(csId, newBlockId);
				addBlockTimer.Check(50*1000, "AddBlock");
			}

			_blockMapping->addBlock(newBlockId, replicaNum, replicas);
			addBlockTimer.Check(1000*50, "AddNewBlock");
			block->set_blockid(newBlockId);
			response->set_status(kOK);
			//logRemote(log, std::bind(&NameServerImpl::syncLogCallback, this, controller, request, response, done, (std::vector<FileInfo>*)NULL, std::placeholders::_1));
			//done->Run();
			_workThreadPool->AddTask(std::bind(&NameServerImpl::syncLogCallback, this, controller, request, response, done, (std::vector<FileInfo>*)NULL, true));
		}
		else
		{
			LOG(WARNING, "AddBlock Failed: %s", path.c_str());
			response->set_status(kGetChunkServerError);
			done->Run();
		}
	}

	void NameServerImpl::blockReport(::google::protobuf::RpcController *controller, const BlockReportRequest *request,
	                                 BlockReportResponse *response, ::google::protobuf::Closure *done)
	{
		gBlockReport.Inc();

		int32_t csId = request->chunkserverid();
		int64_t reportId = request->reportid();

		const ::google::protobuf::RepeatedPtrField<ReportBlockInfo> &blocks = request->blocks();

		int64_t startReport = baidu::common::timer::get_micros();
		int oldId = _chunkserverManager->getChunkServerId(request->chunkserveraddr());
		if(csId != oldId)
		{
			response->set_status(kUnknownCs);
			done->Run();
			return;
		}

		//根据获取的block更新blockmapping
		std::set<int64_t > insertBlocks;
		for(int i = 0; i < blocks.size(); i++)
		{
			gReportBlocks.Inc();
			const ReportBlockInfo &block = blocks.Get(i);
			int64_t curBlockId = block.blockid();
			int64_t curBlockSize = block.blocksize();

			int64_t blockVersion = block.version();
			if(!_blockMapping->updateBlockInfo(curBlockId, csId, curBlockSize, blockVersion))
			{
				response->add_obsoleteblocks(curBlockId);
				_chunkserverManager->removeBlock(csId, curBlockId);
				continue;
			}
			else insertBlocks.insert(curBlockId);
		}

		std::vector<int64_t> lost;
		int64_t  result = _chunkserverManager->addBlockWithCheck(csId, insertBlocks, request->start(), request->end(), &lost, reportId);
		if(lost.size() != 0)
		{
			for(int i = 0; i < lost.size(); i++)
			{
				_blockMapping->dealWithDeadBlock(csId, lost[i]);
			}
		}
		response->set_reportid(result);

		if(_recoverMode != kStopRecover)
		{
			RecoverVec recoverBlocks;
			int hiNum = 0;
			_chunkserverManager->PickRecoverBlocks(csId, &recoverBlocks, &hiNum, _recoverMode == kHiRecover);
			for(auto it = recoverBlocks.begin(); it != recoverBlocks.end(); it++)
			{
				ReplicaInfo *rep = response->add_newreplicas();
				rep->set_blockid((*it).first);
				for(auto destIt = it->second.begin(); destIt != it->second.end(); destIt++)
				{
					rep->add_chunkserveraddress(*destIt);
				}
				rep->set_recovertimeout(FLAGS_lo_recover_timeout);
			}
		}

		_blockMapping->getCloseBlocks(csId, response->mutable_closeblocks());
		response->set_status(kOK);
		done->Run();
	}

	void NameServerImpl::doRegister(::google::protobuf::RpcController *controller,
	                                const ::zfs::RegisterRequest *request, ::zfs::RegisterResponse *response,
	                                ::google::protobuf::Closure *done)
	{
		sofa::pbrpc::RpcController *sofaCntl = reinterpret_cast<sofa::pbrpc::RpcController*>(controller);

		const std::string &address = request->chunkserveraddr();
		const std::string &ipAddress = sofaCntl->RemoteAddress();
		const std::string csIp = ipAddress.substr(ipAddress.find(':'));

		int64_t version = request->namespaceversion();
		if(version != _namespace->getVersion())
		{
			_chunkserverManager->removeChunkServer(address);
		}
		else
		{
			if(_chunkserverManager->handleRegister(csIp, request, response))
			{
				//当chunkserver数量大于expect
				leaveReadOnly();
			}
		}
		response->set_namespaceversion(_namespace->getVersion());
		done->Run();
	}

	void NameServerImpl::leaveReadOnly()
	{
		if(_readonly) _readonly = false;
	}

}