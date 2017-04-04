//
// Created by zackliu on 3/29/17.
//

#include "fs_impl.h"

#include <gflags/gflags.h>

#include <common/sliding_window.h>
#include <common/logging.h>
#include <common/string_util.h>
#include <common/tprinter.h>
#include <common/util.h>

#include "../proto/status_code.pb.h"
#include "../proto/chunkserver.pb.h"
#include "../rpc/rpc.h"
#include "../rpc/nameserver_client.h"

#include "file_impl.h"
#include "file_impl_wrapper.h"
#include "zfs.h"


DECLARE_int32(sdk_thread_num);
DECLARE_string(nameserver_nodes);
DECLARE_string(sdk_write_mode);

namespace zfs
{
	FileSystemImpl::FileSystemImpl() : _rpcClient(NULL), _nameServerClient(NULL), _leaderNameServerIdx(0)
	{
		_localhostName = baidu::common::util::GetLocalHostName();
		_threadPool = new baidu::common::ThreadPool(FLAGS_sdk_thread_num);
	}

	FileSystemImpl::~FileSystemImpl()
	{
		delete _nameServerClient;
		delete _rpcClient;
		_threadPool->Stop(true);
		delete _threadPool;
	}

	bool FileSystem::openFileSystem(const char *nameServer, FileSystem **fs, const FSOptions &)
	{
		FileSystemImpl *impl = new FileSystemImpl;
		if(!impl->connectNameServer(nameServer))
		{
			*fs = NULL;
			return false;
		}
		*fs = impl;
		return true;
	}

	bool FileSystemImpl::connectNameServer(const char *nameServer)
	{
		std::string nameServerNodes = FLAGS_nameserver_nodes;
		if(nameServer != NULL)
		{
			nameServerNodes = std::string(nameServer);
		}

		_rpcClient = new Rpc();
		_nameServerClient = new NameServerClient(_rpcClient, nameServerNodes);
		return true;
	}

	int32_t FileSystemImpl::openFile(const char *path, int32_t flags, File **file, const WriteOptions &writeOptions)
	{
		return openFile(path, flags, 0, file, writeOptions);
	}

	int32_t FileSystemImpl::openFile(const char *path, int32_t flags, int32_t mode, File **file,
	                                 const WriteOptions &options)
	{
		if(!flags & O_WRONLY)
		{
			return BAD_PARAMETER;
		}

		WriteOptions writeOptions = options;
		if(options.writeMode == kWriteDefault)
		{
			if(FLAGS_sdk_write_mode == "fanout") writeOptions.writeMode = kWriteFanout;
			else if(FLAGS_sdk_write_mode == "chains") writeOptions.writeMode = kWriteChains;
			else LOG(FATAL, "wrong flag %s for sdk write mode", FLAGS_sdk_write_mode.c_str());
		}

		baidu::common::timer::AutoTimer autoTimer(100, "OpenFIle", path);
		int32_t  result = OK;
		*file = NULL;

		CreateFileRequest request;
		CreateFileResponse response;

		request.set_filename(path);
		request.set_sequenceid(0);
		request.set_flags(flags);
		request.set_mode(mode & 0777);
		request.set_replicanum(writeOptions.replica);

		bool rpcResult = _nameServerClient->sendRequest(&NameServer_Stub::createFile, &request, &response, 15, 1);
		if(!rpcResult || response.status() != kOK)
		{
			LOG(WARNING, "Open file for write fail: %s, rpc_ret= %d", path, rpcResult);
			result = TIMEOUT;
		}
		else
		{
			*file = new FileImplWrapper(this, _rpcClient, path, flags, writeOptions);
		}
		return result;
	}







}