//
// Created by zackliu on 4/4/17.
//

#include <stdio.h>
#include <signal.h>

#include <sofa/pbrpc/pbrpc.h>
#include <gflags/gflags.h>
#include <common/logging.h>
#include <zconf.h>

#include "chunkserver_impl.h"
#include "../util/version.h"

DECLARE_string(flagfile);
DECLARE_string(chunkserver_port);
DECLARE_string(block_store_path);
DECLARE_string(chunkserver_warninglog);
DECLARE_int32(chunkserver_log_level);
DECLARE_string(bfs_log);
DECLARE_int32(bfs_log_size);
DECLARE_int32(bfs_log_limit);

static volatile sig_atomic_t sQuit = 0;
static void signalIntHandler(int)
{
	sQuit = 1;
}

int main(int argc, char *argv[])
{
	if (argc > 1) {
		std::string extCmd = argv[1];
		if (extCmd == "version")
		{
//			PrintSystemVersion();
			return 0;
		}
	}
	if (FLAGS_flagfile == "")
	{
		FLAGS_flagfile = "./bfs.flag";
	}

	::google::ParseCommandLineFlags(&argc, &argv, false);
	if (FLAGS_bfs_log != "") {
		baidu::common::SetLogFile(FLAGS_bfs_log.c_str());
		baidu::common::SetLogSize(FLAGS_bfs_log_size);
		baidu::common::SetLogSizeLimit(FLAGS_bfs_log_limit);
	}
	baidu::common::SetLogLevel(FLAGS_chunkserver_log_level);
	baidu::common::SetWarningFile(FLAGS_chunkserver_warninglog.c_str());

	sofa::pbrpc::RpcServerOptions opstions;
	opstions.work_thread_num = 8;
	sofa::pbrpc::RpcServer *rpcServer = new sofa::pbrpc::RpcServer(opstions);

	zfs::ChunkServerImpl *chunkServerService = new zfs::ChunkServerImpl;

	if(!rpcServer->RegisterService(chunkServerService, false))
	{
		return -1;
	}

	if(!rpcServer->Start(std::string("0.0.0.0:") + FLAGS_chunkserver_port))
	{
		return -1;
	}

	signal(SIGINT, signalIntHandler);
	signal(SIGTERM, signalIntHandler);
	while(!sQuit)
	{
		sleep(1);
	}

	delete rpcServer;
	LOG(baidu::common::INFO, "RpcServer stop.");
	delete chunkServerService;
	LOG(baidu::common::INFO, "ChunkServer stop.");
	return EXIT_SUCCESS;

}