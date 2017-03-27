// Auther: zackliu1995@hotmail.com

#include <stdio.h>

#include <sofa/pbrpc/pbrpc.h>
#include <gflags/gflags.h>
#include <common/logging.h>
#include <common/string_util.h>

#include "nameserver_impl.h"
#include "../util/version.h"

DECLARE_string(flagfile); //从文件读入参数
DECLARE_string(nameserver_node);

int main(int argc, char *argv[])
{
	if(argc > 1)
	{
		std::string extCmd = argv[1];
		if(extCmd == "Version")
		{
			printSystemVersion();
			return 0;
		}
	}
	if(FLAGS_flagfile == "")
	{
		FLAGS_flagfile = "./config.flag";
	}
	google::ParseCommandLineFlags(&argc, &argv, false);

	LOG(baidu::common::INFO, "NameServer start ...");

	//rpc server
	sofa::pbrpc::RpcServerOptions options;
	sofa::pbrpc::RpcServer rpcServer(options);

	//server
	LOG(baidu::common::INFO, "HA strategy: None"); //TODO: RAFT HA

	zfs::NameServerImpl *nameServerService = new zfs::NameServerImpl();

	//register
	if(!rpcServer.RegisterService(nameServerService))
	{
		return 1;
	}

	//start
	std::string listenAddr = std::string("127.0.0.1:") + FLAGS_nameserver_node;
	if(!rpcServer.Start(listenAddr))
	{
		return 1;
	}
	LOG(baidu::common::INFO, "RpcServer started.");

	rpcServer.Run();

	//delete
	LOG(baidu::common::INFO, "NameServer exit");
	return 0;
}
