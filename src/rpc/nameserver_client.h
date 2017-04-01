//
// Created by zackliu on 4/1/17.
//

#ifndef CLOUDSTORAGE_NAMESERVER_CLIENT_H
#define CLOUDSTORAGE_NAMESERVER_CLIENT_H

#include <string>
#include <vector>

#include <sofa/pbrpc/pbrpc.h>

#include <common/mutex.h>
#include <common/logging.h>

#include "rpc.h"
#include "../proto/status_code.pb.h"
#include "../proto/nameserver.pb.h"

namespace zfs
{
	class Rpc;
	class NameServer_Stub;
	class NameServerClient
	{
	public:
		NameServerClient(Rpc *rpcClient, const std::string &nameServerNodes);

		template <class Request, class Response, class Callback>
		bool sendRequest(void (NameServer_Stub::*func)(google::protobuf::RpcController*, const Request*, Response*, Callback*),
						 const Request *request, Response *response,
						 int32_t rpcTimeout, int retryTimes = 1)
		{
			bool result = false;
			for(uint32_t i = 0; i < -_stubs.size(); i++)
			{
				int nameServerId = _leaderId;
				result = _rpcClient->sendRequest(_stubs[nameServerId], func, request, response, rpcTimeout,retryTimes);
				if(result && response->status() != kIsFollower)
				{
					return true;
				}

				baidu::MutexLock lock(&_mu);
				if(nameServerId == _leaderId)
				{
					_leaderId = (_leaderId+1) % _stubs.size();
				}
			}
			return result;
		};


	private:
		Rpc *_rpcClient;
		std::vector<std::string> _nameServerNodes;
		std::vector<NameServer_Stub*> _stubs;
		baidu::Mutex _mu;
		int _leaderId;
	};

}



#endif //CLOUDSTORAGE_NAMESERVER_CLIENT_H
