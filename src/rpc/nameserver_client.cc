//
// Created by zackliu on 4/1/17.
//

#include "nameserver_client.h"

#include <common/string_util.h>
#include "rpc.h"

namespace zfs
{
	NameServerClient::NameServerClient(Rpc *rpcClient, const std::string &nameServerNodes)
		:_rpcClient(rpcClient), _leaderId(0)
	{
		baidu::common::SplitString(nameServerNodes, ",", &_nameServerNodes);
		_stubs.resize(_nameServerNodes.size());
		for(uint32_t i = 0; i < _nameServerNodes.size(); i++)
		{
			_rpcClient->getStub(_nameServerNodes[i], &_stubs[i]);
		}
	}
}