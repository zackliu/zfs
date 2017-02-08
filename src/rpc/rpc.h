// Auther: zackliu1995@hotmail.com
// rpc 原型

#ifndef ZFS_RPC_H
#define ZFS_RPC_H

#include <assert.h>
#include <functional>
#include <sofa/pbrpc/pbrpc.h>
#include <common/mutex.h>
#include <common/thread_pool.h>
#include <common/logging.h>

namespace zfs {

class Rpc {
public:
    Rpc()
    {
        sofa::pbrpc::RpcClientOptions options;
        options.max_pending_buffer_size = 128;
        rpcClient = new sofa::pbrpc::RpcClient(options);
    }
    ~Rpc()
    {
        delete rpcClient;
    }

    //stub是某种service的实体，比如EchoServer_Stub. server是一个socket 比如 127.0.0.1:12321
    template <class T>
    bool getStub(const std::string server, T **stub)
    {
        MutexLock lock(&hostMapLock)  //加锁，到lock销毁时解锁
        sofa::pbrpc::RpcChannel *channel = NULL;
        HostMap::iterator it = hostMap.find(server);
        if(it == hostMap.end())
        {
            //没有这种server的channel
            sofa::pbrpc::RpcChannelOptions channelOptions;
            channel = new sofa::pbrpc::RpcChannel(rpcClient, server, channelOptions);
            hostMap[sever] = channel;
        }
        else
        {
            channel = it -> second;
        }

        *stub = new T(channel);
        return true;
    }

    template <class Stub, class Request, class Response, class Callback>
    bool sendRequest(Stub *stub, 
                     void(Stub::*func)(google::protobuf::RpcController*, const Request *request, Response *response, Callback *callback),
                     int rpcTimeout,
                     int rpcRetryTimes)
    {
        sofa::pbrpc::RpcController controller;
        for(int retry = 0; retry < rpcRetryTimes; retry++)
        {
            (stub -> *func)(&controller, request, response, NULL);
            if(controller.Failed())
            {
                if(retry == rpcRetryTimes - 1)
                {
                    LOG(WARNING, "SendRequest to %s, failed: %s\n", controller.RemoteAddress().c_str(), controller.ErrorText().c_str());
                }
                else
                {
                    LOG(DEBUG, "SendRequest failed, retry ...");
                }
            }
            else
            {
                return true;
            }
            controller.Reset();
        }
        return false;
    }

    template <class Stub, class Request, class Response, class Callback>
    void asyncRequest(Stub *stub,
                      void(Stub::*func)(google::protobuf::RpcController*, const Request*, Response*, Callback*),
                      int rpcTimeout,
                      int rpcRetryTimes)
    {
        sofa::pbrpc::RpcController *controller = new sofa::pbrpc::RpcController();
        google::protobuf::Closure* done = sofa::pbrpc::NewClosure(&Rpc::template RpcCallback<Request, Response, Callback>,
                                                                  controller, 
                                                                  request, 
                                                                  response, 
                                                                  callback);
        (stub->*func)(controller, request, response, done);
    }

    template <class Request, class Response, class Callback>
    static void rpcCallback(sofa::pbrpc::RpcController* rpcController,
                            const Request* request,
                            Response* response,
                            std::function<void (const Request*, Response*, bool, int)> callback)
    {
        bool failed = rpcController->Failed();
        int error = rpcController->ErrorCode();
        if (failed || error) 
        {
            assert(failed && error);
            if (error != sofa::pbrpc::RPC_ERROR_SEND_BUFFER_FULL) 
            {
                LOG(WARNING, "RpcCallback: %s %s\n",
                    rpc_controller->RemoteAddress().c_str(), rpc_controller->ErrorText().c_str());
            } 
        }
        delete rpc_controller;
        callback(request, response, failed, error);
    }
private:
    sofa::pbrpc::RpcClient *rpcClient;
    typedef std::map<std::string, sofa::pbrpc::RpcChannel*> HostMap;
    HostMap hostMap;
    Mutex hostMapLock;
};

}

#endif