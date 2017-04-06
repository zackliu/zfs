// Auther: zackliu1995@hotmail.com

#ifndef ZFS_NAMESERVER_IMPL_H
#define ZFS_NAMESERVER_IMPL_H

#include <functional>
#include <common/thread_pool.h>

#include "namespace.h"
#include "block_mapping.h"
#include "chunkserver_manager.h"

#include "../proto/nameserver.pb.h"

#include <sofa/pbrpc/http.h>

namespace zfs
{

enum RecoverMode
{
    kStopRecover = 0,
    kHiOnly = 1,
    kRevocerAll = 2,
};

enum DisplayMode
{
    kDisplayAll = 0,
    kAliveOnly = 1,
    kDeadOnly = 2,
    kOverload = 3,
};

class NameServerImpl : public NameServer 
{
public:
    NameServerImpl();
    virtual ~NameServerImpl();
    void createFile(::google::protobuf::RpcController* controller,
                       const CreateFileRequest* request,
                       CreateFileResponse* response,
                       ::google::protobuf::Closure* done);
    void addBlock(::google::protobuf::RpcController* controller,
                       const AddBlockRequest* request,
                       AddBlockResponse* response,
                       ::google::protobuf::Closure* done);
//    void getFileLocation(::google::protobuf::RpcController* controller,
//                       const FileLocationRequest* request,
//                       FileLocationResponse* response,
//                       ::google::protobuf::Closure* done);
//    void listDirectory(::google::protobuf::RpcController* controller,
//                       const ListDirectoryRequest* request,
//                       ListDirectoryResponse* response,
//                       ::google::protobuf::Closure* done);
//    void stat(::google::protobuf::RpcController* controller,
//                       const StatRequest* request,
//                       StatResponse* response,
//                       ::google::protobuf::Closure* done);
//    void rename(::google::protobuf::RpcController* controller,
//                       const RenameRequest* request,
//                       RenameResponse* response,
//                       ::google::protobuf::Closure* done);
//    void unlink(::google::protobuf::RpcController* controller,
//                       const UnlinkRequest* request,
//                       UnlinkResponse* response,
//                       ::google::protobuf::Closure* done);
//    void deleteDirectory(::google::protobuf::RpcController* controller,
//                         const DeleteDirectoryRequest* request,
//                         DeleteDirectoryResponse* response,
//                         ::google::protobuf::Closure* done);
//    void syncBlock(::google::protobuf::RpcController* controller,
//                       const SyncBlockRequest* request,
//                       SyncBlockResponse* response,
//                       ::google::protobuf::Closure* done);
//    void finishBlock(::google::protobuf::RpcController* controller,
//                       const FinishBlockRequest* request,
//                       FinishBlockResponse* response,
//                       ::google::protobuf::Closure* done);
//    void changeReplicaNum(::google::protobuf::RpcController* controller,
//                       const ChangeReplicaNumRequest* request,
//                       ChangeReplicaNumResponse* response,
//                       ::google::protobuf::Closure* done);
    void heartBeat(::google::protobuf::RpcController* controller,
                       const HeartBeatRequest* request,
                       HeartBeatResponse* response,
                       ::google::protobuf::Closure* done);
    void doRegister(::google::protobuf::RpcController* controller,
                       const ::zfs::RegisterRequest* request,
                       ::zfs::RegisterResponse* response,
                       ::google::protobuf::Closure* done);
    void blockReport(::google::protobuf::RpcController* controller,
                       const BlockReportRequest* request,
                       BlockReportResponse* response,
                       ::google::protobuf::Closure* done);
//    void blockReceived(::google::protobuf::RpcController* controller,
//                       const BlockReceivedRequest* request,
//                       BlockReceivedResponse* response,
//                       ::google::protobuf::Closure* done);
//    void pushBlockReport(::google::protobuf::RpcController* controller,
//                       const PushBlockReportRequest* request,
//                       PushBlockReportResponse* response,
//                       ::google::protobuf::Closure* done);
//    void sysStat(::google::protobuf::RpcController* controller,
//                       const SysStatRequest* request,
//                       SysStatResponse* response,
//                       ::google::protobuf::Closure* done);
//    void shutdownChunkServer(::google::protobuf::RpcController* controller,
//            const ShutdownChunkServerRequest* request,
//            ShutdownChunkServerResponse* response,
//            ::google::protobuf::Closure* done);
//    void shutdownChunkServerStat(::google::protobuf::RpcController* controller,
//            const ShutdownChunkServerStatRequest* request,
//            ShutdownChunkServerStatResponse* response,
//            ::google::protobuf::Closure* done);
//    void diskUsage(::google::protobuf::RpcController* controller,
//            const DiskUsageRequest* request,
//            DiskUsageResponse* response,
//            ::google::protobuf::Closure* done);
//    void symlink(::google::protobuf::RpcController* controller,
//            const SymlinkRequest* request,
//            SymlinkResponse* response,
//            ::google::protobuf::Closure* done);
//    void chmod(::google::protobuf::RpcController* controller,
//            const ChmodRequest* request,
//            ChmodResponse* response,
//            ::google::protobuf::Closure* done);
//    bool webService(const sofa::pbrpc::HTTPRequest&, sofa::pbrpc::HTTPResponse&);

private:

    void checkLeader();
    void rebuildBlockMapCallback(const FileInfo &fileInfo);
    void logStatus();
    void checkRecoverMode();
    void leaveReadOnly();
//    void listRecover(sofa::pbrpc::HTTPResponse* response);
    bool logRemote(const NameServerLog& log, std::function<void (bool)> callback);
    void syncLogCallback(::google::protobuf::RpcController* controller,
                         const ::google::protobuf::Message* request,
                         ::google::protobuf::Message* response,
                         ::google::protobuf::Closure* done,
                         std::vector<FileInfo>* removed,
                         bool ret);
//    void transToString(const std::map<int32_t, std::set<int64_t> >& chk_set,
//                       std::string* output);
//    void transToString(const std::set<int64_t>& block_set, std::string* output);
//    void callMethod(const ::google::protobuf::MethodDescriptor* method,
//                    ::google::protobuf::RpcController* controller,
//                    const ::google::protobuf::Message* request,
//                    ::google::protobuf::Message* response,
//                    ::google::protobuf::Closure* done);
//    bool checkFileHasBlock(const FileInfo& file_info,
//                           const std::string& file_name,
//                           int64_t block_id);
//    void setActualFileSize(FileInfo* file);

private:
        baidu::common::ThreadPool *_readThreadPool;
		baidu::common::ThreadPool *_workThreadPool;
        baidu::common::ThreadPool *_reportThreadPool;
        baidu::common::ThreadPool *_heartbeatThreadPool;

        ChunkServerManager *_chunkserverManager;
        BlockMapping *_blockMapping;

        volatile bool _readonly;
        volatile int _recoverTimeout;
        RecoverMode _recoverMode;
        int64_t _startTime;

        NameSpace *_namespace;
        bool _isLeader;

};
}


#endif