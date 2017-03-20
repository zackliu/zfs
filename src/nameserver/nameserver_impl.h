// Auther: zackliu1995@hotmail.com

#ifndef ZFS_NAMESERVER_IMPL_H
#define ZFS_NAMESERVER_IMPL_H

#include <functional>
#include <common/thread_pool.h>

#include "proto/nameserver.pb.h"

#include <sofa/pbrpc/http.h>

namespace zfs
{

enum RecoverMode
{
    kStopRecover = 0;
    kHiOnly = 1;
    kRevocerAll = 2;
};

enum DisplayMode
{
    kDisplayAll = 0;
    kAliveOnly = 1;
    kDeadOnly = 2;
    kOverload = 3;
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
    void AddBlock(::google::protobuf::RpcController* controller,
                       const AddBlockRequest* request,
                       AddBlockResponse* response,
                       ::google::protobuf::Closure* done);
    void GetFileLocation(::google::protobuf::RpcController* controller,
                       const FileLocationRequest* request,
                       FileLocationResponse* response,
                       ::google::protobuf::Closure* done);
    void ListDirectory(::google::protobuf::RpcController* controller,
                       const ListDirectoryRequest* request,
                       ListDirectoryResponse* response,
                       ::google::protobuf::Closure* done);
    void Stat(::google::protobuf::RpcController* controller,
                       const StatRequest* request,
                       StatResponse* response,
                       ::google::protobuf::Closure* done);
    void Rename(::google::protobuf::RpcController* controller,
                       const RenameRequest* request,
                       RenameResponse* response,
                       ::google::protobuf::Closure* done);
    void Unlink(::google::protobuf::RpcController* controller,
                       const UnlinkRequest* request,
                       UnlinkResponse* response,
                       ::google::protobuf::Closure* done);
    void DeleteDirectory(::google::protobuf::RpcController* controller,
                         const DeleteDirectoryRequest* request,
                         DeleteDirectoryResponse* response,
                         ::google::protobuf::Closure* done);
    void SyncBlock(::google::protobuf::RpcController* controller,
                       const SyncBlockRequest* request,
                       SyncBlockResponse* response,
                       ::google::protobuf::Closure* done);
    void FinishBlock(::google::protobuf::RpcController* controller,
                       const FinishBlockRequest* request,
                       FinishBlockResponse* response,
                       ::google::protobuf::Closure* done);
    void ChangeReplicaNum(::google::protobuf::RpcController* controller,
                       const ChangeReplicaNumRequest* request,
                       ChangeReplicaNumResponse* response,
                       ::google::protobuf::Closure* done);
    void HeartBeat(::google::protobuf::RpcController* controller,
                       const HeartBeatRequest* request,
                       HeartBeatResponse* response,
                       ::google::protobuf::Closure* done);
    void Register(::google::protobuf::RpcController* controller,
                       const ::baidu::bfs::RegisterRequest* request,
                       ::baidu::bfs::RegisterResponse* response,
                       ::google::protobuf::Closure* done);
    void BlockReport(::google::protobuf::RpcController* controller,
                       const BlockReportRequest* request,
                       BlockReportResponse* response,
                       ::google::protobuf::Closure* done);
    void BlockReceived(::google::protobuf::RpcController* controller,
                       const BlockReceivedRequest* request,
                       BlockReceivedResponse* response,
                       ::google::protobuf::Closure* done);
    void PushBlockReport(::google::protobuf::RpcController* controller,
                       const PushBlockReportRequest* request,
                       PushBlockReportResponse* response,
                       ::google::protobuf::Closure* done);
    void SysStat(::google::protobuf::RpcController* controller,
                       const SysStatRequest* request,
                       SysStatResponse* response,
                       ::google::protobuf::Closure* done);
    void ShutdownChunkServer(::google::protobuf::RpcController* controller,
            const ShutdownChunkServerRequest* request,
            ShutdownChunkServerResponse* response,
            ::google::protobuf::Closure* done);
    void ShutdownChunkServerStat(::google::protobuf::RpcController* controller,
            const ShutdownChunkServerStatRequest* request,
            ShutdownChunkServerStatResponse* response,
            ::google::protobuf::Closure* done);
    void DiskUsage(::google::protobuf::RpcController* controller,
            const DiskUsageRequest* request,
            DiskUsageResponse* response,
            ::google::protobuf::Closure* done);
    void Symlink(::google::protobuf::RpcController* controller,
            const SymlinkRequest* request,
            SymlinkResponse* response,
            ::google::protobuf::Closure* done);
    void Chmod(::google::protobuf::RpcController* controller,
            const ChmodRequest* request,
            ChmodResponse* response,
            ::google::protobuf::Closure* done);
    bool WebService(const sofa::pbrpc::HTTPRequest&, sofa::pbrpc::HTTPResponse&);

private:
    void checkLeader();
    void rebuildBlockMapCallback(const FileInfo &fileInfo);
    void LogStatus();
    void checkRecoverMode();
    void LeaveReadOnly();
    void ListRecover(sofa::pbrpc::HTTPResponse* response);
    bool LogRemote(const NameServerLog& log, std::function<void (bool)> callback);
    void SyncLogCallback(::google::protobuf::RpcController* controller,
                         const ::google::protobuf::Message* request,
                         ::google::protobuf::Message* response,
                         ::google::protobuf::Closure* done,
                         std::vector<FileInfo>* removed,
                         bool ret);
    void TransToString(const std::map<int32_t, std::set<int64_t> >& chk_set,
                       std::string* output);
    void TransToString(const std::set<int64_t>& block_set, std::string* output);
    void CallMethod(const ::google::protobuf::MethodDescriptor* method,
                    ::google::protobuf::RpcController* controller,
                    const ::google::protobuf::Message* request,
                    ::google::protobuf::Message* response,
                    ::google::protobuf::Closure* done);
    bool CheckFileHasBlock(const FileInfo& file_info,
                           const std::string& file_name,
                           int64_t block_id);
    void SetActualFileSize(FileInfo* file);

private:
        ThreadPool *readThreadPool;
        ThreadPool *workThreadPool;
        ThreadPool *reportThreadPool;
        ThreadPool *heartbeatThreadPool;

        ChunkServerManager *chunkserverManager;

        BlockMappingManager *blockMappingManager;

        volatile bool readonly;
        volatile int recoverTimeout;
        RecoverMode recoverMode;
        int64_t startTime;

        NameSpcae *_namespace;
        bool isLeader;

};
}


#endif