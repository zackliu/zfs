// Auther: zackliu1995@hotmail.com

#ifndef ZFS_NAMESPACE_H
#define ZFS_NAMESPACE_H

#include <stdint.h>
#include <string>
#include <functional>
#include <common/mutex.h>

#include <leveldb/db.h>
#include "proto/nameserver.pb.h"
#include "proto/status_code.pb.h"

namespace zfs
{
class NameSpace
{
public:
    NameSpace();
    void activate(std::function<void (const FileInfo&)> rebuild_callback, NameServerLog *log);
    ~NameSpace();

    StatusCode listDirectory(const std::string &path, google::protobuf::RepeatedPtrField<FileInfo>* outputs);
    StatusCode createFile(const std::string &fileName, int flag, int mode, int replicaNum, std::vector<int64_t>* blocksToRemove, NameServerLog *log = NULL);
    StatusCode removeFile(const std::string &path, FileInfo *fileRemoved, NameServerLog *log = NULL);
    StatusCode deleteDirectory(const std::string &path, bool recursive, std::vector<FileInfo*> fileRemoved, NameServerLog *log = NULL);
    StatusCode diskUsage(const std::string &path, uint64_t *diskUseageSize);
    StatusCode Rename(const std::string &oldPath, const std::string &newPath, bool *needUnlink, FileInfo *removedFile, NameServerLog *log = NULL);
    StatusCode Symlink(const std::string &src, const std::string &dst, NameServerLog *log = NULL);

    bool getFileInfo(const std::string &path, FileInfo *fileInfo);
    bool updateFileInfo(const FileInfo &fileInfo, NameServerLog* log = NULL);
    bool deleteFileInfo(const std::string fileKey, NameServerLog* log = NULL);
    int64_t getVersion() const;
    bool rebuildBlockMap(std::function<void (const FileInfo&)> callback);
    static std::string normalizePath(const std::string &path);
    void tailLog(const std::string &log);
    int64_t getNewBlockId();

private:
    enum FileType
    {
        kDefault = 0,
        kDir = 1,
        kSymlink = 2,
    };

    FileType getFileType(int type);
    bool getLinkSrcPath(const FileInfo &info, FileInfo *srcInfo);
    StatusCode buildPath(const std::string &path, FileInfo *fileInfo, std::string *fileName, NameServerLog* log = NULL);
    static void encodingStoreKey(int64_t entryId, const std::string &path, std::string *keyStr);
    static void decodingStoreKey(const std::string &keyStr, int64_t entryId, std::string *path);
    bool getFromStore(const std::string &key, FileInfo *info);
    void setupRoot();
    bool lookup(const std::string &path, FileInfo *info);
    bool lookup(int64_t pid, const std::string &name, FileInfo *info);
    StatusCode internalDeleteDirectory(const FileInfo &dirInfo, bool recursive, std::vector<std::string> *filesRemoved, NameServerLog *log);
    StatusCode InternalComputeDiskUsage(const FileInfo &info, uint64_t *diskUsageSIze);
    uint32_t encodeLog(NameServerLog *log, int32_t type, const std::string &key, const std::string &value);
    void initBlockIdUpBound(NameServerLog *log);
    void updateBlockIdUpBound(NameServerLog *log);

private:
    leveldb::DB *db;
    int64_t version;
    volatile int64_t lastEntryId;
    FileInfo rootPath;
    int64_t blockIdUpBound;
    int64_t nextBlockId;
    Mutex mu;
};
}

#endif