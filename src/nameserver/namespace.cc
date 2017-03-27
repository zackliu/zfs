// Auther: zackliu1995@hotmail.com

#include "namespace.h"

#include <fcntl.h>
#include <gflags/gflags.h>
#include <leveldb/db.h>
#include <leveldb/cache.h>
#include <leveldb/write_batch.h>
#include <common/logging.h>
#include <common/timer.h>
#include <common/util.h>
#include <common/atomic.h>
#include <common/string_util.h>

DECLARE_string(namedbPath);
DECLARE_int64(namedbCacheSize);
DECLARE_int32(defaultReplicaNum);
DECLARE_int32(blockIdAllocationSize);
DECLARE_bool(checkOrphan);

using baidu::common::LogLevel::INFO;
using baidu::common::LogLevel::DEBUG;
using baidu::common::LogLevel::ERROR;
using baidu::common::LogLevel::FATAL;
using baidu::common::LogLevel::WARNING;

const int64_t kRootEntryId = 1;

namespace zfs
{
NameSpace::NameSpace():version(0), lastEntryId(1), blockIdUpBound(1), nextBlockId(1)
{
    leveldb::Options options;
    options.create_if_missing = true;
    options.block_cache = leveldb::NewLRUCache(64 * 1024L);
    leveldb::Status s = leveldb::DB::Open(options, "./db", &db);
    if(!s.ok())
    {
        db = NULL;
        LOG(baidu::common::ERROR, "Open leveldb fails: %s", s.ToString().c_str());
        exit(1);
    }
    
    //activateDb(NULL, NULL);
}

NameSpace::~NameSpace()
{
	delete db;
	db = NULL;
}

void NameSpace::activateDb(std::function<void (const FileInfo&)> callback, NameServerLog *log)
{
    std::string versionKey(8, 0);
    versionKey.append("version");
    std::string versionVal;
    leveldb::Status s = db -> Get(leveldb::ReadOptions(), versionKey, &versionVal);
    if(s.ok())
    {
        if(versionVal.size() != sizeof(int64_t))
        {
            LOG(FATAL, "Bad namespace version len = %lu", versionVal.size());
        }
        version = *(reinterpret_cast<int64_t*>(&versionVal[0]));
        LOG(INFO, "Load namespace version: %ld", version);
    }
    else
    {
        version = baidu::common::timer::get_micros();
        versionVal.resize(8);
        *(reinterpret_cast<int64_t*>(&versionVal[0])) = version;

        leveldb::Status s = db -> Put(leveldb::WriteOptions(), versionKey, versionVal);
        if(!s.ok())
        {
            LOG(FATAL, "Write NameSpace version failed %s", s.ToString().c_str());
        }
        encodeLog(log, kSyncWrite, versionKey, versionVal);
        LOG(INFO, "Create new namespace version: %ld", version);
    }
    setupRoot(); //设置根目录的fileinfo
    rebuildBlockMap(callback); //按照db中的内容重建blockmap
	initBlockIdUpbound(log);
}

void NameSpace::setupRoot()
{
    rootPath.set_entryid(kRootEntryId);
    rootPath.set_name("");
    rootPath.set_parententryid(kRootEntryId);
    rootPath.set_type(01755);//八进制
    rootPath.set_ctime(static_cast<uint32_t>(version / 1000000));

}

bool NameSpace::rebuildBlockMap(std::function<void (const FileInfo&)> callback)
{
    int64_t blockNum = 0;
    int64_t fileNum = 0;
    int64_t linkNum = 0;
    std::set<int64_t> entryIdSet;
    entryIdSet.insert(rootPath.entryid());
    leveldb::Iterator *it = db -> NewIterator(leveldb::ReadOptions());
    for(it->Seek(std::string(7, '\0') + "\1"); it -> Valid(); it -> Next()) //从fatherEntryId=00000001开始找
    {
        FileInfo fileInfo;
        bool ret = fileInfo.ParseFromArray(it->value().data(), it->value().size());
        assert(ret);

        if(lastEntryId < fileInfo.entryid())
        {
            lastEntryId = fileInfo.entryid();
        }

        FileType fileType = getFileType(fileInfo.type());
        if(fileType == kDefault)
        {
            for(int i = 0; i < fileInfo.blocks_size(); i++)
            {
                if(fileInfo.blocks(i) >= nextBlockId)
                {
                    nextBlockId = fileInfo.blocks(i)+1;
                    blockIdUpBound = nextBlockId;
                }
                blockNum++;
            }
            fileNum++;
            if(callback)
            {
                callback(fileInfo);
            }
        }
        else if(fileType == kSymlink)
        {
            linkNum++;
        }
        else
        {
            entryIdSet.insert(fileInfo.entryid());
        }
    }

    LOG(INFO, "rebuildBlockMap done. %ld directories, %ld symlinks, %ld files, %lu blocks, lastEntryId = E%ld",
        entryIdSet.size(), linkNum, fileNum, blockNum, lastEntryId);

    if(FLAGS_checkOrphan)
    {
        std::vector<std::pair<std::string, std::string> > orphanEntries;
        for(it -> Seek(std::string(7, '\0') + "\1"); it -> Valid(); it -> Next())
        {
            FileInfo fileInfo;
            bool ret = fileInfo.ParseFromArray(it->value().data(), it->value().size());
            assert(ret);
            int64_t parentEntryId = 0;
            std::string fileName;
            decodingStoreKey(it->key().ToString(), &parentEntryId, &fileName);
            if(entryIdSet.find(parentEntryId) == entryIdSet.end())
            {
                LOG(WARNING, "Orphan entry PE%ld E%ld %s", parentEntryId, fileInfo.entryid(), fileName.c_str());
                orphanEntries.push_back(std::make_pair(it->key().ToString(), it->key().ToString()));
            }
            LOG(INFO, "Check orphhan done, %lu entries", orphanEntries.size());
        }
    }

    delete it;
    return true;
}

//void NameSpace::initBlockIdUpbound(NameServerLog *log)
//{
//    std::string blockIdUpboundKey(8, 0);
//    blockIdUpboundKey.append("blockIdUpbound");
//    std::string blockIdUpboundVal;
//    leveldb::Status s = db -> Get(leveldb::ReadOptions(), blockIdUpboundKey, &blockIdUpboundVal);
//    if(s.IsNotFound())
//    {
//        LOG(INFO, "Init block id upbound");
//        updateBlockIdUpbound(log);
//    }
//    else if (s.ok())
//    {
//        blockIdUpBound = *(reinterpret_cast<int64_t*>(&blockIdUpboundVal[0]));
//        nextBlockId = blockIdUpBound;
//        LOG(INFO, "Load block id upbound: %ld", blockIdUpBound);
//        updateBlockIdUpbound(log);
//    }
//    else
//    {
//        LOG(FATAL, "Load block id upbound failed: %s", s.ToString().c_str());
//    }
//}
//
//void NameSpace::updateBlockIdUpbound(NameServerLog *log)
//{
//    std::string blockIdUpboundKey(8, 0);
//    blockIdUpboundKey.append("blockIdUpbound");
//    std::string blockIdUpboundVal;
//    blockIdUpboundVal.resize(8);
//    blockIdUpBound += FLAGS_blockIdAllocationSize; //??
//    *(reinterpret_cast<int64_t*>(&blockIdUpboundVal[0])) = blockIdUpBound;
//    leveldb::Status s = db -> Put(leveldb::WriteOptions(), blockIdUpboundKey, blockIdUpboundVal);
//    if(!s.ok())
//    {
//        LOG(FATAL, "Update block id upbound fail: %s", s.ToString().c_str());
//    }
//    else
//    {
//        LOG(INFO, "Update block id upbound to %ld", blockIdUpBound);
//    }
//    encodeLog(log, kSyncWrite, blockIdUpboundKey, blockIdUpboundVal);
//}
//
//
//
int64_t NameSpace::getVersion() const
{
    return version;
}

NameSpace::FileType NameSpace::getFileType(int type)
{
    int mode = (type >> 9);
    return static_cast<FileType>(mode);
}

bool NameSpace::getLinkSrcPath(const FileInfo &info, FileInfo *srcInfo)
{
    *srcInfo = info;
    FileType fileType = getFileType(info.type());

    while(fileType == kSymlink)
    {
        std::string symLink = srcInfo->symlink();
        std::string symPath = normalizePath(symLink);
        LOG(INFO, "GetLinkSrcPath %s", symPath.c_str());
        if(!lookUp(symPath, srcInfo))
        {
            LOG(INFO, "GetLinkSrcPath failed %s", symLink.c_str());
            return false;
        }
        fileType = getFileType(srcInfo->type());
    }

    if(getFileType(srcInfo->type()) == kDir)
    {
        return false;
    }
    return true;
}

void NameSpace::encodingStoreKey(int64_t entryId, const std::string &path, std::string *keyStr)
{
    keyStr->resize(8);
    baidu::common::util::EncodeBigEndian(&(*keyStr)[0], (uint64_t)entryId);
    keyStr->append(path);
}

void NameSpace::decodingStoreKey(const std::string &keyStr, int64_t *entryId, std::string *path)
{
    assert(keyStr.size() >= 8UL);
    if(entryId)
    {
        *entryId = baidu::common::util::DecodeBigEndian64(keyStr.c_str());
    }
    if(path)
    {
        path->assign(keyStr, 8, std::string::npos);
    }
}

bool NameSpace::getFromStore(const std::string &key, FileInfo *info)
{
    std::string val;
    leveldb::Status s = db->Get(leveldb::ReadOptions(), key, &val);
    if(!s.ok())
    {
        LOG(DEBUG, "getFromStore get failed %s %s", key.substr(8).c_str(), s.ToString().c_str());
        return false;
    }
    if(!info->ParseFromString(val))
    {
        LOG(WARNING, "getFromStore parse failed %s", key.substr(8).c_str());
        return false;
    }
    return true;
}

//StatusCode NameSpace::listDirectory(const std::string &path, google::protobuf::RepeatedPtrField<FileInfo> *outputs)
//{
//    outputs->Clear();
//    FileInfo fileInfo;
//    if（!lookUp(path, &fileInfo))
//    {
//        return kNsNotFound;
//    }
//
//    if(getFileType(fileInfo.type()) != kDir)
//    {
//        FileInfo *pFileInfo = outputs->Add();
//        pFileInfo->CopyFrom(fileInfo);
//        pFileInfo->clear_name();
//        LOG(INFO, "List %s return %ld items", path.c_str(), outputs->size());
//        return kOK;
//    }
//
//    int64_t entryId = fileInfo.entryId();
//    LOG(DEBUG, "listDirectory entryId = E%ld", entryId);
//    common::timer::AutoTimer autoTimer(100, "listDirectory iterate", path.c_str()); //析构时自动计算时间
//    std::string keyStart, keyEnd;
//    encodingStoreKey(entryId, "", &keyStart);//搜索同一父id下的文件
//    encodingStoreKey(entryId + 1, "", &keyEnd);
//    leveldb::Iterator *it = db->NewIterator(leveldb::ReadOptions());
//    for(it->Seek(keyStart); it->Valid(); it->Next())
//    {
//        leveldb::Slice key = it -> key();
//        if(key.compare(keyEnd) >= 0)
//        {
//            break;
//        }
//        FileInfo *pFileInfo = outputs -> Add();
//        bool ret = pFileInfo->ParseFromArray(it->value().data(), it->value().size());
//        assert(ret);
//        pFileInfo->set_name(std::string(key.data()+8, key.size()-8));//从第8位开始，即自己的name
//        LOG(DEBUG, "List %s return %s[%s]", path.c_str(), pFileInfo->name().c_str(), common::DebugString(key.ToString().c_str()));
//    }
//
//    LOG(INFO, "List return %ld items", outputs->size());
//    delete it;
//    return kOK;
//}
//
StatusCode NameSpace::buildPath(const std::string &path, FileInfo *fileInfo, std::string *fileName, NameServerLog* log = NULL)
{
    std::vector<std::string> paths;
    if(!baidu::common::util::SplitPath(path, &paths))
    {
        LOG(WARNING, "path split failed %s", path.c_str());
        return kBadParameter;
    }

    int64_t parentId = kRootEntryId;
    int depth = paths.size();
    fileInfo->set_entryid(parentId);
    std::string infoValue;
    for(int i = 0; i < depth-1; i++)
    {
        if(!lookUp(parentId, paths[i], fileInfo))
        {
            fileInfo->set_type((1<<9)|01755);//dir
            fileInfo->set_ctime(time(NULL));
            fileInfo->set_entryid(baidu::common::atomic_add64(&lastEntryId, 1) + 1);
            fileInfo->SerializeToString(&infoValue);
            std::string keyStr;
            encodingStoreKey(parentId, paths[i], &keyStr);
            leveldb::Status s = db->Put(leveldb::WriteOptions(), keyStr, infoValue);
            assert(s.ok());
            encodeLog(log, kSyncWrite, keyStr, infoValue);
            LOG(INFO, "Create path recursively: %s E%ld", paths[i].c_str(), fileInfo->entryid());
        }
        else
        {
            if(getFileType(fileInfo->type()) != kDir)
            {
                LOG(INFO, "Create path fail: %s is not a directory", paths[i].c_str());
                return kBadParameter;
            }
        }
        parentId = fileInfo->entryid();
    }
    *fileName = paths[depth - 1];
    return kOK;
}

StatusCode NameSpace::createFile(const std::string &filePathAndName, int flag, int mode, int replicaNum, std::vector<int64_t>* blocksToRemove, NameServerLog *log = NULL)
{
    if(filePathAndName == "/")
    {
        return kBadParameter;
    }
    FileInfo parentFileInfo, fileInfo;
    std::string fileName, infoValue;
    StatusCode status = buildPath(filePathAndName, &parentFileInfo, &fileName, log);
    if(status != kOK)
    {
        return status;
    }
    int64_t parentId = parentFileInfo.entryid();
    bool exist = lookUp(parentId, fileName, &fileInfo);

    if(exist)
    {
        if((flag & O_TRUNC) == 0)
        {
            LOG(INFO, "createFile %s failed: Already existed", fileName.c_str());
            return kFileExists;
        }
        else //将打开的文件清零,必须是普通文件
        {
            if(getFileType(fileInfo.type()) == kDir)
            {
                LOG(INFO, "createFile %s failed: directory with the same name existed", fileName.c_str());
                return kFileExists;
            }
            for(int i = 0; i < fileInfo.blocks_size(); i++)
            {
                blocksToRemove->push_back(fileInfo.blocks(i));
            }
        }
    }

    //set new created file info and SerializeToString
    fileInfo.set_type(((1 << 11) - 1) & mode); //11,111,111,111 & mode
    fileInfo.set_entryid(baidu::common::atomic_add64(&lastEntryId, 1) + 1);
    fileInfo.set_ctime(time(NULL));
    fileInfo.set_replicas(replicaNum<=0 ? FLAGS_defaultReplicaNum : replicaNum);
    fileInfo.SerializeToString(&infoValue);

    //use key and value to store in the leveldb
    std::string infoKey;
    encodingStoreKey(parentId, fileName, &infoKey);
    leveldb::Status s = db->Put(leveldb::WriteOptions(), infoKey, infoValue);
    if(s.ok())
    {
        LOG(INFO, "createFile %s E%ld ", filePathAndName.c_str(), fileInfo.entryid());
        return kOK;
    }
    else
    {
        LOG(WARNING, "createFile %s failed: db put failed %s", filePathAndName.c_str(), s.ToString().c_str());
        return kUpdateError;
    }
}
//
//bool NameSpace::deleteFileInfo(const std::string fileKey, NameServerLog* log = NULL)
//{
//    leveldb::Status s = db->Delete(leveldb::WriteOptions(), fileKey);
//    if(!s.ok())
//    {
//        return false;
//    }
//    encodeLog(log, kSyncDelete, fileKey, "");
//    return true;
//}
//
//StatusCode NameSpace::removeFile(const std::string &filePathAndName, FileInfo *fileRemoved, NameServerLog *log = NULL)
//{
//    StatusCode retStatus = kOK;
//    if(lookUp(filePathAndName, fileRemoved))
//    {
//        if(getFileType(fileRemoved->type()) != kDir)
//        {
//            if(filePathAndName == "/" || filePathAndName.empty())
//            {
//                LOG(INFO, "root type = %d", fileRemoved->type());
//            }
//
//            std::string fileKey;
//            encodingStoreKey(fileRemoved->parentEntryId(), fileRemoved->name(), &fileKey);
//            if(deleteFileInfo(fileKey, log))
//            {
//                LOG(INFO, "Unlink done: %s", filePathAndName.c_str());
//            }
//            else
//            {
//                LOG(WARNING, "Unlink failed: %s", filePathAndName.c_str());
//                retStatus = kUpdateError;
//            }
//        }
//        else
//        {
//            LOG(WARNING, "Unlink doesn't support directory: %s", filePathAndName.c_str());
//            retStatus = kBadParameter;
//        }
//    }
//    else
//    {
//        LOG(INFO, "Unlink is not found: %s", filePathAndName.c_str());
//        retStatus = kNsNotFound;
//    }
//    return retStatus;
//}
//
//StatusCode NameSpace::internalDeleteDirectory(const FileInfo &dirInfo, bool recursive, std::vector<FileInfo> *filesRemoved, NameServerLog *log)
//{
//    int64_t entryId = dirInfo.entryId();
//    std::string keyStart, keyEnd;
//    encodingStoreKey(entryId, "", &keyStart);
//    encodingStoreKey(entryId+1, "", &keyEnd);
//
//    leveldb::Iterator *it = db->NewIterator(leveldb::ReadOptions());
//    it->Seek(keyStart);
//    //有子文件但是不允许递归删除
//    if(it->Valid() && it->Key().compare(keyEnd) < 0 && recursive == false)
//    {
//        LOG(INFO, "The directory is not empty :%s", dirInfo.name().c_str());
//        delete it;
//        return kDirNotEmpty;
//    }
//
//    StatusCode retStatus = kOK;
//    leveldb::WriteBatch batch;
//    for(; it->Valid(); it->Next())
//    {
//        leveldb::Slice key = it->key();
//        if(key.compare(keyEnd) >= 0)
//        {
//            break;
//        }
//        std::string childName(key.data()+8, key.size()-8);
//        FileInfo childInfo;
//        bool ret = childInfo.ParseFromArray(it->value().data(), it->value().size());
//        assert(ret);
//        if(getFileType(childInfo.type()) == kDir)
//        {
//            childInfo.set_parentEntryId(entryId);
//            childInfo.set_name(childName);
//            LOG(INFO, "recursive to path %s", childName.c_str());
//            retStatus = internalDeleteDirectory(childInfo, true, filesRemoved, log);
//            if(retStatus != kOK)
//            {
//                break;
//            }
//        }
//        else
//        {
//            encodeLog(log, kSyncDelete, std::string(key.data(), key.size()), "");
//            batch.Delete(key);
//            childInfo.set_parentEntryId(entryId);
//            childInfo.set_name(childName);
//            LOG(DEBUG, "deleteDirectory remove push %s", childName.c_str());
//            filesRemoved->push_back(childInfo);
//            LOG(INFO, "Unlink file: %s", childName.c_str());
//        }
//    }
//    delete it;
//
//    std::string storeKey;
//    encodingStoreKey(dirInfo.parentEntryId(), dirInfo.name(), &storeKey);
//    batch.Delete(storeKey);
//    EncodeLog(log, kSyncDelete, storeKey, "");
//
//    leveldb::Status s = db->Write(leveldb::WriteOptions(), batch);
//    if(s.ok())
//    {
//        LOG(INFO, "deleteDirectory done: %s", dirInfo.name().c_str());
//    }
//    else
//    {
//        LOG(INFO, "Unlink directory failed: %s", dirInfo.name().c_str());
//        LOG(FATAL, "NameSpace write to storage failed");
//        retStatus = kUpdateError;
//    }
//    return retStatus;
//}
//
//
//StatusCode NameSpace::deleteDirectory(const std::string &path, bool recursive, std::vector<FileInfo> *fileRemoved, NameServerLog *log = NULL)
//{
//    fileRemoved->clear();
//    FileInfo fileInfo;
//    if(!lookUp(path, &fileInfo))
//    {
//        LOG(INFO, "deleteDirectory, %s is not found", path.c_str())
//        return kNsNotFound;
//    }
//    else if (getFileType(fileInfo.type()) != kDir)
//    {
//        LOG(WARNING, "deleteDirectory, %s is not a directory", path.c_str());
//        return kBadParameter;
//    }
//    return internalDeleteDirectory(fileInfo, recursive, fileRemoved, log);
//}
//
//
//StatusCode NameSpace::internalComputeDiskUsage(const FileInfo &info, uint64_t *diskUsageSize)
//{
//    int64_t entryId = info.entryId();
//    std::string keyStart, keyEnd;
//    encodingStoreKey(entryId, "", &keyStart);
//    encodingStoreKey(entryId + 1, "", &keyEnd);
//    leveldb::Iterator *it = db->NewIterator(leveldb::ReadOptions());
//    it->Seek(keyStart);
//
//    StatusCode retStatus = kOK;
//    for(; it->Valid(); it->Next())
//    {
//        leveldb::Slice key = it->key();
//        if(key.compare(keyEnd) >= 0)
//        {
//            break;
//        }
//        std::string childName(key.data()+8, key.size()-8);
//        FileInfo childInfo;
//        assert(childInfo.ParseFromArray(it->value().data(), it->value().size()));
//        if(getFileType(childInfo.type()) == kDir)
//        {
//            childInfo.set_parentEntryId(entryId);
//            retStatus = internalComputeDiskUsage(childInfo, diskUsageSize);
//            if(retStatus != kOK)
//            {
//                break;
//            }
//        }
//        else
//        {
//            diskUsageSize += childInfo.size();
//        }
//    }
//
//    delete it;
//    return retStatus;
//}
//
//
//StatusCode NameSpace::diskUsage(const std::string &path, uint64_t *diskUseageSize)
//{
//    if(diskUseageSize == NULL)
//    {
//        return kBadParameter;
//    }
//
//    *diskUseageSize = 0;
//    FileInfo fileInfo;
//    if(!lookUp(path, &fileInfo))
//    {
//        LOG(INFO, "diskUsage file or directory not found : %s", path.c_str());
//        return kNsNotFound;
//    }
//    else if(getFileType(fileInfo.type()) != kDir)
//    {
//        *diskUseageSize = fileInfo.size();
//        return kOK;
//    }
//    return internalComputeDiskUsage(fileInfo, diskUseageSize);
//}
//
//StatusCode NameSpace::rename(const std::string &oldPath, const std::string &newPath, bool *needUnlink, FileInfo *removedFile, NameServerLog *log = NULL)
//{
//    *needUnlink = false;
//    if(oldPath.empty() || newPath.empty() || oldPath == "/" || newPath == "/" || oldPath = newPath)
//    {
//        return kBadParameter;
//    }
//
//    FileInfo oldFileInfo;
//    if(!lookUp(oldPath, &oldFileInfo))
//    {
//        LOG(INFO, "Reanme old file not found: %s", oldPath.c_str());
//        return kNsNotFound;
//    }
//
//    std::vector<std::string> newPaths;
//    if(!common::util::SplitPath(newPath, newPaths))
//    {
//        LOG(INFO, "createFile split failed: %s", newPath.c_str());
//        return kBadParameter;
//    }
//
//    int64_t parentId = kRootEntryId;
//    for(uint32_t i = 0; i < newPaths.size()-1; i++)
//    {
//        FileInfo tempInfo;
//        if(!lookUp(parentId, newPaths[i], &tempInfo))
//        {
//            LOG(INFO, "Rename to %s is not exists", newPaths[i].c_str());
//            return kNsNotFound;
//        }
//        if(getFileType(tempInfo.type()) != kDir)
//        {
//            LOG(INFO, "Rename to %s is not a directory", newPath[i].c_str());
//            return kBadParameter;
//        }
//        if(tempInfo.entryId() == oldFileInfo.entryId())
//        {
//            LOG(INFO, "Rename to %s failed because %s is the parent directory of %s", newPath[i].c_str(), oldPath.c_str(), newPath.c_str());
//            return kBadParameter;
//        }
//        parentId = tempInfo.entryId();
//    }
//
//    const std::string &dstName = newPaths[newPaths.size()-1];
//    FileInfo dstFileInfo;
//    if(lookUp(parentId, dstName, &dstFileInfo))
//    {
//        if(getFileType(dstFileInfo.type()) == kDir || getFileType(oldFileInfo.type()) == kDir)
//        {
//            LOG(INFO, "Rename %s to %s failed: source or dist is not a file", oldPath.c_str(), newPath.c_str());
//            return kBadParameter;
//        }
//        if(getFileType(dstFileInfo.type()) != kSymlink)
//        {
//            *needUnlink = true;
//            removedFile->CopyFrom(dstFileInfo);
//            removedFile->set_name(dstName);
//        }
//    }
//
//    std::string oldKey, newKey;
//    encodingStorekey(oldFileInfo.parentEntryId(), oldFileInfo.name(), &oldKey);
//    encodingStoreKey(parentId, dstName, &newKey);
//
//    std::string value;
//    oldFileInfo.clear_parentEntryId();
//    oldFileInfo.clear_name();
//    oldFileInfo.SerializeToString(&value);
//
//    leveldb::WriteBatch batch;
//    batch.Put(newKey, value);
//    batch.Delete(oldKey);
//
//    //encodelog
//
//    leveldb::Status s = db->Write(leveldb::WriteOptions(), &batch);
//    if(s.ok())
//    {
//        LOG(INFO, "Rename %s to %s succeed", oldPah.c_str(), newPath.c_str());
//        return  kOK;
//    }
//    else
//    {
//        LOG(WARNING, "Rename %s to %s failed", oldPah.c_str(), newPath.c_str());
//        return kUpdateError;
//    }
//
//}

	bool NameSpace::lookUp(const std::string &path, FileInfo *info)
	{
		if(path == "/")
		{
			info->CopyFrom(rootPath);
			return true;
		}
		std::vector<std::string> paths;
		if(!baidu::common::util::SplitPath(path, &paths) || path.empty())
		{
			return false;
		}

		int64_t parentId = kRootEntryId;
		int64_t entryId = kRootEntryId;

		for(size_t i = 0; i < paths.size(); i++)
		{
			if(!lookUp(entryId, paths[i], info))
			{
				return false;
			}
			parentId = entryId;
			entryId = info->entryid();
			LOG(DEBUG, "LookUp %s entry_id= E%ld ", paths[i].c_str(), entryId);
		}

		info->set_name(paths[paths.size()-1]);
		info->set_parententryid(parentId);
		LOG(INFO, "LookUp %s return %s", path.c_str(), info->name().c_str());
		return true;
	}

	bool NameSpace::lookUp(int64_t pid, const std::string &name, FileInfo *info)
	{
		std::string keyStr;
		encodingStoreKey(pid, name, &keyStr);
		if(!getFromStore(keyStr, info))
		{
			LOG(INFO, "LookUp E%ld %s return false", pid, name.c_str());
			return false;
		}
		LOG(DEBUG, "LookUp E%ld %s return true", pid, name.c_str());
		return true;
	}

	std::string NameSpace::normalizePath(const std::string &path)
	{
		std::string res;
		if(path.empty() || path[0] == '/')
		{
			res = "/";
		}
		bool slash = false;
		for(int i = 0; i < path.size(); i++)
		{
			if(path[i] == '/')
			{
				if(slash) continue;
				slash = true;
			}
			else slash = false;
			res.push_back(path[i]);
		}

		if(res.size() > 1 && res[res.size() - 1] == '/')
		{
			res.resize(res.size()-1);
		}
		return res;
	}

}