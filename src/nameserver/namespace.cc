// Auther: zackliu1995@hotmail.com

#include "namespace.h"

#include <fcntl.h>
#include <gflags/gflags.h>
#include <leveldb/db.h>
#include <leveldb/cache.h>
#include <leveldb/logging.h>
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

const int64_t kRootEntryId = 1;

namespace zfs
{
NameSpace::NameSpace():version(0), lastEntryId(1), blockIdUpBound(1), nextBlockId(1)
{
    leveldb::Options options;
    options.create_if_missing = true;
    options.block_cache = leveldb::NewLRUCache(FLAGS_namedbCacheSize * 1024L);
    leveldb::Status s = leveldb::DB::Open(options, FLAGS_namedbPath, &db);
    if(!s.ok())
    {
        db = NULL;
        LOG(ERROR, "Open leveldb fails: %s", s.ToString().c_str());
        exit(1);
    }
    
    activateDb(NULL, NULL);
}

void NameSpace::activateDb(std::function<void (const FileInfo&)> rebuild_callback, NameServerLog *log)
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
        version = common::timer::get_micros();
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
    initBlockIdUpBound(log);
}

void NameSpace::setupRoot()
{
    rootPath.set_entryId(kRootEntryId);
    rootPath.set_name("");
    rootPath.set_parentEntryId(kRootEntryId);
    rootPath.set_type(01755);//八进制
    rootPath.set_ctime(static_cast<uint32_t>(version / 1000000));

}

bool NameSpace::rebuildBlockMap(std::function<void (const FileInfo&)> callback)
{
    int64_t blockNum = 0;
    int64_t fileNum = 0;
    int64_t linkNum = 0;
    std::set<int64_t> entryIdSet;
    entryIdSet.insert(rootPath.entryId());
    leveldb::Iterator *it = db -> NewIterator(leveldb::ReadOptions());
    for(it->Seek(std::string(7, '\0') + '\1'); it -> Valid(); it -> Next()) //从fatherEntryId=00000001开始找
    {
        FileInfo fileInfo;
        bool ret = fileInfo.ParseFromArray(it->value().data(), it->value().size());
        assert(ret);

        if(lastEntryId < fileInfo.entryId())
        {
            lastEntryId = fileInfo.entryId();
        }

        FileType fileType = getFileType(fileInfo.type());
        if(fileType == kDefault)
        {
            for(int i = 0; i < fileInfo.blocks_size(); i++)
            {
                if(fileInfo.blocks(i) >= nextBlockId)
                {
                    nextBlockId = fileInfo.block(i)+1;
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
            entryIdSet.insert(fileInfo.entryId());
        }
    }

    LOG(INFO, "rebuildBlockMap done. %ld directories, %ld symlinks, %ld files, %lu blocks, lastEntryId = E%ld", 
        entryIdSet.size(), linkNum, fileNum, blockNum, lastEntryId);
    
    if(FLAGS_checkOrphan)
    {
        std::vector<std::pair<std::string, std::string> > orphanEntries;
        for(it -> Seek(std::string(7, '\0') + '\1'); it -> Valid(); it -> Next())
        {
            FileInfo fileInfo;
            bool ret = fileInfo.ParseFromArray(it->value().data(), it->value().size());
            assert(ret);
            int64_t parentEntryId = 0;
            std::string fileName;
            decodingStoreKey(it->key().ToString(), &parentEntryId, &fileName);
            if(entryIdSet.find(parentEntryId) == entryIdSet.end())
            {
                LOG(WARNING, "Orphan entry PE%ld E%ld %s", parentEntryId, fileInfo.entryId(), fileName.c_str());
                orphanEntries.push_back(std::make_pair(it->key().ToString(), it->key().ToString()));
            }
            LOG(INFO, "Check orphhan done, %lu entries", orphanEntries.size());
        } 
    }

    delete it;
    return true;
}

void NameSpace::initBlockIdUpbound(NameServerLog *log)
{
    std::string blockIdUpboundKey(8, 0);
    blockIdUpboundKey.append("blockIdUpbound");
    std::string blockIdUpboundVal;
    leveldb::Status s = db -> Get(leveldb::ReadOptions(), blockIdUpboundKey, &blockIdUpboundVal);
    if(s.IsNotFound())
    {
        LOG(INFO, "Init block id upbound");
        updateBlockIdUpbound(log);
    }
    else if (s.ok())
    {
        blockIdUpBound = *(reinterpret_cast<int64_t*>(&blockIdUpboundVal[0]));
        nextBlockId = blockIdUpBound;
        LOG(INFO, "Load block id upbound: %ld", blockIdUpBound);
        updateBlockIdUpbound(log);
    }
    else 
    {
        LOG(FATAL, "Load block id upbound failed: %s", s.ToString().c_str());
    }
}

void NameSpace::updateBlockIdUpbound(NameServerLog *log)
{
    std::string blockIdUpboundKey(8, 0);
    blockIdUpboundKey.append("blockIdUpbound");
    std::string blockIdUpboundVal;
    blockIdUpboundVal.resize(8);
    blockIdUpBound += FLAGS_blockIdAllocationSize; //??
    *(reinterpret_cast<int64_t*>(&blockIdUpboundVal[0])) = blockIdUpBound;
    leveldb::Status s = db -> Put(leveldb::WriteOptions(), blockIdUpboundKey, blockIdUpboundVal);
    if(!s.ok())
    {
        LOG(FATAL, "Update block id upbound fail: %s", s.ToString().c_str());
    }
    else 
    {
        LOG(INFO, "Update block id upbound to %ld", blockIdUpBound);
    }
    encodeLog(log, kSyncWrite, blockIdUpboundKey, blockIdUpboundVal);
}

NameSpace::~NameSpace()
{
    delete db;
    db = NULL;
}

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

    while(fileTyep == kSymlink)
    {
        std::string symLink = srcInfo->symLink();
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
    keyStr.resize(8);
    common::util::EncodeBigEndian(&(*keyStr)[0], (uint64_t)entryId);
    keyStr->append(path);
}

void NameSpace::decodingStoreKey(const std::string &keyStr, int64_t *entryId, std::string *path)
{
    assert(keyStr.size() >= 8UL);
    if(entryId)
    {
        *entryId = common::util::DecodeBigEndian64(keyStr.c_str());
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




}