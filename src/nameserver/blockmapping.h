// Auther: zackliu1995@hotmail.com

#ifndef ZFS_BLOCKMAPPING_H
#define ZFS_BLOCKMAPPING_H

#include <set>
#include <map>
#include <queue>
#include <string>

#include <gflags/gflags.h>

#include <common/mutex.h>
#include <common/thread_pool.h>
#include <proto/status_code.ph.h>
#include <proto/nameserver.pb.h>

namespace zfs
{

struct NSBlock
{
    int64_t id;
    int64_t version;
    std::set<int32_t> replica;
    int64_t blockSize;
    uint32_t expectReplicaNum;
    RecoverStat recoverStatus;
    std::set<int32_t> incompleteReplica;
    NSBlock();
    NSBlock(int64_t blockId, int32_t replica, int64_t version, int64_t size);
    bool operator< (const NSBlock &b) const
    {
        return (this->replica.size() >= b.replica.size());
    }
};

struct RecoverBlockNum
{
    int64_t loRecoverNum;
    int64_t hiRecoverNum;
    int64_t loPending;
    int64_t hiPending;
    int64_t lostNum;
    int64_t incompleteNum;
    RecoverBlockNum() : loRecoverNum(0), hiRecoverNum(0), loPending(0), hiPending(0), 
                        lostNum(0), incompleteNum(0) {}
};

struct RecoverBlockSet
{
    std::set<int64_t> hiRecover;
    std::set<int64_t> loRecover;
    std::set<int64_t> lost;
    std::map<int32_t, std::set<int64_t> > hiCheck;
    std::map<int32_t, std::set<int64_t> > loCheck;
    std::map<int32_t, std::set<int64_t> > incomplete;
};

class BlockMapping
{
public:
    BlockMapping(ThreadPool *threadPool);
    bool getBlock(int64_t blockId, NSBlock *block);
    bool getLocatedBlock(int64_t blockId, std::vector<int32_t> *replica, int64_t *blockSize, 
                         RecoverStat *status);
    bool changeReplicaNum(int64_t blockId, int32_t replicaNum);
    void addBlock(int64_t blockId, int32_t replica, const std::vector<int32_t> &initReplicas);
    void rebuildBlock(int64_t blockId, int32_t replica, int64_t version, int64_t size);
    bool updateBlockInfo(int64_t blockId, int32_t serverId, int64_t blockSize,
                         int64_t blockVersion);
    void removeBlocksForFile(const FileInfo &fileInfo, std::map<int64_t, std::set<int32_t> > *blocks);
    void removeBlock(int64_t blockId, std::map<int64_t, std::set<int32_t> > *blocks);
    void dealWithDeadNode(int32_t csId, const std::set<int64_t> &blocks);
    void dealWithDeadBlock(int32_t csId, int64_t blockId);
    StatusCode checkBlockVersion(int64_t blockId, int64_t version);
    void pickRecoverBlocks(int32_t csId, int32_t blockNum,
                           std::vector<std::pair<int64_t, std::set<int32_t> > >* recoverBlocks,
                           RecoverPri pri);
    void processRecoveredBlock(int32_t csId, int64_t blockId, StatusCode status);
    void getCloseBlocks(int32_t csId, google::protobuf::RepeatedField<int64_t>* closeBlocks);
    void getStat(int32_t csId, RecoverBlockNum  *recoverNum);
    void listRecover(RecoverBlockSet* blocks);
    int32_t getCheckNum();
    void markIncomplete(int64_t blockId);
private:
    void dealWithDeadBlockInternal(int32_t csId, int64_t blockId);
    typedef std::map<int32_t, std::set<int64_t> > CheckList;
    void listCheckList(const CheckList& checkList, std::map<int32_t, std::set<int64_t> > *result);
    void listRecoverList(const std::set<int64_t> &recoverSet, std::set<int64_t> *result);
    void tryRecover(NSBlock *block);
    bool removeFromRecoverCheckList(int32_t csId, int64_t blockId);
    void checkRecover(int32_t csId, int64_t blockId);
    void insertToIncomplete(int64_t blockId, const std::set<int32_t> &incReplica);
    void removeFromIncomplete(int64_t blockId, int32_t csId);
    bool getBlockPtr(int64_t blockId, NSBlock **block);
    void setState(NSBlock *block, RecoverStat stat);
    bool setStateIf(NSBlock *block, RecoverStat from, RecoverStat to);

    bool updateWritingBlock(NSBlock* nsblock, int32_t csId, int64_t blockSize,
                            int64_t blockVersion);
    bool updateNormalBlock(NSBlock* nsblock, int32_t csId, int64_t blockSize,
                           int64_t blockVersion);
    bool updateIncompleteBlock(NSBlock* nsblock,int32_t csId, int64_t blockSize,
                               int64_t blockVersion);
private:
    Mutex mu;
    ThreadPool* threadPool;
    typedef std::map<int64_t, NSBlock*> NSBlockMap;
    NSBlockMap blockMap;

    CheckList hiRecoverCheck;
    CheckList loRecoverCheck;
    CheckList incomplete;
    std::set<int64_t> loPriRecover;
    std::set<int64_t> hiPriRecover;
    std::set<int64_t> lostBlocks;                               
};



}



#endif