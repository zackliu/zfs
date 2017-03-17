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
    std::set<int32_t> incomplete_replica;
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
    
}



}



#endif