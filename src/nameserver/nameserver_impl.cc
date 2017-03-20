// Auther: zackliu1995@hotmail.com

#include "nameserver_impl.h"

#include <set>
#include <map>
#include <sstream>
#include <cstdlib>

#include <gflags/gflags.h>
#include <sofa/pbrpc/pbrpc.h>

#include <common/counter.h>
#include <common/logging.h>
#include <common/string_util.h>

#include "nameserver/block_mapping_manager.h"
#include "nameserver/chunkserver_manager.h"
#include "nameserver/namespace.h"

#include "proto/status_code.pb.h"

DECLARE_bool(bfsWebKickEnable);
DECLARE_int32(nameserverStartRecoverTimeout);
DECLARE_int32(chunkserverMaxPendingBuffers);
DECLARE_int32(nameserverReportThreadNum);
DECLARE_int32(nameserverWorkThreadNum);
DECLARE_int32(nameserverReadThreadNum);
DECLARE_int32(nameserverHeartbeatThreadNum);
DECLARE_int32(blockmappingBucketNum);
DECLARE_int32(hiRecoverTimeout);
DECLARE_int32(loRecoverTimeout);
DECLARE_int32(blockReportTimeout);
DECLARE_bool(cleanRedundancy);


namespace zfs
{

baidu::common::Counter gGetLocation;
baidu::common::Counter gAddBlock;
baidu::common::Counter gHeartBeat;
baidu::common::Counter gBlockReport;
baidu::common::Counter gUnlink;
baidu::common::Counter gCreateFile;
baidu::common::Counter gListDir;
baidu::common::Counter gReportBlocks;
extern baidu::common::Counter gBlocksNum;

NameServerImpl::NameServerImpl(): 

}