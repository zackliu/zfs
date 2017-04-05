//
// Created by zackliu on 4/4/17.
//

#include "data_block.h"

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <functional>

#include <gflags/gflags.h>
#include <common/counter.h>
#include <common/thread_pool.h>
#include <common/sliding_window.h>

#include <common/logging.h>

#include "file_cache.h"
#include "disk.h"

DECLARE_int32(write_buf_size);

namespace zfs
{
	extern baidu::common::Counter g_block_buffers;
	extern baidu::common::Counter g_buffers_new;
	extern baidu::common::Counter g_buffers_delete;


}