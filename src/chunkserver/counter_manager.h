//
// Created by zackliu on 4/4/17.
//

#ifndef CLOUDSTORAGE_COUNTER_MANAGER_H
#define CLOUDSTORAGE_COUNTER_MANAGER_H

#include <stdint.h>
#include <string.h>
#include <string>

#include <common/mutex.h>
#include <common/counter.h>
#include <common/string_util.h>

namespace zfs
{
	class ChunkserverCounterManager
	{
	public:
		struct ChunkserverStat {
			int64_t block_buffers;
			int64_t buffers_new;
			int64_t buffers_delete;
			int64_t unfinished_bytes;
			int64_t find_ops;
			int64_t read_ops;
			int64_t read_bytes;
			int64_t write_ops;
			int64_t recover_count;
			int64_t recover_bytes;
			int64_t refuse_ops;
			int64_t rpc_delay;
			int64_t rpc_delay_all;
			int64_t rpc_count;
			ChunkserverStat() :
					block_buffers(0),
					buffers_new(0),
					buffers_delete(0),
					unfinished_bytes(0),
					find_ops(0),
					read_ops(0),
					read_bytes(0),
					write_ops(0),
					recover_count(0),
					recover_bytes(0),
					refuse_ops(0),
					rpc_delay(0),
					rpc_delay_all(0),
					rpc_count(0) {}
		};

		ChunkserverCounterManager();

	};


	class DiskCounterManager
	{
	public:

	};
}

#endif //CLOUDSTORAGE_COUNTER_MANAGER_H
