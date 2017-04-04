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


	class DiskCounterManager {
	public:
		struct DiskCounters {
			// block number
			baidu::common::Counter blocks;
			// size of buf written a period of time (stat)
			baidu::common::Counter write_bytes;
			// number of blocks are being writing
			baidu::common::Counter writing_blocks;
			// size of buf writing to blocks
			// Inc when a buf is created (in Block::Write)
			// Dec when the buf is popped out by sliding window
			baidu::common::Counter writing_bytes;
			// data size
			baidu::common::Counter data_size;
			// number of buf in waiting list (block_buf_list_), equivalent to block_buf_list_.size()
			// Inc when adding a buf to block_buf_list_
			// Dec when a buf is erased from block_buf_list_
			baidu::common::Counter pending_buf;
			baidu::common::Counter mem_read_ops;
			baidu::common::Counter disk_read_ops;
		};
		struct DiskStat {
			int64_t blocks;
			int64_t write_bytes;
			int64_t writing_blocks;
			int64_t writing_bytes;
			int64_t data_size;
			int64_t pending_buf;
			int64_t mem_read_ops;
			int64_t disk_read_ops;
			double load;
			DiskStat() :
					blocks(0),
					write_bytes(0),
					writing_blocks(0),
					writing_bytes(0),
					data_size(0),
					pending_buf(0),
					mem_read_ops(0),
					disk_read_ops(0),
					load(0.0) {}
			void ToString(std::string* str) {
				str->append(" blocks=" + baidu::common::NumToString(blocks));
				str->append(" w_bytes=" + baidu::common::HumanReadableString(write_bytes));
				str->append(" wing_blocks=" + baidu::common::HumanReadableString(writing_blocks));
				str->append(" wing_bytes=" + baidu::common::HumanReadableString(writing_bytes));
				str->append(" size=" + baidu::common::HumanReadableString(data_size));
				str->append(" pending_w=" + baidu::common::HumanReadableString(pending_buf));
				str->append(" mem_read_ops=" + baidu::common::NumToString(mem_read_ops));
				str->append(" disk_read_ops=" + baidu::common::NumToString(disk_read_ops));
			}
			bool operator<(const DiskStat& s) const {
				return s.load < load;
			}

		};
	public:
		DiskCounterManager();
		void GatherCounters(DiskCounters* counters);
		DiskStat GetStat();
	private:
		baidu::Mutex mu_;
		DiskStat stat_;
		int64_t last_gather_time_;
}

#endif //CLOUDSTORAGE_COUNTER_MANAGER_H
