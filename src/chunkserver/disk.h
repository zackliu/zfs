//
// Created by zackliu on 4/4/17.
//

#ifndef CLOUDSTORAGE_DISK_H
#define CLOUDSTORAGE_DISK_H

#include <stdint.h>
#include <map>
#include <string>
#include <vector>

#include <common/thread_pool.h>

#include "../proto/status_code.pb.h"
#include "counter_manager.h"

namespace zfs
{
	class BlockMeta;
	class Block;
	class FileCache;

	class Disk
	{
	public:
		Disk(const std::string &path, int64_t quota);
		~Disk();
	};
}



#endif //CLOUDSTORAGE_DISK_H
