//
// Created by zackliu on 4/4/17.
//

#ifndef CLOUDSTORAGE_DATA_BLOCK_H
#define CLOUDSTORAGE_DATA_BLOCK_H

#include <string>
#include <vector>

#include <common/mutex.h>
#include <common/thread_pool.h>
#include <common/sliding_window.h>

#include "../proto/status_code.pb.h"
#include "../proto/block.pb.h"

namespace zfs
{
	struct Buffer
	{
		const char *_data;
		int32_t _len;
		Buffer(const char *buff, int32_t len)
				:_data(buff), _len(len) {}
		Buffer()
				:_data(NULL), _len(0) {}
		Buffer(const Buffer &o)
				:_data(o._data), _len(o._len){}
	};

	class FileCache;
	class Disk;

	class Block
	{
	public:
		Block(const BlockMeta &meta, Disk *disk, FileCache *fileCache);
		~Block();

		void addRef();
		void decRef();
		int getRef() const;
		int64_t read(char *buf, int64_t len, int64_t offset);
		bool write(int32_t seq, int64_t offset, const char *data, int64_t len, int64_t *addUse = NULL);

		void setRecover();
		bool isRecover() const;

		void setVersion(int64_t version);
		int getVersion() const;

	private:

	private:
		enum Type
		{
			InDisk,
			InMen,
		};

		enum fdStatus
		{
			kNotCreated = -1;
			kClosed = -2;
		};


	};
}


#endif //CLOUDSTORAGE_DATA_BLOCK_H
