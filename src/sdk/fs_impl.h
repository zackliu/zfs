//
// Created by zackliu on 3/29/17.
//

#ifndef CLOUDSTORAGE_FS_IMPL_H
#define CLOUDSTORAGE_FS_IMPL_H

#include <vector>
#include <string>

#include <common/thread_pool.h>

#include "zfs.h"
#include "../proto/status_code.pb.h"

namespace zfs
{
	class FileSystemImpl : public FileSystem
	{
	public:
		FileSystemImpl();
		~FileSystemImpl();

		int32_t openFile(const char *path, int32_t flags, File **file,
		                 const ReadOptions &readOptions);
		int32_t openFile(const char *path, int32_t flags, File **file,
		                 const WriteOptions &writeOptions);
		int32_t openFile(const char *path, int32_t flags, int32_t mode, File **file,
		                 const WriteOptions &writeOptions);
		int32_t closeFile(File *file);

	private:
		std::vector<std::string> _nameServerAddress;
		std::string _localhostName;
		baidu::ThreadPool *_threadPool;
	};
}


#endif //CLOUDSTORAGE_FS_IMPL_H
