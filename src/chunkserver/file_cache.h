//
// Created by zackliu on 4/4/17.
//

#ifndef CLOUDSTORAGE_FILE_CACHE_H
#define CLOUDSTORAGE_FILE_CACHE_H

#include <common/cache.h>

namespace zfs
{
	class FileCache
	{
	public:
		FileCache(int32_t cacheSize);
		~FileCache();

	private:
		baidu::common::Cache *_cache;
	};
}

#endif //CLOUDSTORAGE_FILE_CACHE_H
