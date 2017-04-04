//
// Created by zackliu on 4/4/17.
//

#ifndef CLOUDSTORAGE_FILE_IMPL_WRAPPER_H
#define CLOUDSTORAGE_FILE_IMPL_WRAPPER_H

#include "zfs.h"

#include <memory>

namespace zfs
{
	class FileImpl;
	class FileSystemImpl;
	class Rpc;


	class FileImplWrapper: public File
	{
	public:
		FileImplWrapper(FileSystemImpl *fs, Rpc *rpcClient, const std::string &name, int32_t flags, const WriteOptions &options);
		FileImplWrapper(FileSystemImpl *fs, Rpc *rpcClient, const std::string &name, int32_t flags, const ReadOptions &options);
		FileImplWrapper(FileImpl *fileImpl);

		virtual ~FileImplWrapper();
		virtual int32_t pread(char *buf, int32_t readSize, int64_t offset, bool readAhead = false);
		virtual int64_t seek(int64_t offset, int32_t whence);
		virtual int32_t read(char *buf, int32_t readSize);
		virtual int32_t write(const char *buf, int32_t writeSize);
		virtual int32_t close();

	private:
		std::shared_ptr<FileImpl> _impl;
		FileImplWrapper(const FileImplWrapper&);
		void operator=(const FileImplWrapper&);
	};
}


#endif //CLOUDSTORAGE_FILE_IMPL_WRAPPER_H
