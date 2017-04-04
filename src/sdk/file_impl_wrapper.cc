//
// Created by zackliu on 4/4/17.
//

#include "file_impl_wrapper.h"
#include "file_impl.h"

namespace zfs
{
	FileImplWrapper::FileImplWrapper(FileSystemImpl *fs, Rpc *rpcClient, const std::string &name, int32_t flags,
	                                 const WriteOptions &options)
		:_impl(new FileImpl(fs, rpcClient, name, flags, options)) {}

	FileImplWrapper::FileImplWrapper(FileSystemImpl *fs, Rpc *rpcClient, const std::string &name, int32_t flags,
	                                 const ReadOptions &options)
		:_impl(new FileImpl(fs, rpcClient, name, flags, options)){}

	FileImplWrapper::FileImplWrapper(FileImpl *fileImpl)
		:_impl(fileImpl){}

	FileImplWrapper::~FileImplWrapper()
	{
		_impl->close();
	}

	int32_t FileImplWrapper::pread(char *buf, int32_t readSize, int64_t offset, bool readAhead)
	{
		return _impl->pread(buf, readSize, offset, readAhead);
	}

	int32_t FileImplWrapper::read(char *buf, int32_t readSize)
	{
		return _impl->read(buf, readSize);
	}

	int32_t FileImplWrapper::write(const char *buf, int32_t writeSize)
	{
		return _impl->write(buf, writeSize);
	}

	int32_t FileImplWrapper::close()
	{
		return _impl->close();
	}
}