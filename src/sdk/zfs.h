//
// Created by zackliu on 3/28/17.
//

#ifndef CLOUDSTORAGE_ZFS_H
#define CLOUDSTORAGE_ZFS_H

#include <stdint.h>
#include <fcntl.h>
#include <string>
#include <map>
#include <vector>

namespace zfs
{
	const int OK = 0;
	const int BAD_PARAMETER = -1;
	const int PERMISSION_DENIED = -2;
	const int NOT_ENOUGH_QUOTA = -3;
	const int NETWORK_UNAVAILABLE = -4;
	const int TIMEOUT = -5;
	const int NOT_ENOUGH_SPACE = -6;
	const int OVERLOAD = -7;
	const int META_NOT_AVAILABLE = -8;
	const int UNKNOWN_ERROR = -9;

	const char *strError(int errorCode);

	enum WriteMode
	{
		kWriteDefault;
		kWriteChains;
		kWriteFanout;
	};

	struct WriteOptions
	{
		int flushTimeout;
		int syncTimeout;
		int closeTimeout;
		int replica;
		WriteMode writeMode;
		bool syncOnClose;
		WriteOptions(): flushTimeout(-1), syncTimeout(-1), closeTimeout(-1),
	                    replica(-1), writeMode(kWriteDefault),
	                    syncOnClose(false) {}
	};

	struct ReadOptions
	{
		int timeout;
		ReadOptions(): timeout(-1);
	};

	struct FSOptions
	{
		const char *username;
		const char *password;
		FSOptions(): username(NULL), password(NULL) {}
	};

	class File
	{
	public:
		File() {}
		virtual ~File() {}
		virtual int32_t pread(char *buf, int32_t readSize, int64_t offset, bool reada = false) = 0;
		virtual int64_t seek(int64_t offset, int32_t whence) = 0;
		virtual int32_t read(char* buf, int32_t readSize) = 0;
		virtual int32_t write(const char* buf, int32_t writeSize) = 0;
		virtual int32_t flush() = 0;
		virtual int32_t sync() = 0;
		virtual int32_t close() = 0;

	private:
		File(const File&);
		void operator=(const File&);
	};

	struct ZfsFileInfo
	{
		int64_t size;
		uint32_t  ctime;
		uint32_t  mode;
		char name[1024];
	};

	class FileSystem
	{
	public:
		FileSystem() {}
		virtual ~FileSystem() {}

		static bool openFileSystem(const char *nameserver, FileSystem **fs, const FSOptions&);
		virtual int32_t openFile(const char *path, int32_t flags, File **file,
								 const ReadOptions &readOptions) = 0;
		virtual int32_t openFile(const char *path, int32_t flags, File **file,
		                         const WriteOptions &writeOptions) = 0;
		virtual int32_t openFile(const char *path, int32_t flags, int32_t mode, File **file,
		                         const WriteOptions &writeOptions) = 0;
		virtual int32_t closeFile(File *file) = 0;

	private:
		FileSystem(const FileSystem&);
		void operator=(const FileSystem&);
	};

}

#endif //CLOUDSTORAGE_ZFS_H
