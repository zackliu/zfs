//
// Created by zackliu on 3/29/17.
//

#include <gflags/gflags.h>

#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <string>
#include <cstring>
#include <unistd.h>
#include <stdint.h>
#include <sys/stat.h>
#include <map>

#include <common/string_util.h>
#include <common/timer.h>
#include <common/util.h>

#include "../sdk/zfs.h"

DECLARE_string(flagfile);
DECLARE_string(nameserver_nodes);

void printUsage()
{
	printf("Usage:\nclient <commond> path\n");
	printf("\ttouch <path>...: create a new file\n");
	printf("\tput <local_path> <zfs_path>...: copy file from local to bfs\n");
	printf("\tget <zfs_path> <local_path>...: copy file to local\n");
}

int zfsTouch(zfs::FileSystem *fs, int argc, char *argv[])
{
	if(argc < 1)
	{
		printUsage();
		return 1;
	}

	int res = 0;
	zfs::File *file;
	for(int i = 0; i < argc; i++)
	{
		res = fs->openFile(argv[i], O_WRONLY, 0644, &file, zfs::WriteOptions());
		if(res != 0)
		{
			fprintf(stderr, "Open file error: %s\n", argv[i]);
		}
	}
	return 0;
}

int zfsGet(zfs::FileSystem *fs, int argc, char *argv[])
{
	if(argc < 1)
	{
		printUsage();
		return 1;
	}

	std::string source = argv[0], target;
	if(argc >= 2)
	{
		target = argv[1];
	}

	std::vector<std::string> sourcePaths;
	bool sourceIsDir = false;
	if(!baidu::common::util::SplitPath(source, &sourcePaths, &sourceIsDir)
		|| sourceIsDir || sourcePaths.empty())
	{
		fprintf(stderr, "Bad file path: %s\n", source.c_str());
		return 1;
	}

	std::string sourceFileName = sourcePaths[sourcePaths.size()-1];
	if(target.empty() || target[target.size()-1] == '/')
	{
		target += sourceFileName;
	}

	baidu::common::timer::AutoTimer autoTimer(0, "Get", argv[0]);

	zfs::File *file;
	if(fs->openFile(source.c_str(), O_RDONLY, &file, zfs::ReadOptions()) != 0)
	{
		fprintf(stderr, "Get file from zfs failed: %s\n", source.c_str());
		return 1;
	}

	FILE *fp = fopen(target.c_str(), "wb");
	if(fp == NULL)
	{
		fprintf(stderr, "Open local file failed: %s\n", target.c_str());
		fclose(fp);
		delete file;
		return 1;
	}

	char buf[10240];
	int64_t bytes = 0;
	int32_t len = 0;
	while(1)
	{
		len = file->read(buf, sizeof(buf));
		if(len <= 0)
		{
			if(len < 0) fprintf(stderr, "Read from %s failed\n", source.c_str());
			break;
		}
		bytes += len;
		fwrite(buf, sizeof(char), len, fp);
	}

	printf("Read %ld bytes from %s\n", bytes, source.c_str());
	delete file;
	fclose((fp));
	return len;
}

int zfsPut(zfs::FileSystem *fs, int argc, char *argv[])
{
	if(argc < 1)
	{
		printUsage();
		return 1;
	}

	std::string source = argv[0], target;
	if(argc >= 2)
	{
		target = argv[1];
	}

	std::vector<std::string> sourcePaths;
	bool sourceIsDir = false;
	if(!baidu::common::util::SplitPath(source, &sourcePaths, &sourceIsDir)
	   || sourceIsDir || sourcePaths.empty())
	{
		fprintf(stderr, "Bad local file path: %s\n", source.c_str());
		return 1;
	}

	std::string sourceFileName = sourcePaths[sourcePaths.size()-1];
	if(target.empty() || target[target.size()-1] == '/')
	{
		target += sourceFileName;
	}

	baidu::common::timer::AutoTimer autoTimer(0, "Put", argv[0]);

	FILE *fp = fopen(source.c_str(), "rb");
	if(fp == NULL)
	{
		fprintf(stderr, "Open local file failed: %s\n", source.c_str());
		fclose(fp);
		return 1;
	}

	stat st;
	if(stat(source.c_str(), &st) != 0)
	{
		fprintf(stderr, "Get file stat info failed: %s\n", source.c_str());
		fclose(fp);
		return 1;
	}

	zfs::File *file;
	if(fs->openFile(target.c_str(), O_WRONLY | O_TRUNC, st.st_mode, &file, zfs::WriteOptions()) != 0)
	{
		fprintf(stderr, "Open file from zfs failed: %s\n", target.c_str());
		fclose(fp);
		delete file;
		return 1;
	}

	char buf[10240];
	int64_t bytes = 0;
	int32_t len = 0;
	while(1)
	{
		len = fread(buf, sizeof(char), sizeof(buf), fp);
		if(len <= 0)
		{
			if(len < 0) fprintf(stderr, "Read from %s failed\n", source.c_str());
			break;
		}
		int32_t writeLen =  file->write(buf, len);
		if(writeLen < len)
		{
			fprintf(stderr, "Write to %s failed\n", target.c_str());
			break;
		}
		bytes += len;
	}

	fclose(fp);
	delete file;
	printf("Put file to %s %ld bytes\n", target.c_str(), bytes);
	return len;
}

int main(int argc, char *argv[])
{
	FLAGS_flagfile = "./zfs.flag";
	int gflagsArgc = 1;
	::google::ParseCommandLineFlags(&gflagsArgc, &argv, false);

	if(argc < 2)
	{
		printUsage();
		return 0;
	}

	zfs::FileSystem *fs;
	std::string nameServerAddress = FLAGS_nameserver_nodes;
	if(!zfs::FileSystem::openFileSystem(nameServerAddress.c_str(), &fs, zfs::FSOptions()))
	{
		fprintf(stderr, "open FileSystem error: %s\n", nameServerAddress.c_str());
		return 1;
	}

	int res = 1;
	if(strcmp(argv[1], "touch") == 0)
	{
		res = zfsTouch(fs, argc-2, argv+2);
	}
	else if(strcmp(argv[1], "put") == 0)
	{
		res = zfsPut(fs, argc-2, argv+2);
	}
	else if(strcmp(argv[1], "get") == 0)
	{
		res = zfsGet(fs, argc-2, argv+2);
	}
	else
	{
		fprintf(stderr, "Unknow common: %s\n", argv[1]);
	}
	return res;
}
