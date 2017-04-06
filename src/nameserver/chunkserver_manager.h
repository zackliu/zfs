//
// Created by zackliu on 3/23/17.
//

#ifndef CLOUDSTORAGE_CHUNKSERVER_MANAGER_H
#define CLOUDSTORAGE_CHUNKSERVER_MANAGER_H

#include <set>
#include <map>
#include <functional>

#include <common/thread_pool.h>
#include "../proto/nameserver.pb.h"
#include "../proto/status_code.pb.h"

namespace zfs
{
	const double kChunkServerLoadMax = 0.999999;

	class BlockMapping;
	typedef std::vector<std::pair<int64_t, std::vector<std::string> > > RecoverVec;

	class Blocks
	{
	public:
		Blocks(int32_t csId) : _reportId(-1), _csId(csId) {}
		int64_t getReportId();
		void insert(int64_t blockId);
		void remove(int64_t blockId);
		void cleanUp(std::set<int64_t > *blocks);
		void moveNew();
		int64_t checkLost(int64_t reportId, const std::set<int64_t> &blocks,
						  int64_t start, int64_t end, std::vector<int64_t> *lost);

	private:
		baidu::common::Mutex _mu, _newMu;
		std::set<int64_t> _blocks, _newBlocks;
		int64_t _reportId;
		int32_t _csId;
	};


	class ChunkServerManager
	{
	public:
		struct Status
		{
			int32_t  writeQps, readQps;
			int64_t  writeSpeed, readSpeed, recoverSpeed;
		};

		ChunkServerManager(baidu::common::ThreadPool *threadPool, BlockMapping *blockMapping);
		void cleanChunkServer(ChunkServerInfo *cs, const std::string reason);
		void removeBlock(int32_t id, int64_t blockId);
		bool getChunkServerChains(int num, std::vector<std::pair<int32_t , std::string> > *chains, const std::string &clientAddress);
		std::string getChunkServerAddress(int32_t id);
		void addBlock(int32_t id, int64_t blockId);
		bool removeChunkServer(const std::string &address);
		bool handleRegister(const std::string &ip, const RegisterRequest *request, RegisterResponse *response);
		void handleHeartbeat(const HeartBeatRequest *request, HeartBeatResponse *response);
		int32_t addChunkServer(const std::string &address, const std::string &ip, const std::string &tag, int64_t quota);
		bool updateChunkServer(int csId, const std::string &tag, int64_t quota);

	private:
		void deadCheck();
		void logStatus();
		Blocks* getBlockMap(int32_t csId);
		bool getChunkServerPtr(int32_t csId, ChunkServerInfo **info);
		void randomSelect(std::vector<std::pair<double, ChunkServerInfo*> > *loads, int num);
	private:
		Status _status;
		baidu::common::ThreadPool *_threadPool;
		BlockMapping *_blockMapping;
		baidu::common::Mutex _mu;
		int32_t _chunkServerNum;
		int32_t _nextChunkServerId;
		typedef std::map<int32_t , ChunkServerInfo*> ServerMap; //id和一个ChunkServer对应
		ServerMap _chunkServers;
		std::map<std::string, int32_t> _addressMap; //socket和ChunkServerid对应
		std::map<int32_t, Blocks*> _blockMap; //ChunkServerid和所拥有的一组blocks对应
		std::map<int32_t, std::set<ChunkServerInfo*> > _heartbeatList; //first:heartbeat时间 second:Chunk信息

		std::vector<std::string> _chunkServersToOffline;

		std::string _localhostName, _localZone;
		Params _params;
	};
}


#endif //CLOUDSTORAGE_CHUNKSERVER_MANAGER_H
