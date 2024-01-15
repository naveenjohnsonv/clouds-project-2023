#include "shardmaster.h"

/**
 * Based on the server specified in JoinRequest, you should update the
 * shardmaster's internal representation that this server has joined. Remember,
 * you get to choose how to represent everything the shardmaster tracks in
 * shardmaster.h! Be sure to rebalance the shards in equal proportions to all
 * the servers. This function should fail if the server already exists in the
 * configuration.
 *
 * @param context - you can ignore this
 * @param request A message containing the address of a key-value server that's
 * joining
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status StaticShardmaster::Join(::grpc::ServerContext* context,
                                       const ::JoinRequest* request,
                                       Empty* response) {
    std::lock_guard<std::mutex> lock(serverMutex);
    if(serverShardMap.find(request->server()) != serverShardMap.end()) {
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Server already exists");
    }
    servers.push_back(request->server());
    std::vector<shard_t> newShardVector;
    serverShardMap[request->server()] = newShardVector;
    RebalanceShards(serverShardMap, servers);
    return ::grpc::Status::OK;
}

/**
 * LeaveRequest will specify a list of servers leaving. This will be very
 * similar to join, wherein you should update the shardmaster's internal
 * representation to reflect the fact the server(s) are leaving. Once that's
 * completed, be sure to rebalance the shards in equal proportions to the
 * remaining servers. If any of the specified servers do not exist in the
 * current configuration, this function should fail.
 *
 * @param context - you can ignore this
 * @param request A message containing a list of server addresses that are
 * leaving
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status StaticShardmaster::Leave(::grpc::ServerContext* context,
                                        const ::LeaveRequest* request,
                                        Empty* response) {
    std::lock_guard<std::mutex> lock(serverMutex);
    for(int i = 0; i < request->servers_size(); i++) {
        auto it = std::find(servers.begin(), servers.end(), request->servers(i));
        if(it == servers.end()) {
            return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Server doesn't exist!");
        }
        serverShardMap.erase(request->servers(i));
        servers.erase(it);
    }
    RebalanceShards(serverShardMap, servers);
    return ::grpc::Status::OK;
}

/**
 * Move the specified shard to the target server (passed in MoveRequest) in the
 * shardmaster's internal representation of which server has which shard. Note
 * this does not transfer any actual data in terms of kv-pairs. This function is
 * responsible for just updating the internal representation, meaning whatever
 * you chose as your data structure(s).
 *
 * @param context - you can ignore this
 * @param request A message containing a destination server address and the
 * lower/upper bounds of a shard we're putting on the destination server.
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status StaticShardmaster::Move(::grpc::ServerContext* context,
                                       const ::MoveRequest* request,
                                       Empty* response) {
    std::lock_guard<std::mutex> lock(serverMutex);
    auto serverIt = serverShardMap.find(request->server());
    if(serverIt == serverShardMap.end()) {
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Server doesn't exist. Move Error!");
    }
    shard_t shardToMove = {request->shard().lower(), request->shard().upper()};
    for(auto& serverShards : serverShardMap) {
        std::vector<shard_t>& shards = serverShards.second;
        std::vector<shard_t> newShardVector;
        for(const auto& shard : shards) {
            auto overlapStatus = get_overlap(shard, shardToMove);
            if(overlapStatus == OverlapStatus::OVERLAP_START) {
                newShardVector.push_back({shardToMove.upper + 1, shard.upper});
            } else if(overlapStatus == OverlapStatus::OVERLAP_END) {
                newShardVector.push_back({shard.lower, shardToMove.lower - 1});
            } else if(overlapStatus == OverlapStatus::COMPLETELY_CONTAINS) {
                newShardVector.push_back({shard.lower, shardToMove.lower - 1});
                newShardVector.push_back({shardToMove.upper + 1, shard.upper});
            } else if (overlapStatus == OverlapStatus::NO_OVERLAP) {
                newShardVector.push_back(shard);
            }
        }
        shards = std::move(newShardVector);
    }
    serverShardMap[request->server()].push_back(shardToMove);
    sortAscendingInterval(serverShardMap[request->server()]);
    return ::grpc::Status::OK;
}

/**
 * When this function is called, you should store the current servers and their
 * corresponding shards in QueryResponse. Take a look at
 * 'protos/shardmaster.proto' to see how to set QueryResponse correctly. Note
 * that its a list of ConfigEntry, which is a struct that has a server's address
 * and a list of the shards its currently responsible for.
 *
 * @param context - you can ignore this
 * @param request An empty message, as we don't need to send any data
 * @param response A message that specifies which shards are on which servers
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status StaticShardmaster::Query(::grpc::ServerContext* context,
                                        const StaticShardmaster::Empty* request,
                                        ::QueryResponse* response) {
    std::lock_guard<std::mutex> lock(serverMutex);
    for(const auto& server : servers) {
        auto configEntry = response->add_config();
        configEntry->set_server(server);
        for(const auto& shard : serverShardMap[server]) {
            auto shardEntry = configEntry->add_shards();
            shardEntry->set_lower(shard.lower);
            shardEntry->set_upper(shard.upper);
        }
    }
    return ::grpc::Status::OK;
}
