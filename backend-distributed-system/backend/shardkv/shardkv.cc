#include <grpcpp/grpcpp.h>

#include "shardkv.h"

/**
 * This method is analogous to a hashmap lookup. A key is supplied in the
 * request and if its value can be found, we should either set the appropriate
 * field in the response Otherwise, we should return an error. An error should
 * also be returned if the server is not responsible for the specified key
 *
 * @param context - you can ignore this
 * @param request a message containing a key
 * @param response we store the value for the specified key here
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status ShardkvServer::Get(::grpc::ServerContext* context,
                                  const ::GetRequest* request,
                                  ::GetResponse* response) {
    auto requestedKey = request->key();
    auto it = keyValueDatabase.find(requestedKey);
    if(it == keyValueDatabase.end()) {
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Specified key not found in the database");
    }
    response->set_data(it->second);
    return ::grpc::Status::OK;
}

/**
 * Insert the given key-value mapping into our store such that future gets will
 * retrieve it
 * If the item already exists, you must replace its previous value.
 * This function should error if the server is not responsible for the specified
 * key.
 *
 * @param context - you can ignore this
 * @param request A message containing a key-value pair
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status ShardkvServer::Put(::grpc::ServerContext* context,
                                  const ::PutRequest* request,
                                  Empty* response) {
    std::string requestedKey = request->key();
    std::string requestedData = request->data();
    std::string requestedUser = request->user();
    if(primaryServerAddress == address && !backupServerAddress.empty()) {
        auto serverChannel = ::grpc::CreateChannel(backupServerAddress, ::grpc::InsecureChannelCredentials());
        auto newkvStub = Shardkv::NewStub(serverChannel);
        ::grpc::ClientContext context;
        auto status = newkvStub->Put(&context, *request, response);
        if (!status.ok()) return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Operation failed");
    }
    int keyID = extractID(requestedKey);
    if(keyServerMap.find(keyID) == keyServerMap.end() || keyServerMap[keyID]!=shardmanager_address) {
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Server not responsible for the specified key");
    }
    if(requestedKey.find("post", 0) == std::string::npos) {
        keyValueDatabase["all_users"] += (requestedKey+",");
        keyValueDatabase[requestedKey] = requestedData;
        return ::grpc::Status::OK;
    }
    if (requestedUser.empty()) {
        keyValueDatabase[requestedKey] = requestedData;
        return ::grpc::Status::OK;
    }
    int userID = extractID(requestedUser);
    std::string postUserKey = requestedUser + "_posts";
    if (keyServerMap[userID] != shardmanager_address) {
        std::chrono::milliseconds timespan(100);
        auto serverChannel = grpc::CreateChannel(keyServerMap[userID], grpc::InsecureChannelCredentials());
        auto stub = Shardkv::NewStub(serverChannel);
        int i = 0;
        while(i < MAX_SERVER_ATTEMPTS) {
            ::grpc::ClientContext context;
            AppendRequest request;
            Empty res;
            request.set_key(postUserKey);
            request.set_data(requestedKey);
            auto status = stub->Append(&context, request, &res);
            if(status.ok()) break;
            std::this_thread::sleep_for(timespan);
            i++;
        }
        if (i == MAX_SERVER_ATTEMPTS) {
            return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Unable to contact server");
        }
    } else {
        keyValueDatabase[postUserKey] += (requestedKey + ",");
    }
    postUserMap[requestedKey] = requestedUser;
    keyValueDatabase[requestedKey] = requestedData;
    return ::grpc::Status::OK;
}

/**
 * Appends the data in the request to whatever data the specified key maps to.
 * If the key is not mapped to anything, this method should be equivalent to a
 * put for the specified key and value. If the server is not responsible for the
 * specified key, this function should fail.
 *
 * @param context - you can ignore this
 * @param request A message containngi a key-value pair
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>"
 */
::grpc::Status ShardkvServer::Append(::grpc::ServerContext* context,
                                     const ::AppendRequest* request,
                                     Empty* response) {
    std::string requestedKey = request->key();
    std::string requestedData = request->data();
    int keyID = extractID(requestedKey);
    std::string user = (requestedKey[0] == 'p') ? postUserMap[requestedKey] : (requestedKey[requestedKey.length()-1] == 's') ? "" : requestedKey;
    if(keyServerMap.find(keyID) == keyServerMap.end() || keyServerMap[keyID] != shardmanager_address) {
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Server not responsible for the specified key");
    }
    if (requestedKey.back() == 's') {
        keyValueDatabase[requestedKey].append(requestedData + ",");
        return ::grpc::Status::OK;
    }
    bool isPostKey = requestedKey.find("post_") == 0;
    std::string userKey = isPostKey ? postUserMap[requestedKey] + "_posts" : "all_users";
    if (keyValueDatabase.find(requestedKey) == keyValueDatabase.end()) {
        keyValueDatabase[requestedKey] = requestedData;
        keyValueDatabase[userKey].append(requestedKey + ",");
    } else {
        keyValueDatabase[requestedKey].append(requestedData);
    }
    return ::grpc::Status::OK;
}

/**
 * Deletes the key-value pair associated with this key from the server.
 * If this server does not contain the requested key, do nothing and return
 * the error specified
 *
 * @param context - you can ignore this
 * @param request A message containing the key to be removed
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status ShardkvServer::Delete(::grpc::ServerContext* context,
                                           const ::DeleteRequest* request,
                                           Empty* response) {
    auto requestedKey = request->key();
	if(this->keyValueDatabase.find(requestedKey)!=this->keyValueDatabase.end())
        this->keyValueDatabase.erase(requestedKey);
    else {
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Server not responsible for the specified key");
    }
    keyValueDatabase.erase(requestedKey);
    return ::grpc::Status::OK;
}

/**
 * This method is called in a separate thread on periodic intervals (see the
 * constructor in shardkv.h for how this is done). It should query the shardmaster
 * for an updated configuration of how shards are distributed. You should then
 * find this server in that configuration and look at the shards associated with
 * it. These are the shards that the shardmaster deems this server responsible
 * for. Check that every key you have stored on this server is one that the
 * server is actually responsible for according to the shardmaster. If this
 * server is no longer responsible for a key, you should find the server that
 * is, and call the Put RPC in order to transfer the key/value pair to that
 * server. You should not let the Put RPC fail. That is, the RPC should be
 * continually retried until success. After the put RPC succeeds, delete the
 * key/value pair from this server's storage. Think about concurrency issues like
 * potential deadlock as you write this function!
 *
 * @param stub a grpc stub for the shardmaster, which we use to invoke the Query
 * method!
 */
void ShardkvServer::QueryShardmaster(Shardmaster::Stub* stub) {
    Empty query;
    QueryResponse response;
    ::grpc::ClientContext cc;
    std::chrono::milliseconds timespan(100);
    auto status = stub->Query(&cc, query, &response);
    if (!status.ok()) {
        std::cerr << "Failed to query shardmaster: " << status.error_message() << std::endl;
        return;
    }
    std::map<int, std::string> newKeyServerMap;
    for (int i = 0; i < response.config_size(); ++i) {
        std::string serv = response.config(i).server();
        for (int j = 0; j < response.config(i).shards_size(); ++j) {
            for (int k = response.config(i).shards(j).lower(); k <= response.config(i).shards(j).upper(); ++k) {
                newKeyServerMap[k] = serv;
            }
        }
    }
    for (const auto& [k, serv] : newKeyServerMap) {
        if (keyServerMap.find(k) != keyServerMap.end() && serv != keyServerMap[k] && keyServerMap[k] == shardmanager_address) {
            auto serverChannel = grpc::CreateChannel(serv, grpc::InsecureChannelCredentials());
            auto stub = Shardkv::NewStub(serverChannel);
            std::vector<std::string> keys = {"user_" + std::to_string(k), "post_" + std::to_string(k), "user_" + std::to_string(k) + "_posts"};
            for (const auto& key : keys) {
                if (keyValueDatabase.find(key) != keyValueDatabase.end()) {
                    for (int i = 0; i < MAX_SERVER_ATTEMPTS; ++i) {
                        ::grpc::ClientContext cc;
                        PutRequest req;
                        Empty res;
                        req.set_key(key);
                        req.set_data(keyValueDatabase[key]);
                        auto stat = stub->Put(&cc, req, &res);
                        if (stat.ok()) {
                            keyValueDatabase.erase(key);
                            if (key.find("post_") != std::string::npos) postUserMap.erase(key);
                            else if (key.find("user_") != std::string::npos && key.find("_posts") == std::string::npos) {
                                std::string userListAsString = keyValueDatabase["all_users"];
                                std::vector<std::string> usersVector = parse_value(userListAsString, ",");
                                usersVector.erase(find(usersVector.begin(), usersVector.end(), key));
                                userListAsString = "";
                                for (auto user : usersVector) {
                                    userListAsString += user;
                                    userListAsString += ",";
                                }
                                keyValueDatabase["all_users"] = userListAsString;
                            }
                            break;
                        } else {
                            std::this_thread::sleep_for(timespan);
                        }
                    }
                }
            }
        }
    }
    keyServerMap = std::move(newKeyServerMap);
}


/**
 * This method is called in a separate thread on periodic intervals (see the
 * constructor in shardkv.h for how this is done).
 * BASIC LOGIC - PART 2
 * It pings the shardmanager to signal the it is alive and available to receive Get, Put, Append and Delete RPCs.
 * The first time it pings the sharmanager, it will  receive the name of the shardmaster to contact (by means of a QuerySharmaster).
 *
 * PART 3
 *
 *
 * @param stub a grpc stub for the shardmaster, which we use to invoke the Query
 * method!
 * */
void ShardkvServer::PingShardmanager(Shardkv::Stub* stub) {
    PingRequest request;
    request.set_server(address);
    request.set_viewnumber(currentAcknowledgedViewNumber);
    grpc::ClientContext cc;
    PingResponse response;
    auto status = stub->Ping(&cc, request, &response);
    currentAcknowledgedViewNumber = response.id();
    backupServerAddress = response.backup();
    primaryServerAddress = response.primary();
    if(status.ok()) {
        if(shardmaster_address.empty()) {
            shardmaster_address = response.shardmaster();
            if(primaryServerAddress != address) {
                auto channel = grpc::CreateChannel(primaryServerAddress, grpc::InsecureChannelCredentials());
                auto stub = Shardkv::NewStub(channel);
                grpc::ClientContext cc;
                DumpResponse dump_response;
                stub->Dump(&cc, Empty(), &dump_response);
                for(auto& kv : dump_response.database()) keyValueDatabase.insert({kv.first, kv.second});
            }
        }
    }
    return;
}

/**
 * PART 3 ONLY
 *
 * This method is called by a backup server when it joins the system for the firt time or after it crashed and restarted.
 * It allows the server to receive a snapshot of all key-value pairs stored by the primary server.
 *
 * @param context - you can ignore this
 * @param request An empty message
 * @param response the whole database
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status ShardkvServer::Dump(::grpc::ServerContext* context, const Empty* request, ::DumpResponse* response) {
    auto dataset = response->mutable_database();
    for(const auto& kv : keyValueDatabase) {
        dataset->insert({kv.first, kv.second});
    }
    return ::grpc::Status::OK;
}
