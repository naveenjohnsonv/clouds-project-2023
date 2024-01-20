#include <grpcpp/grpcpp.h>

#include "shardkv_manager.h"

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
::grpc::Status ShardkvManager::Get(::grpc::ServerContext* context,
                                  const ::GetRequest* request,
                                  ::GetResponse* response) {
    std::lock_guard<std::mutex> lock(serverMutex);
    auto serverChannel = ::grpc::CreateChannel(primaryServerAddress, ::grpc::InsecureChannelCredentials());
    Shardkv::Stub shardkvStub(serverChannel);
    ::grpc::ClientContext cc;
    auto status = shardkvStub.Get(&cc, *request, response);
    return status.ok() ? ::grpc::Status::OK : ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Operation failed");
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
::grpc::Status ShardkvManager::Put(::grpc::ServerContext* context,
                                  const ::PutRequest* request,
                                  Empty* response) {
    std::lock_guard<std::mutex> lock(serverMutex);
    auto serverChannel = ::grpc::CreateChannel(primaryServerAddress, ::grpc::InsecureChannelCredentials());
    Shardkv::Stub shardkvStub(serverChannel);
    ::grpc::ClientContext cc;
    auto status = shardkvStub.Put(&cc, *request, response);
    return status.ok() ? ::grpc::Status::OK : ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Operation failed");
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
::grpc::Status ShardkvManager::Append(::grpc::ServerContext* context,
                                     const ::AppendRequest* request,
                                     Empty* response) {
    std::lock_guard<std::mutex> lock(serverMutex);
    auto serverChannel = ::grpc::CreateChannel(primaryServerAddress, ::grpc::InsecureChannelCredentials());
    Shardkv::Stub shardkvStub(serverChannel);
    ::grpc::ClientContext cc;
    auto status = shardkvStub.Append(&cc, *request, response);
    return status.ok() ? ::grpc::Status::OK : ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Operation failed");
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
::grpc::Status ShardkvManager::Delete(::grpc::ServerContext* context,
                                           const ::DeleteRequest* request,
                                           Empty* response) {
    std::lock_guard<std::mutex> lock(serverMutex);
    auto serverChannel = ::grpc::CreateChannel(primaryServerAddress, ::grpc::InsecureChannelCredentials());
    Shardkv::Stub shardkvStub(serverChannel);
    ::grpc::ClientContext cc;
    auto status = shardkvStub.Delete(&cc, *request, response);
    return status.ok() ? ::grpc::Status::OK : ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Operation failed");
}

/**
 * In part 2, this function get address of the server sending the Ping request, who became the primary server to which the
 * shardmanager will forward Get, Put, Append and Delete requests. It answer with the name of the shardmaster containeing
 * the information about the distribution.
 *
 * @param context - you can ignore this
 * @param request A message containing the name of the server sending the request, the number of the view acknowledged
 * @param response The current view and the name of the shardmaster
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status ShardkvManager::Ping(::grpc::ServerContext* context, const PingRequest* request,
                                       ::PingResponse* response) {
    std::lock_guard<std::mutex> lock(serverMutex);
    std::string serverAddress = request->server();
    std::vector<std::string> currentViewServers;
    if(primaryServerAddress.empty() || primaryServerAddress == serverAddress) {
        primaryServerAddress = (primaryServerAddress.empty()) ? serverAddress : primaryServerAddress;
        lastAcknowledgedViewNumber = (primaryServerAddress == serverAddress) ? request->viewnumber() : currentViewNumber;
        currentViewNumber = (primaryServerAddress == serverAddress) ? currentViewNumber : currentViewNumber + 1;
        currentViewServers.push_back(primaryServerAddress);
        currentViewServers.push_back(backupServerAddress.empty() ? "" : backupServerAddress);
        views[currentViewNumber] = currentViewServers;
        response->set_id(currentViewNumber);
        response->set_primary(primaryServerAddress);
        response->set_backup(backupServerAddress.empty() ? "" : backupServerAddress);
    } else if (backupServerAddress.empty() && serverAddress != primaryServerAddress) {
        backupServerAddress = serverAddress;
        currentViewNumber++;
        currentViewServers.push_back(primaryServerAddress);
        currentViewServers.push_back(backupServerAddress);
        views[currentViewNumber] = currentViewServers;
        response->set_id(lastAcknowledgedViewNumber);
        response->set_primary(primaryServerAddress);
        response->set_backup(backupServerAddress);
    } else if (backupServerAddress == serverAddress) {
        std::string primary = views[lastAcknowledgedViewNumber].at(0);
        std::string backup = views[lastAcknowledgedViewNumber].at(1);
        response->set_primary(primary);
        response->set_backup(backup);
        response->set_id(lastAcknowledgedViewNumber);
    } else {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Exceeded server capacity");
    }
    pingIntervals[serverAddress].Push(std::chrono::high_resolution_clock::now());
    response->set_shardmaster(sm_address);
    return ::grpc::Status::OK;
}
