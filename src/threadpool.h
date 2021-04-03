#pragma once
#include <thread>
#include <queue>
#include <future>
#include <condition_variable>
#include <grpc++/grpc++.h>
#include "vendor.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using vendor::BidQuery;
using vendor::BidReply;
using vendor::Vendor;

struct VendorBid
{
    std::string vendor_id;
    double price;
};

class VendorClient
{
public:
    explicit VendorClient(std::shared_ptr<Channel> channel)
        : stub_(Vendor::NewStub(channel)) {}
    void AsyncAskBid(const std::string &product_name);
    vendor::BidReply AsyncCompleteRpc();

private:
    struct AsyncClientCall
    {
        BidReply reply;
        ClientContext context;
        Status status;
        std::unique_ptr<grpc::ClientAsyncResponseReader<BidReply> > response_reader;
    };

    std::unique_ptr<Vendor::Stub> stub_;
    grpc::CompletionQueue cq_;
};

class ThreadPool
{
public:
    ThreadPool(int, std::vector<std::string>);
    ~ThreadPool();
    enum Status
    {
        AVAILABLE,
        WORKING
    };
    template <class F, class... Args>
    auto addTask(F &&f, Args &&...args)
        -> std::future<typename std::result_of<F(Args...)>::type>;

    std::future<std::vector<VendorBid> > appendQuery(const std::string &query);

private:
    void run();
    std::vector<VendorBid> askBid(const std::string &query);
    std::vector<std::thread> workers;
    std::queue<std::function<void()> > tasks;
    std::condition_variable condition;
    std::mutex queueMutex;
};
