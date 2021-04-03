#include "threadpool.h"
#include <iostream>
#include <future>
#include <functional>
#include <stdexcept>
#include <unistd.h>
#include <grpc++/grpc++.h>
#include "vendor.grpc.pb.h"
using vendor::Vendor;
using vendor::BidQuery;
using vendor::BidReply;
using grpc::Channel;

std::vector<std::thread> workers;
static std::vector<std::shared_ptr<Channel>> channels;
static thread_local std::vector<VendorClient*> vendor_clients;

ThreadPool::ThreadPool(int nrOfThreads, std::vector<std::string> vendorAddresses) {
	for(std::string address : vendorAddresses) {
		std::shared_ptr<Channel> channel_ptr =
			grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
		channels.push_back(channel_ptr);
	}

	for(int i = 0;i < nrOfThreads; i++) {
		workers.emplace_back([this, i] { run(); });
		std::cout << "Thread number : " + std::to_string(i) + " is ready crotttt" << std::endl;
	}
}

void ThreadPool::run() {
	thread_local Status status;
	for(std::shared_ptr<Channel> channel : channels) {
		VendorClient* vendor_client_ptr = new VendorClient(channel);
		vendor_clients.push_back(vendor_client_ptr);
	}
	while(true) {
		status = Status::AVAILABLE;
		std::function<void()> task;
		std::unique_lock<std::mutex> lock(queueMutex);
		condition.wait(lock, [this]{ return !tasks.empty();});
		status = Status::WORKING;
		task = tasks.front();
		tasks.pop();
		lock.unlock();

		// Execute it
		task();
	}
}

template<class F, class... Args>
auto ThreadPool::addTask(F&& f, Args&&... args)
	-> std::future<typename std::result_of<F(Args...)>::type>
{
	using return_type = typename std::result_of<F(Args...)>::type;
	auto task = std::make_shared<std::packaged_task<return_type()>>
		(std::bind(std::forward<F>(f), std::forward<Args>(args)...));

	std::future<return_type> res = task->get_future();
	{
		std::unique_lock<std::mutex> lock(queueMutex);
		tasks.push([task](){ (*task)(); });
	}
	condition.notify_one();
	return res;
}

auto ThreadPool::appendQuery(const std::string& query)
	-> std::future<std::vector<VendorBid>>
{
	std::future<std::vector<VendorBid>> futureTask =
		this->addTask( [this, query] {
			       return ThreadPool::askBid(query);
			       } );
	return futureTask;
}

std::vector<VendorBid> ThreadPool::askBid(const std::string& query)
{
	std::vector<VendorBid> vendor_bids;
	for(int i = 0; i < vendor_clients.size(); ++i) {
		VendorClient* vendor_client_ptr = vendor_clients[i];
		vendor_client_ptr->AsyncAskBid(query);
	}

	for(int i = 0; i < vendor_clients.size(); ++i) {
		VendorClient* vendor_client_ptr = vendor_clients[i];
		vendor::BidReply bid_reply = vendor_client_ptr->AsyncCompleteRpc();
		double bid_price = bid_reply.price();
		std::string vendor_id = bid_reply.vendor_id();
		vendor_bids.emplace_back(VendorBid{vendor_id, bid_price});
	}
	return vendor_bids;
}

void VendorClient::AsyncAskBid(const std::string& product_name) {
	BidQuery request;
	request.set_product_name(product_name);
	AsyncClientCall* call = new AsyncClientCall;

	call->response_reader = stub_->AsyncgetProductBid(&call->context, request, &cq_);

	call->response_reader->Finish(&call->reply, &call->status, (void*)call);
}

vendor::BidReply VendorClient::AsyncCompleteRpc() {
	void* got_tag;
	bool ok = false;

	cq_.Next(&got_tag, &ok);

	AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);

	GPR_ASSERT(ok);

	BidReply reply_;
	if (call->status.ok()) {
		std::cout << "Reply received vendor ID: " << call->reply.vendor_id() << std::endl;
		reply_ = call->reply;
	} else {
		std::cout << "RPC failed" << std::endl;
	}

	delete call;
	return reply_;
}
