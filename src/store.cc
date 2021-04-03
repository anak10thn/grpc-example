#include "threadpool.h"
#include <string>
#include <fstream>
#include <chrono>
#include <thread>
#include <vector>
#include <iostream>
#include <grpc++/grpc++.h>
#include "store.grpc.pb.h"
#include "vendor.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;
using store::Store;
using store::ProductInfo;
using store::ProductReply;
using store::ProductQuery;

class StoreImpl final {
public:
	~StoreImpl() {
		server_->Shutdown();
		cq_->Shutdown();
	}

	void RunServer(std::string portNumber, ThreadPool *pool) {
		ServerBuilder builder;
		std::string server_address("0.0.0.0:" + portNumber);
		builder.AddListeningPort(server_address,
					 grpc::InsecureServerCredentials());
		builder.RegisterService(&service_);
		cq_ = builder.AddCompletionQueue();
		server_ = builder.BuildAndStart();
		this->pool_ = pool;
		std::cout << "Starting to run under " << server_address << std::endl;
		HandleRpcs();
	}
private:
	class CallData {
	public:
		CallData(Store::AsyncService* service, ServerCompletionQueue* cq,
			 ThreadPool *pool)
			: service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
				this->pool_ = pool;
				Proceed();
			}

		void Proceed() {
			if (status_ == CREATE) {
				std::cout << "CREATE: uhhhh." << std::endl;
				service_->RequestgetProducts(&ctx_,
							     &request_,
							     &responder_,
							     cq_,
							     cq_,
							     this);
				std::cout << " - Started processing Request GetProduct." << std::endl;
				status_ = PROCESS;
			} else if (status_ == PROCESS) {
				std::cout << "Creating alternative CallData" << std::endl;
				new CallData(service_, cq_, pool_);
				std::string query = request_.product_name();
				std::future<std::vector<VendorBid>> futureQuery = pool_->appendQuery(query);
				std::vector<VendorBid> vendorBids = futureQuery.get();
				for(VendorBid bid : vendorBids) {
					ProductInfo* product_info = reply_.add_products();
					std::cout << "Bid: (" << query << ", " << bid.vendor_id<< ", " << bid.price << ")" << std::endl;
					product_info->set_price(bid.price);
					product_info->set_vendor_id(bid.vendor_id);
				}

				std::cout << "Received response for: " << request_.product_name() << std::endl;
				status_ = FINISH;
				responder_.Finish(reply_, Status::OK, this);
			} else {
				GPR_ASSERT(status_ == FINISH);
				delete this;
			}
		}

	private:
		Store::AsyncService* service_;
		ServerCompletionQueue* cq_;
		ServerContext ctx_;
		ProductQuery request_;
		ProductReply reply_;
		ServerAsyncResponseWriter<ProductReply> responder_;
		enum CallStatus { CREATE, PROCESS, FINISH };
		CallStatus status_;
	        ThreadPool *pool_;
	};

	void HandleRpcs() {
		new CallData(&service_, cq_.get(), pool_);
		void* tag;
		bool ok;
		while(true) {
			std::cout << "Waiting for client request" << "\n";
			GPR_ASSERT(cq_->Next(&tag, &ok));
			std::cout << "Parsing client request" << "\n";
			GPR_ASSERT(ok);
			static_cast<CallData*>(tag)->Proceed();
		}
	}

	Store::AsyncService service_;
	std::unique_ptr<ServerCompletionQueue> cq_;
	std::unique_ptr<Server> server_;
	ThreadPool *pool_;
};

std::vector<std::string> getAddresses(std::string addressesLocation) {
	std::vector<std::string> ipAddresses;
	std::ifstream vendorsFile (addressesLocation);
	int addrIndex = -1;
	if (vendorsFile.is_open()) {
		std::string ipAddr;
		while (getline(vendorsFile, ipAddr)) {
			if (addrIndex == -1) {
				ipAddresses.push_back(ipAddr);
			} else if (addrIndex == 0) {
				ipAddresses.push_back(ipAddr);
				break;
			} else {
				--addrIndex;
			}
		}
		vendorsFile.close();
		return ipAddresses;
	} else {
		std::cerr << "Failed to open file " << addressesLocation << std::endl;
		return {};
	}
}

int main(int argc, char** argv) {
	int nrOfThreads;
        std::string addressesList, portNumber;
	if (argc == 4) {
		addressesList = std::string(argv[1]);
        nrOfThreads = atoi(argv[2]);
		portNumber = argv[3];
	} else {
		std::cerr << "Usage: ./store vendor_addresses number_of_threads port\n" << std::endl;
		return EXIT_FAILURE;
	}
	ThreadPool *pool = new ThreadPool(nrOfThreads, getAddresses(addressesList));
	StoreImpl store;
	store.RunServer(portNumber, pool);
}
