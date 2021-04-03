### Big Picture

  - In this project, you are going to implement major chunks of a simple distributed service using [grpc](http://www.grpc.io).
  - Learnings from this project will also help you in the next project as you will become familiar with grpc and multithreading with threadpool.
  
### Overview
  - You are going to build a store (You can think of Amazon Store!), which receives requests from different users, querying the prices offered by the different registered vendors.
  - Your store will be provided with a file of <ip address:port> of vendor servers. On each product query, your server is supposed to request all of these vendor servers for their bid on the queried product.
  - Once your store has responses from all the vendors, it is supposed to collate the (bid, vendor_id) from the vendors and send it back to the requesting client.
  
### Learning outcomes
  - Synchronous and Asynchronous RPC packages
  - Building a multi-threaded store in a distributed service

### Environment Setup

  - Follow this [link](https://grpc.io/docs/languages/cpp/quickstart/) for a cmake based setup

### How You Are Going to Implement It ( Step-by-step suggestions)

1. Make sure you understand how GRPC- synchronous and asynchronous calls work. Understand the given helloworld [example](https://github.com/grpc/grpc/tree/master/examples/cpp/helloworld). You will be building your store with asynchronous mechanisms ONLY.
2. Establish asynchronous GRPC communication between -
    - Your store and user client. 
    - Your store and the vendors.  
3. Create your thread pool and use it. Where will you use it and for what?
   Upon receiving a client request, you store will assign a thread from the thread pool to the incoming request for processing.
    - The thread will make async RPC calls to the vendors
    - The thread will await for all results to come back
    - The thread will collate the results
    - The thread will reply to the store client with the results of the call
    - Having completed the work, the thread will return to the thread pool
4. Do you have your user client request reaching to the vendors now? And can you see the bids from the different vendors at your user client end? Congratulations you almost got it! Now use the test harness to test if your server can serve multiple clients concurrently and make sure that your thread handling is correct. 

### Keep In Mind
1. Your Server has to handle
  - Multiple concurrent requests from clients
  - Be stateless so far as client requests are concerned (once the client request is serviced it can forget the client)
  - Manage the connections to the client requests and the requests it makes to the 3rd party vendors.
2.  Server will get the vendor addresses from a file with line separated strings <ip:port>
3.  Your server should be able to accept `command line input` of the vendor addresses file,  `address` on which it is going to expose its service and `maximum number of threads` its threadpool should have.
4. The format of the invocation is:

       ./store <filepath for vendor addresses> \
               <ip address:port to listen on for clients> \
               <maximum number of threads in threadpool>
 

### Given to You
  1. run_tests.cc - This will simulate real world users sending concurrent product queries. This will be released soon to you.
  2. client.cc - This will be providing ability to connect to the store as a user.
  3. vendor.cc - This wil act as the server providing bids for different products. Multiple instances of it will be run listening on different ip address and port.
  4. `Two .proto files`  
    - store.proto - Comm. protocol between user(client) and store(server)  
    - vendor.proto -Comm. protocol between store(client) and vendor(server)  

### How to run the test setup
  - Go to project3 directory and build the program.
  - Three binaries would be created in the bin folder - `store`,`run_tests` and `run_vendors`. (Note that the location of bin folder depends on how you build the program.)  
  - First run the command `./run_vendors vendor_addresses.txt &` to start a process which will run multiple servers on different threads listening to (ip_address:ports) from the file given as command line argument.
  - Then start up your store which will read the same address file to know vendors' listening addresses. Also, your store should start listening on a port(for clients to connect to) given as command line argument.
  - Then finally run the command `./run_tests $IP_and_port_on_which_store_is_listening $max_num_concurrent_client_requests` to start a process which will simulate real world clients sending requests at the same time.
  - This process read the queries from the file `product_query_list.txt`
  - It will send some queries and print back the results, which you can use to verify your whole system's flow.

### Grading
This project is not performance oriented, we will only test the functionality and correctness.  

Below is the rubric:

**Total Possible Score:** 12

| Score | Reason |
| ----- | ------ |
| +2 | Code compiles |
| +4 | Query output is correct |
| +3 | Threadpool management |
| +1 | `store-server` operates in `async` fashion |
| +1 | `store-client` operates in `async` fashion |
| +1 | Readme |


### Deliverables
Please follow the instructions carefully. The folder you hand in must contain the following:
  - `Readme.txt` - text file containing anything about the project that you want to tell the TAs.
  - `CMakeLists.txt` - You might need to change it if you add more source files.
  - `Store source files` - store.cc(must) containing the source code for store management
  - `Threadpool source files` - threadpool.h(must), containing the source code for threadpool management
  - You can add supporting files too(in addition to above two), if you need to keep your code more structured, clean etc.

**Submission Directory Structure:**

      Readme.txt
      src/CMakeLists.txt
      src/store.cc
      src/threadpool.h
      src/any_additional_supporting_files.*

You must use the collect_submission.py to create the submission zip file. Submit the zip file in gradescope. You can verify your submission using the autograder in gradescope.

# FAQ

FAQ can be found [here](faq.md).
