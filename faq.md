# FAQ for Project 3

#### Q1. Do we submit `client.cc`? The description.md within the project repo indicates that we need to provide an "Async client of your store". I assume this is the tests/`client.cc` source file?
**Ans:** The ``store.cc`` implementation requires both an async client and an async server. Implementing these correctly will fetch you the marks as described in the rubric. This is unrelated to the `client.cc` under test/ directory.

#### Q2. When the rubric refers to an "async client", it is the one that is used to call the vendor RPC methods, not an async client that could be used to call the store RPC methods?
**Ans:** To make it clear.

This is the flow:
1. `client.cc` talks to `store.cc`
2. `store.cc` handles connections from `client.cc` using an async server
3. `store.cc` talks to `vendor.cc` using async RPC vendor methods.

You need both an async client AND an async server in your `store.cc` implementation.

#### Q3. Can we copy the example code given by grpc? We are not allowed to copy code, but given that we are doing grpc and Google has an example on how to set up a server.  Can we copy the code from grpc to start our project?
**Ans:** Sure. Setting up the  grpc server can be treated as boilerplate code. Many of you will end up using the google example code.

#### Q4. Does the returned prices need to be sorted?
**Ans:** We don't expect it to be sorted. You're free to sort it the way you see fit, as long as the store displays all the correct results.

#### Q5. Where does store get list of vendor addresses?Do I just need to hard code the file path as `./vendor_addresses.txt` in `store.cc`? 
**Ans:** The vendor addresses can be read from a file present in same directory as the executable so you can hard code the path to "./vendor_addresses.txt" in `store.cc`. You could hard code the addresses too but it will be easier for testing if you can read it from "vendor_addresses.txt" file. While submitting, please ensure that the vendor address file is being passed in as a command line argument.

#### Q6. How will threadpool be tested?
**Ans:** We do not have hard criteria for checking threadpool but these are the things we look for : 
Threadpool should have a way to accept task ( jobs queue),the free threads in pool should have a mechanism to take up tasks from the jobs queue. Also, when a task is complete the thread should join the pool and upon destroying a threadpool object the threads in the pool should be killed and exited ( ensure no zombies after object dies).
Also, it's fine if you are seeing higher latency for concurrent request handling since all the vendor servers are local and the communication latency is minimal.
Finally, one thread has to connect to multiple vendors to collate price queries of the client and respond back to a client.

#### Q7. Are we allowed to use boosts' theadpool library implementation?
**Ans:** No. you're to write your own. You may however, understand the boost implementation and incorporate it's features into your implementation. *[don't forget to cite!]*

#### Q8. Can we assume the threadpool just takes in a void function and takes in void?  Or does the threadpool have to work for all functions passed in as a job?
**Ans:** No restrictions/requirements on nature of functions that can be queued at threadpool. If your store server queues void  input functions as tasks to threadpool then it's good enough for threadpool implementation to accept only void function tasks.

#### Q9. Does it matter where we call the threadpool?  I can see valid designs for using the threadpool from the server to call the client, or from the client to process the request.  Does it matter where we make the calls?
**Ans:** There isn't a specific guideline for that. We just wanna see if you can parallelize productquery handling using threadpool. You are free to use any logic for you async server.
 - We don't have to worry about what happens in "RequestgetProducts" - we just tell the system we're ready to listen to yet another rpc call.
- From our perspective, our main thread requests the grpc service to listen for an RPC call.
- We tell it to then inform us through the completion queue to tell us when the request comes in.
- In order to minimize the time spent in processing this RPC which came in, we just happen to use a thread from the pool to process and reply back to the client.
- Meanwhile our main thread can request for another rpc event and wait on queue.

#### Q10. I can make several async calls, but eventually I need to block until all the vendor results are back. There doesn't appear to be much useful work my thread can do until every ProductBid is back either. So is this asynchronous?
**Ans:** Yes you need to block until all vendor requests are complete.  The vendor requests themselves, however, can all be made in parallel which is what makes them async (vs serializing vendor requests).
#### Q11. So my threadpool design is largely based on threadpool examples online Is it okay?
**Ans:** Yes. But please don’t forget to cite.

#### Q12. The store client (async) talks to the vendor server (sync), and store server (async) talks to the client (sync), so both the communications have half sync and half async. Is this allowed? 
**Ans:** Yes, this is allowed--you don't have control over anything but `store.cc`, which must be entirely async. Whether the clients and stores you're connecting to use async or sync communication is out of your hands, and both do not have to adhere to the same synchronization approach in tandem. GRPC takes care of all the details for you.
