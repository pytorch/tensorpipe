# EFA

The EFA communication model can be considered as a simplified ibverbs. The send/recv operation is async by the event queue and completion queue. And the operation itself doesn't need memory registration like ibverbs and also doesn't act like stream operation in socket. The complexity at the memory part is handled by the underlying provider(libfabric+efa). The overall implementation can be considered as Reactor from ibverbs + StreamOperation from uv.

EFA supports the send-after-send order guarantees for data operation, which means the message order is preserved. However, the completion order is not guaranteed when reading events from completion queue.
For example, sender posts S1, S2, S3 three send operations; receiver posts R1, R2, R3 three recv operations. These operations are exactly matched due to send-after-send guarantee. But when reading from the completion queue at receiver side, the completion order might be R2, R1, R3 or other. Same for the sender side.

This brings complexity in the busy polling thread when dealing with completion events, since the callback of write operations should be executed in order. To address this issue, the pointer of the `EFAWriteOperation` is passed as operation context when post send event. And in the completion stage, it will set the mode of `EFAWriteOperation` to completed. A seperate function is executed later by iterating the `writeOperations_` deque from front, and execute callback if the operation is done.  

For the receiver part, it's more complex. For example there are two incoming writeOperations. It will become 4 send operation at sender side, SEND_SIZE1, SEND_PAYLOAD1, SEND_SIZE2, SEND_PAYLOAD2. At the receiver side, the expected behavior is 
1. Post receive event of single 64bits size, such as RECV_SIZE1
2. Poll from cq when RECV_SIZE1 is done, and post RECV_PAYLOAD1 with the size in RECV_SIZE1
3. Did the same thing for the second operation

However when the four send operation issued concurrently, the first completion event at receiver side might be RECV_SIZE2. If we follow the process above, the recv order will be messed up. To address this problem, the implementation used tag matching. That each operation will have a index decided at the sender side. Two indicator, kLength=1ULL<<32, kPayload=1ULL<<33, are used to indicate the type of the message. The message tag is a 64bit integer, that the high 32 bits are indicators (kLength or kPayload), and the low 32 bits are operation ids. 

Send side:

1. `post_send(buffer=&length, size=sizeof(int64_t), tag=kLength | msg_id, ...)`
2. `post_send(buffer=buffer, size=length, tag = kPayload | msg_id, ...)`
3. `msg_id++` msg_id is uint32_t

Receiver side:
At receiver size, we first recv the message with high 32 bits equaling kLength and decode the index from low 32 bits. And then post a recv event with tag `kPayload | msg_id`
1. `post_recv(buffer=&length, size=sizeof(int64_t), tag=kLength, ignore=0xffffffff, ...)` ignore=0xfffffff means ignore lower 32 bits when matching tag
2. decode message id from the incoming message tag (take lower 32bits)
3. `post_recv(buffer=buffer, size=length, tag=kPayload | msg_id, ignore=0, ...)` ignore=0 means the tag should be exactly the same to match