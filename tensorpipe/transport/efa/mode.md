The general model of EFA is similar to efa, which has event queues and completion queues for send/recv.
User have to poll the completion queue to trigger the send/recv event happen. Otherwise the event will just stay in the event queue.

The code below mainly from aws-ofi-nccl, which used libfabric to implement nccl's interface

Send process:

1. Push sent event to the completion queue
```Cpp
while (true){
    rc = fi_send(...) # return code
    if (rc == 0)
        break; # send succeed
    else if (rc == -FI_EAGAIN) {
        # This is a retryable error
        # Can attempt to progress the completion queue to make send event happen
        /*
        * Process completions so that you have enough
        * resources for sending connect message
        */
        ret = nccl_ofi_progress(nccl_ofi_component[dev]);
        if (OFI_UNLIKELY(ret != 0))
            goto error;
    }
    else {
        NCCL_OFI_WARN("Unable to send connect message for dev %d. RC: %zd, ERROR: %s",
                    dev, rc, fi_strerror(-rc));
        ret = ncclSystemError;
        goto error;
    }
} while (true);
```
This part is bit different from efa, that pushing send event to the event queue may fail, which might need retry.

2. Progress the completion queue
```Cpp
do {
    ret = nccl_ofi_progress(nccl_ofi_component[dev]);
    if (OFI_UNLIKELY(ret != 0))
        goto error;
} while (true);
```

The receive process is the same as the send process. 

Some design question I'd like to ask for suggestions:
1. Since the memory doesn't need to be pinned/registered, do I still need the RingBuffer related class for EFA?
2. How should I arrange the event loop? Previously I used a busy polling thread for the progress of completion queue, and retrying push the send event to the queue directly in the main thread. Is this a good practice in tensorpipe?
  