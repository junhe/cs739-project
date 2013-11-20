//
// This header file defines all data structures used
// for communicating between processes. 

class ShuffleRequest {
    // process rank_from should send some data
    // to process rank_to.
    // Specifically, the data to be sent is 
    // at from_offset of process rank_from. It
    // should be sent to process rank_to, and be
    // saved at to_offset
    public:
        int rank_from; 
        int rank_to;
        
        //need 64bits for large file
        long long from_offset;      
        long long to_offset;
        long long length;
#define PUTREQUEST 0
#define GETREQUEST 1 
        int flag; // PUTREQUEST or GETREQUEST
        // Note that if flag==GETREQUEST, only 
        // rank_to, to_offset, length are valid.
        // The rest is uninitialized. This is 
        // because the the receiver process does
        // not know who holds the data it needs.

        int order; // this for scheduling.
                   // For example, if order=3,
                   // this request will be the
                   // third request being handled
                   // by a process.
};




