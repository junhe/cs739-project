#ifndef __request_H__
#define __request_H__

#include <string>
#include <sstream>
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
        off_t offset;      
        off_t length;

        int sendOrder;
        int receiveOrder;
        int order; // this for scheduling.
                   // For example, if order=3,
                   // this request will be the
                   // third request being handled
                   // by a process.

        std::string to_str() {
            std::ostringstream oss;
            oss 
                << "Flow:"
                << rank_from << "->"
                << rank_to << ","
                << "offset:"
                << offset << ","
                << "length:"
                << length << ","
                << "order:"
                << order << ","
                << "sendOrder:"
                << sendOrder << ","
                << "receiveOrder"
                << receiveOrder;
            return oss.str();
        }
};


#endif

