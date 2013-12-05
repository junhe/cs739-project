#ifndef __WorkloadPool_H__
#define __WorkloadPool_H__

#include <vector>
#include <string>

#include "request.h"
#include "Index.h"
#include "MapFetcher.h"

class Pattern {
    public:
        off_t start_offset;
        off_t segment_size;
};


// I need this class to:
// At rank0: 
//  fetch workload entries from file
//  send workload entries to all PEs, includeing rank0
// At rank 1~n-1:
//  receive and execute entry
//
// Note that this class does NOT do MPI_init() and 
// MPI_finalize(). You need to use this class between them.
class WorkloadPool {
    public:
        void single_fill(); // fill rank 0 with workload for debug
        void distributed_fill(); //fill many ranks with workload
        void play_in_the_pool();
        void gather_writes();
        std::string pool_to_str(std::vector<HostEntry> pool);
        std::vector<ShuffleRequest>
            get_shuffle_requests_DEBUG();
        void distribute_requests(std::vector<ShuffleRequest> requests);
        void receive_requests();

        WorkloadPool(int rank, int np, std::string wl_path, int bufsz=4096);
        ~WorkloadPool();

        std::vector<HostEntry> _pool;
        std::vector<HostEntry> _pool_reunion; // reusing _pool would make it messy.
        std::vector<ShuffleRequest> _shuffle_plan;

    private:
        int _rank;
        int _np;
        size_t _bufsize; // buf to be send between ranks
        
        // this is only for rank0
        MapFetcher *_fetcher; 

        void fill_rank0();
        void fill_rankother();
};

#endif

