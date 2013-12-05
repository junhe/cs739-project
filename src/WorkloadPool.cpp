#include <iostream>
#include <sstream>
#include <vector>
#include <assert.h>
#include <climits>
#include <mpi.h>
#include <stdlib.h>

#include "WorkloadPool.h"

using namespace std;

////////////////////////////////////////
////////////////////////////////////////
////////////////////////////////////////
////////////////////////////////////////
////////////////////////////////////////
////////////////////////////////////////
// First of all, some helper functions for WorkloadPool only

// The return of this is a pair (start offset, segment size)
Pattern
decide_target_pattern(vector<HostEntry> pool)
{
    //assert( _rank == 0 ); // only rank 0 can do this
    assert( pool.size() > 0 );
    // Find the logical offset min, max
    // (max-min)/np is the segment size for each rank
    off_t off_min = LLONG_MAX; // a little conservative here..
    off_t off_max = -1; // off_t should be signed! right?
    //cout << "off_min:" << off_min << ", " << LLONG_MAX << endl;
    int max_rank = 0;
    vector<HostEntry>::iterator it;
    for ( it = pool.begin();
          it != pool.end();
          it++ )
    {
        //cout << "logical offset: " << it->logical_offset << endl;
        if ( it->logical_offset < off_min ) {
            off_min = it->logical_offset;
        }

        off_t logical_end = it->logical_offset + it->length;
        if ( logical_end > off_max ) {
            off_max = logical_end;
        }

        if ( it->id > max_rank ) {
            max_rank = it->id;
        }
    }

    int np = max_rank + 1;
    off_t segment_size = (off_max - off_min) / np;
    off_t mod = (off_max - off_min) % np;
    //cout << "starting off:" << off_min << endl;
    //cout << "segment_size:" << segment_size << endl;
    if ( mod != 0 ) {
        cout << "WARNING: mod is not zero! :" << mod << endl;
    }
    Pattern ret;
    ret.start_offset = off_min;
    ret.segment_size = segment_size;

    return ret;
}

// a helper class
class SegmentContext {
    public:
        off_t index; // which segment it is, 0,1,2,..
        off_t start_offset;
        off_t end_offset;
        off_t in_segment_offset;
        off_t original_offset;
};

// given a offset, return the context it falls in
SegmentContext
get_segment_context(off_t offset, Pattern pat)
{
    SegmentContext context;
    
    context.original_offset = offset;
    int global_index = offset / pat.segment_size;
    context.index = (offset - pat.start_offset) / pat.segment_size;
    context.in_segment_offset = offset % pat.segment_size;
    context.start_offset = pat.segment_size * global_index;
    // end_offset is the smallest offset that is outside of
    // the segment
    context.end_offset = pat.segment_size * (global_index + 1); 

    return context;
}

// This function compares the worklaod in the pool with
// the target pattern, and then figure how we should move
// the data from one rank to another.
// The output of this function will be the input of the
// scheduler.
// 
// The steps are:
//   1. look at each entry in the pool, split it if it 
//      crosses boundaries
//   2. decide the destination of each piece. We only
//      describe each edge once.
//
// The input is (starting offset, segment size)
vector<ShuffleRequest>
generate_data_flow_graph(vector<HostEntry> pool, Pattern pat)
{
    vector<ShuffleRequest> ret;

    vector<HostEntry>::iterator it;
    for ( it = pool.begin();
          it != pool.end();
          it++ )
    {
        HostEntry hentry = *it;  
        SegmentContext end_context, cur_context;
        ShuffleRequest request;

        off_t cur_off = hentry.logical_offset;
        off_t end_off = hentry.logical_offset + hentry.length;
        end_context = get_segment_context(end_off, pat);

        while ( cur_off < end_off ) {
            cur_context = get_segment_context(cur_off, pat);
            if ( cur_context.index != hentry.id ) {
                // we need shuffle
                request.rank_from = hentry.id;
                request.rank_to = cur_context.index;

                request.from_offset = cur_off;
                request.to_offset = cur_off;
                request.length = min(cur_context.end_offset, end_context.original_offset) 
                                    - cur_off;
                request.flag = PUTREQUEST;
                cout << request.to_str() << endl;
                ret.push_back(request);
            }
            cur_off = cur_context.end_offset;
            //cout << cur_off << "| " << end_off << endl;
        }
    }
    return ret;
}


////////////////////////////////////////
////////////////////////////////////////
////////////////////////////////////////
////////////////////////////////////////
////////////////////////////////////////
////////////////////////////////////////
////////////////////////////////////////
////////////////////////////////////////
////////////////////////////////////////
////////////////////////////////////////
////////////////////////////////////////
////////////////////////////////////////
////////////////////////////////////////
////////////////////////////////////////
////////////////////////////////////////
////////////////////////////////////////
////////////////////////////////////////
////////////////////////////////////////
////////////////////////////////////////
////////////////////////////////////////
////////////////////////////////////////
////////////////////////////////////////
////////////////////////////////////////
////////////////////////////////////////
////////////////////////////////////////
// WorkloadPool functions

WorkloadPool::WorkloadPool(int rank, int np, string wl_path, int bufsz)
    : _rank(rank), _np(np), _bufsize(bufsz)
{
    if ( _rank == 0 ) {
        _fetcher = new MapFetcher(1000, wl_path.c_str());
    } else {
        _fetcher = NULL;
    }
}

WorkloadPool::~WorkloadPool()
{
    if ( _fetcher != NULL )
        delete _fetcher;
}

string
WorkloadPool::pool_to_str(vector<HostEntry> pool)
{
    string poolstr;
    vector<HostEntry>::iterator it;
    for ( it = pool.begin(); it != pool.end(); it++ ) {
        poolstr += it->show() + '\n';       
    }
    return poolstr;
}

void
WorkloadPool::single_fill()
{
    assert( _rank == 0 );
    HostEntry hentry; // a temp entry holder

    _fetcher->fillBuffer(); // fill it first, since sometimes I want all entries
                           // are in memory before timing.
  
    while ( _fetcher->fetchEntry(hentry) != EOF ) {
        _pool.push_back(hentry);
    }
}


void
WorkloadPool::fill_rank0()
{
    assert( _rank == 0 );
    HostEntry hentry; // a temp entry holder
    if ( _rank == 0 ) {
        _fetcher->fillBuffer(); // fill it first, since sometimes I want all entries
                               // are in memory before timing.
      
        while ( _fetcher->fetchEntry(hentry) != EOF ) {
            //cout << hentry.show() << endl;
            if ( hentry.id == 0 ) {
                _pool.push_back(hentry);
            } else {
                // send to others

                int endflag = 0; // if it is 1, it is the end
                                 // if it is 0, new entry coming
                assert( hentry.id < _np );
                // tell receiver a job is coming
                MPI_Send(&endflag, 1, MPI_INT, 
                        hentry.id, 1, MPI_COMM_WORLD);
                // Send it out
                MPI_Send(&hentry, sizeof(hentry), MPI_CHAR, 
                        hentry.id, 1, MPI_COMM_WORLD);
            }
        }
        //cout << "number of entries:" << _pool.size() << endl;

        //cout << "fetched all entries from workload file" << endl;
        int dest_rank;
        int endflag = 1;
        for ( dest_rank = 1 ; dest_rank < _np ; dest_rank++ ) {
            assert( dest_rank < _np );
            MPI_Send(&endflag, 1, MPI_INT, dest_rank, 1, MPI_COMM_WORLD);
        }
    }
}

void
WorkloadPool::fill_rankother()
{
    assert( _rank != 0 );

    int endflag;
    MPI_Status stat;
    HostEntry hentry;
    
    while (true) {
        // get the flag and decide what to do
        MPI_Recv( &endflag, 1, MPI_INT, 0, 1, MPI_COMM_WORLD, &stat );
        if ( endflag == 0 ) {
            // have a workload entry to play
            MPI_Recv( &hentry, sizeof(hentry), MPI_CHAR,
                    0, 1, MPI_COMM_WORLD, &stat );
            //cout << "Other: " << hentry.show() << endl;
            _pool.push_back( hentry );
        } else {
            // nothing to do, the end
            break; // don't do return, comm_buf needs to be freed
        }
    }
}

void
WorkloadPool::distributed_fill()
{
    if ( _rank == 0 ) {
        fill_rank0();
    } else {
        fill_rankother();
    }
    //cout << "I am rank " << _rank << " My pool size is " << _pool.size() << endl;
}

// This function gathers all the writes
// from all the ranks (including rank0)
// to rank0, so rank0 can decide what pattern
// they should make together.
void
WorkloadPool::gather_writes()
{
    if ( _rank == 0 ) {
        // First of all, put rank0's own entries to _pool_reunion
        _pool_reunion.assign(_pool.begin(), _pool.end()); // _pool_reunion = _pool works?

        int src_rank;
        MPI_Status stat;
        for ( src_rank = 1 ; src_rank < _np ; src_rank++ ) {
            int entry_count;
            // Get the count of entries to expect
            MPI_Recv( &entry_count, 1, MPI_INT, 
                      src_rank, 1, MPI_COMM_WORLD, &stat );
            //cout << "Get count " << entry_count << " from " << src_rank << endl;
            int i;
            for ( i = 0; i < entry_count; i++ ) {
                HostEntry hentry;
                MPI_Recv( &hentry, sizeof(hentry), MPI_CHAR, 
                          src_rank, 1, MPI_COMM_WORLD, &stat );
                //cout << hentry.show() << endl;
                _pool_reunion.push_back( hentry );
            }
        }
        
        //cout << pool_to_str( _pool_reunion );
    } else {
        int count = _pool.size();
        // The rank 0 how many she will expect.
        MPI_Send(&count, 1, MPI_INT, 0, 1, MPI_COMM_WORLD);

        vector<HostEntry>::iterator it;
        for ( it = _pool.begin(); it != _pool.end(); it++ ) {
            // Send it out
            MPI_Send(&(*it), sizeof(HostEntry), MPI_CHAR, 
                        0, 1, MPI_COMM_WORLD);           
        }
    }
}



// This might be the function to be timed.
// The start of this function marks the end of our preparation phase.
// When this function is started, each rank has the workload
// it needs to do in the _pool. Now we need to start writing them to
// a shared file.
void 
WorkloadPool::play_in_the_pool()
{
    MPI_Barrier(MPI_COMM_WORLD); // This mimics the start of an application.

    gather_writes();

    if (_rank == 0) {
        Pattern pat;
        vector<ShuffleRequest> requests;

        pat = decide_target_pattern(_pool_reunion);
        requests = generate_data_flow_graph(_pool_reunion, pat);
    }

    int rc, ret;
    MPI_Status stat;
    MPI_File fh;
    char filename[] = "shared.file";

    rc = MPI_File_open( MPI_COMM_WORLD, filename, 
            MPI_MODE_CREATE | MPI_MODE_WRONLY, MPI_INFO_NULL, &fh ); 
    assert(rc == MPI_SUCCESS);

    // iterate all workload entries in the pool
    vector<HostEntry>::iterator it;
    for ( it = _pool.begin(); it != _pool.end(); it++ ) {
        //cout << it->show() << endl;

        HostEntry entry = *it;
        char *buf = (char *)malloc(entry.length);
        assert(buf != NULL);

        ret = MPI_File_write_at(fh, entry.logical_offset,
            (void *) buf,  entry.length, MPI_CHAR, &stat);
        assert(ret == MPI_SUCCESS);

        free(buf);
    }

    MPI_File_close(&fh);
}

vector<ShuffleRequest>
WorkloadPool::get_shuffle_requests_DEBUG()
{
    Pattern pat;
    vector<ShuffleRequest> requests;

    pat = decide_target_pattern(_pool);
    requests = generate_data_flow_graph(_pool, pat);

    return requests;
}


