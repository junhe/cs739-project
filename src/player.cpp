#include <iostream>
#include <fstream>
#include <sstream>
#include <iterator>
#include <stdlib.h>
#include <vector>
#include <queue>
#include <assert.h>
#include <map>
#include <iomanip>

#include <mpi.h>

#include "Index.h"
#include "Util.h"
#include "MapFetcher.h"

using namespace std;

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
        void fill();
        void play_in_the_pool();
        void gather_writes();
        string pool_to_str(vector<HostEntry> pool);
        vector<off_t> decide_target_pattern();

        WorkloadPool(int rank, int np, string wl_path, int bufsz=4096);
        ~WorkloadPool();

        vector<HostEntry> _pool;
        vector<HostEntry> _pool_reunion; // reusing _pool would make it messy.

    private:
        int _rank;
        int _np;
        size_t _bufsize; // buf to be send between ranks
        
        // this is only for rank0
        MapFetcher *_fetcher; 

        void fill_rank0();
        void fill_rankother();
};

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
WorkloadPool::fill()
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
        
        cout << pool_to_str( _pool_reunion );
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

vector<off_t>
WorkloadPool::decide_target_pattern()
{
    assert( _rank == 0 ); // only rank 0 can do this
    assert( _pool_reunion.size() > 0 );
    // Find the logical offset min, max
    // (max-min)/np is the segment size for each rank
    off_t off_min = 1 << (sizeof(off_t)-1); // a little conservative here..
    off_t off_max = -1; // off_t should be signed! right?
    vector<HostEntry>::iterator it;
    for ( it = _pool_reunion.begin();
          it != _pool_reunion.end();
          it++ )
    {
        if ( it->logical_offset < off_min ) {
            off_min = it->logical_offset;
        }

        off_t logical_end = it->logical_offset + it->length;
        if ( logical_end > off_max ) {
            off_max = logical_end;
        }
    }

    off_t segment_size = (off_max - off_min) / _np;
    off_t mod = (off_max - off_min) % _np;
    cout << "starting off:" << off_min << endl;
    cout << "segment_size:" << segment_size << endl;
    if ( mod != 0 ) {
        cout << "WARNING: mod is not zero! :" << mod << endl;
    }
    vector<off_t> ret;
    ret.push_back(off_min);
    ret.push_back(segment_size);

    return ret;
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
        decide_target_pattern();
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

int main(int argc, char **argv)
{
    int rank, size;

    MPI_Init (&argc, &argv);/* starts MPI */
    MPI_Comm_rank (MPI_COMM_WORLD, &rank);/* get current process id */
    MPI_Comm_size (MPI_COMM_WORLD, &size);/* get number of processes */

    if ( argc != 2 ) {
        if ( rank == 0 )
            cout << "usage: mpirun -np N " << argv[0] << " workload-file" << endl;
        MPI_Finalize();
        exit(1);
    }

    if ( size == 0 ) {
        cout << "Sorry, we don't want to handle 1 process case." << endl;
        MPI_Finalize();
        exit(1);
    }

    WorkloadPool wlpool (rank, size, argv[1]); 
    wlpool.fill();
    wlpool.play_in_the_pool();
    // now, in each rank's wlpool._pool, we have the workload
    // for this rank

    MPI_Finalize();
    return 0;
}



