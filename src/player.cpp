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
        WorkloadPool(int rank, int np, string wl_path, int bufsz=4096);
        ~WorkloadPool();

        vector<HostEntry> _pool;

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
        cout << "number of entries:" << _pool.size() << endl;

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
            cout << "Other: " << hentry.show() << endl;
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
    cout << "I am rank " << _rank << " My pool size is " << _pool.size() << endl;
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
        return 1;
    }


    WorkloadPool wlpool (rank, size, argv[1]); 
    wlpool.fill();
    // now, in each rank's wlpool._pool, we have the workload
    // for this rank

    MPI_Finalize();
    return 0;
}



