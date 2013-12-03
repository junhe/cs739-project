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
    private:
        int _rank;
        int _np;
        size_t _bufsize; // buf to be send between ranks
        
        // this is only for rank0
        MapFetcher *_fetcher; 
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
WorkloadPool::fill()
{
    HostEntry hentry; // a temp entry holder
    if ( _rank == 0 ) {
        _fetcher->fillBuffer(); // fill it first, since sometimes I want all entries
                               // are in memory before timing.
      
        while ( _fetcher->fetchEntry(hentry) != EOF ) {
            cout << hentry.show() << endl;
        }
    }
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

    MPI_Finalize();
    return 0;
}



