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
#include <climits>

#include <mpi.h>

#include "Index.h"
#include "Util.h"
#include "MapFetcher.h"
#include "WorkloadPool.h"

using namespace std;

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

    WorkloadPool wlpool (rank, size, argv[1]); 

//#define DEBUG
#ifdef DEBUG
    wlpool.single_fill();
    //cout << wlpool._pool.size() << endl;
    vector<ShuffleRequest> requests = 
            wlpool.get_shuffle_requests_DEBUG();
    // >>>>>>>> requests is the inputer of scheduler <<<<<<<<<
    vector<ShuffleRequest>::iterator it;
    for ( it = requests.begin();
          it != requests.end();
          it++ )
    {
        cout << it->to_str() << endl;
    }
#else
    wlpool.distributed_fill();
    // now, in each rank's wlpool._pool, we have the workload
    // for this rank
    wlpool.play_in_the_pool();
#endif

    MPI_Finalize();
    return 0;
}



