#include <iostream>
#include <vector>

#include "request.h"

using namespace std;

int main()
{
    ShuffleRequest sr;
    vector<ShuffleRequest> reqs;
    int i;


    // initialize only part of it, just for 
    // demonstration.
    sr.rank_from = 1;


    for ( i = 0 ; i < 5 ; i++ ) {
        reqs.push_back(sr);
    }

    vector<ShuffleRequest>::iterator it;
    for ( it = reqs.begin() ; it != reqs.end() ; ++it ) {
        cout << it->rank_from << endl;
    }

    ///////////////////////
    // Interface of scheduler
    //
    // INPUT:
    // The request scheduler at P0 should take
    // vector<ShuffleRequest> as input.
    // I'll handle the shuffle request passing
    // among processes.
    //
    // OUTPUT:
    // vector<ShuffleRequest> with .order assigned.
    //
    //
    // Note: the output part i am not sure...
    //  

    return 0;
}



