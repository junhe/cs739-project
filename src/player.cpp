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

#include "Index.h"
#include "Util.h"
#include "MapFetcher.h"

using namespace std;

int main(int argc, char **argv)
{
    string mapfilename = argv[1];
    
    // Initial MapFetcher
    MapFetcher mf(100,  mapfilename.c_str());
    HostEntry hentry; // a temp entry holder

    mf.fillBuffer(); // fill it first, since sometimes I want all entries
                     // are in memory before timing.
  
    while ( mf.fetchEntry(hentry) != EOF ) {
        cout << ".";
    }
}



