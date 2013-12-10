#ifndef __Scheduler_H__
#define __Scheduler_H__
#include <vector>
#include "request.h"

int getRandU(int nMin, int nMax);
std::vector<ShuffleRequest> randomShuffle( 
        std::vector<ShuffleRequest> reqsOrig, int processNum );
std::vector<ShuffleRequest> greedyShuffle( 
        std::vector<ShuffleRequest> reqsOrig, int processNum  );
void printSchedule(std::vector< ShuffleRequest> reqs );

#endif

