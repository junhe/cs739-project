#include <iostream>
#include <algorithm>    // std::random_shuffle
#include <vector>       // std::vector
#include <ctime> 

#include "Scheduler.h"

////////////////////////////////////////
///  scheduler from Jia

using namespace std;

int getRandU(int nMin, int nMax)
{
   // return nMin + (int)((double)rand() / (RAND_MAX+1) * (nMax-nMin+1));
    return nMin + (rand() % (int)(nMax - nMin + 1));
}

// a random scheduling, with possble stochasitic guarantees
vector<ShuffleRequest>  randomShuffle( vector<ShuffleRequest> reqsOrig, int processNum ){
    
    //int *processOrderCount = new int(processNum);
    int *processOrderCount = new int[processNum];
    
    for (int i=0;  i<processNum; i++){
        processOrderCount[i] = 0;
    }
    
    vector<ShuffleRequest> reqsSched;
    
    // random permutation
    vector<int> shufflePerm;    
    // initialize
    for (size_t i=0; i<reqsOrig.size(); i++) 
        shufflePerm.push_back(i); // 1 2 3 4 5 6 7 8 9
    
    // using built-in random generator:
    srand ( unsigned ( std::time(0) ) );
    std::random_shuffle ( shufflePerm.begin(), shufflePerm.end() );
 
    for(size_t i=0; i<reqsOrig.size(); i++){
        //cout<< shufflePerm[i] <<endl;
     //   ShuffleRequest sr = reqsOrig[ shufflePerm[i] ];
        ShuffleRequest sr;
        
        sr = reqsOrig[ shufflePerm[i] ];
        
        //sr.rank_from = reqsOrig[ shufflePerm[i] ].rank_from;
        //sr.rank_to = reqsOrig[ shufflePerm[i] ].rank_to;
        
        processOrderCount[sr.rank_from]++;
        processOrderCount[sr.rank_to]++;
        
      //  cout << "i: "<< i<<endl;
        //cout<<  "sr.rank_from: " << sr.rank_from <<endl;
        //cout<<  "processOrderCount[sr.rank_from]: " << processOrderCount[sr.rank_from] <<endl;
        
        sr.sendOrder = processOrderCount[sr.rank_from];
        sr.receiveOrder = processOrderCount[sr.rank_to];
        
        
       
       // cout<<  "sr.sendOrder : " << sr.sendOrder <<endl;
         
        reqsSched.push_back(sr);
        // cout <<"Push success."<<endl;
    }
        
    
    // Clean up
    
   // delete(processOrderCount);
    
    return reqsSched;
}



// a greedy heuristic algorithm
vector<ShuffleRequest> greedyShuffle( vector<ShuffleRequest> reqsOrig, int processNum  ){
    
    int *processOrderCount = new int[processNum];
    
    for (int i=0;  i<processNum; i++){
        processOrderCount[i] = 0;
    }
    
    // request schedule record: 1 for already scheduled; 0 for non-scheduled
    int *requestScheduled = new int[reqsOrig.size()];
    
    for (size_t i=0;  i<reqsOrig.size(); i++){
        requestScheduled[i] = 0;
    }
    
    // process workload: including both sending and receiving
    double *processWorkload = new double[processNum];
    
    for (int i=0;  i<processNum; i++){
        processWorkload[i] = 0;
    }
    
    // process running record
    int *processBusy = new int[processNum];
    
   
    
    
    
    // Count node workload
    for (size_t i=0; i<reqsOrig.size(); i++) {
        
        processWorkload[reqsOrig[i].rank_from] += (double) reqsOrig[i].length;
        processWorkload[reqsOrig[i].rank_to] += (double) reqsOrig[i].length;
        
    }

    vector<ShuffleRequest> reqsSched;
    size_t scheduledCount = 0;

    while(scheduledCount < reqsOrig.size()){
        
        // degug to see how many rounds all together
        
        cout<<"scheduledCount: "<< scheduledCount <<endl;
       
        // Initialize process status
        for (int i=0;  i<processNum; i++){
            processBusy[i] = 0;
        }

        //Stage 1;
        
        // find one non-scheduled request with largest weights
       
        double maxWeight1 = 0;
        int reqsIndex1 = -1;
        for(size_t i=0; i<reqsOrig.size(); i++){
        
            if (requestScheduled[i] > 0){
                continue;
            }
            
            double requestWeights1 = processWorkload[reqsOrig[i].rank_from] + processWorkload[reqsOrig[i].rank_to];
            if (requestWeights1 > maxWeight1) {
                maxWeight1 = requestWeights1 ;
                reqsIndex1 = i;
                
            }
            
        }
            
            
        if (reqsIndex1 >= 0 ){
            
            scheduledCount ++;
            
            requestScheduled[reqsIndex1] = scheduledCount;
                
            ShuffleRequest sr1 = reqsOrig[ reqsIndex1 ];
   
            processOrderCount[sr1.rank_from]++;
            processOrderCount[sr1.rank_to]++;
                
            sr1.sendOrder = processOrderCount[sr1.rank_from];
            sr1.receiveOrder = processOrderCount[sr1.rank_to];
            sr1.order = scheduledCount;
        
            reqsSched.push_back(sr1);
            
            //update process status
            processBusy[sr1.rank_from] = 1;
            processBusy[sr1.rank_to] = 1;
            
            // update process work load
            
            processWorkload[sr1.rank_from] -= (double) sr1.length;
            processWorkload[sr1.rank_to] -= (double) sr1.length;
            
            // Stage 2
            
            bool processToAdd = true;
            while(processToAdd){
                
                processToAdd = false;
                
                double maxWeight2 = 0;
                int reqsIndex2 = -1;
                for(size_t i=0; i<reqsOrig.size(); i++){
                    
                    if (requestScheduled[i] > 0 || processBusy[reqsOrig[i].rank_from>0 || processBusy[reqsOrig[i].rank_to]]){
                        continue;
                    }
                    
                    double requestWeights2 = processWorkload[reqsOrig[i].rank_from] + processWorkload[reqsOrig[i].rank_to];
                    if (requestWeights2 > maxWeight2) {
                        maxWeight2 = requestWeights2 ;
                        reqsIndex2 = i;
                        processToAdd = true;
                        
                    }
                    
                }
            
            
                if (reqsIndex2 >= 0 ){
                    
                    scheduledCount ++;
                    
                    requestScheduled[reqsIndex2] = scheduledCount;
                    
                    ShuffleRequest sr2 = reqsOrig[ reqsIndex2 ];
                    
                    processOrderCount[sr2.rank_from]++;
                    processOrderCount[sr2.rank_to]++;
                    
                    sr2.sendOrder = processOrderCount[sr2.rank_from];
                    sr2.receiveOrder = processOrderCount[sr2.rank_to];
                    sr2.order = scheduledCount;
                    
                    reqsSched.push_back(sr2);
                    
                    //update process status
                    processBusy[sr2.rank_from] = 1;
                    processBusy[sr2.rank_to] = 1;
                    
                    // update process work load
                    
                    processWorkload[sr2.rank_from] -= (double) sr2.length;
                    processWorkload[sr2.rank_to] -= (double) sr2.length;
                }
                

            }
            
            
        }
        
    }
    
    // debug
    for (size_t i=0;  i<reqsOrig.size(); i++){
        ; //cout << "request no. " << i<< "; order:  "<< requestScheduled[i] << "; length: "<< reqsOrig[i].length << endl;
    }
    
    for (int i=0;  i<processNum; i++){ // all should be zero
        ; // cout << "processWorkload: "<< processWorkload[i] <<  endl;
    }

    
    
    // Clean up
    
    delete(processOrderCount);
    delete(processWorkload);
    
    return reqsSched;
}




void printSchedule(vector< ShuffleRequest> reqs ){
    vector<ShuffleRequest>::iterator it;
    for ( it = reqs.begin() ; it != reqs.end() ; ++it ) {
        cout << it->rank_from<< " --> " <<  it->rank_to  
             << " sendOrder: " << it->sendOrder 
             << " receiveOrder: "  <<  it->receiveOrder << endl;
    }
}


