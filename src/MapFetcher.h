#ifndef __MapFetcher_H__
#define __MapFetcher_H__

#include <queue>
#include <fstream>

class HostEntry;

class MapFetcher
{
    public:
        std::ifstream _mapStream;
        std::queue <HostEntry> _entryBuf;
        int _bufSize; // if _bufSize is 0, then read
                      // from file every time. 
                      // if not, read from buffer if buffer
                      // is no empty
        
        int readEntryFromStream(HostEntry &entry);
        int fetchEntry(HostEntry &entry);
        int fillBuffer();

        MapFetcher(int bsize, const char *mapfilename);
        ~MapFetcher();
    
    private:
        int line2entry(std::string line, HostEntry &hentry);
};


#endif

