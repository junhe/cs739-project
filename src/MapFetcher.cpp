
#include <string>
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <queue>
#include <assert.h>
#include <iterator>

#include "Index.h"
#include "Util.h"
#include "MapFetcher.h"

using namespace std;

MapFetcher::MapFetcher(int bsize, const char *mapfilename)
    :_bufSize(bsize)
{ 
    assert(_bufSize > 0);
    _mapStream.open(mapfilename);
    if (! _mapStream.is_open()) {
        cout << "ERROR, map file (" << mapfilename << ") cannot be open." << endl;
        exit(1);
    }
}

MapFetcher::~MapFetcher()
{
    _mapStream.close();
}

// Return 1: got the entry
// Return EOF: reach the eof of file, entry may have
//             random data
int 
MapFetcher::readEntryFromStream(HostEntry &entry)
{
    string line;
    while (getline(_mapStream, line)) {
        istringstream iss(line);
        
        if ( !line2entry( line, entry ) == 0 ) {
            cerr << "failed to line2entry(), might be a comment" << endl;
            continue; // skip this, and fetch the next one
        } else {
            return 1;
        }
    }

    return EOF;
}

// convert a text line to a HostEntry
// return -1: not a valid plfs map entry
// return 0: success
int
MapFetcher::line2entry(string line, HostEntry &hentry)
{
    if ( line.length() > 0 ) {
        if ( line[0] == '#' ) {
            // it is a comment line
            cerr << "it is a comment line" << endl;
            return -1;
        } else {
            // it is a valid plfs map row

            // remove the weird symbols
            Util::replaceSubStr( "[", " ", line );
            Util::replaceSubStr( "]", " ", line );
            Util::replaceSubStr( ".", " ", line, 107 ); //107 is the byte # where chunk info starts

            vector<string> tokens;
            vector<string>::iterator iter;
            istringstream iss(line);
            copy(istream_iterator<string>(iss),
                istream_iterator<string>(),
                back_inserter<vector<string> >(tokens));
           
            // pid
            hentry.id = atoi( tokens[7].c_str() );

            // logical offset
            stringstream convert(tokens[2]);
            if ( !(convert >> hentry.logical_offset) ) {
                cout << "error on converting" << endl;
                exit(-1);
            }
            
            // length
            convert.clear();
            convert.str(tokens[3]);
            if ( !(convert >> hentry.length) ) {
                cout << "error on converting" << endl;
                exit(-1);
            }

            // begin_timestamp
            convert.clear();
            convert.str(tokens[4]);
            if ( !(convert >> hentry.begin_timestamp) ) {
                cout << "error on converting" << endl;
                exit(-1);
            }
            
            // end_timestamp
            convert.clear();
            convert.str(tokens[5]);
            if ( !(convert >> hentry.end_timestamp) ) {
                cout << "error on converting" << endl;
                exit(-1);
            }

            // physical offset
            convert.clear();
            convert.str(tokens[8]);
            if ( !(convert >> hentry.physical_offset) ) {
                cout << "error on converting" << endl;
                exit(-1);
            }
            
            return 0;
        }
    } else {
        // line has zero size
        cerr << "line has zero size" << endl;
        return -1;
    }
}

// fetch one entry from buffer, if buffer is
// empty, read it from map file.
// Note that when buffer size should be at least 1
//
// Return number of entries fetched, or EOF (indicating
// nothing more can be fetched (EOF and empty buffer))
int 
MapFetcher::fetchEntry(HostEntry &entry)
{
    // Fill buffer if it is empty
    if ( _entryBuf.empty() ) {
        if ( fillBuffer() == 0 ) {
            // buffer is empty due to empty source
            return EOF;
        }
    }

    assert(!_entryBuf.empty());
    entry = _entryBuf.front(); // the oldest entry
    _entryBuf.pop();
    return 1;
}

// It tries to fill up the buffer, but it can fail
// when the source (plfs map file) is empty.
// Let me assume that's the only reason. When it returns
// < buffer size, you know the source is empty.
int
MapFetcher::fillBuffer()
{
    int nleft = _bufSize - _entryBuf.size();
    while ( nleft > 0 ) {
        HostEntry lp_entry;
        int ret;
        ret = readEntryFromStream(lp_entry);
        if ( ret == 1 ) {
            _entryBuf.push(lp_entry);
            nleft--;
        } else {
            // ret == EOF
            break;
        }
    }   
    return (_bufSize - nleft);
}








