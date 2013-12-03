#include <sstream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <unistd.h>
#include <iostream>
#include <stdlib.h>
#include <map>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <algorithm>

#include "Index.h"
#include "Util.h"

using namespace std;

//////////////////////////////////////////////////////////////
// HostEntry

string
HostEntry::show() const
{
    ostringstream oss;

    oss.precision(26);
    oss << id 
        << " "
        << logical_offset
        << " "
        << length
        << " "
        << physical_offset
        << " "
        << begin_timestamp
        << " "
        << end_timestamp
        << " ";
    return oss.str();
}

bool 
HostEntry::compareLogical_offset(
        const HostEntry &a, const HostEntry &b)
{
    return a.logical_offset < b.logical_offset;
}


///////////////////////////////////////////////////////////////
// Index

Index::Index(const char *physical_path, bool compress_contiguous)
    : _physical_path(physical_path),
      _index_fd(-1),
      _compress_contiguous(compress_contiguous)

{
    // open physical file for index
    _index_fd = Util::Open(_physical_path.c_str(), 
                        O_CREAT|O_TRUNC|O_WRONLY);
    assert( _index_fd != -1 );
}

Index::Index(bool compress_contiguous)
    : _index_fd(-1),
      _compress_contiguous(compress_contiguous)
{
}

Index::~Index()
{
    if ( _index_fd != -1 ) {
        Util::Close(_index_fd);
    }
}

void
Index::addEntry( const HostEntry &entry )
{
    _hostIndex.push_back( entry );
}

int
Index::flush()
{
    // ok, vectors are guaranteed to be contiguous
    // so just dump it in one fell swoop
    size_t  len = _hostIndex.size() * sizeof(HostEntry);
    
    if ( len == 0 ) {
        return 0;    // could be 0 if we weren't buffering
    }
    
    void *start = &_hostIndex[0];
    int ret     = Util::WriteN(start, len, _index_fd);
    if ( (size_t)ret != (size_t)len ) {
        cerr << "Failed to write to Index file" << endl;
        exit(-1);
    }
    Util::Flush(_index_fd);
    _hostIndex.clear();
    return((ret < 0) ? ret : 0);
}


//TODO: I ignored timestamp of this function
void 
Index::addWrite( off_t offset, size_t length, pid_t pid,
               double begin_timestamp, double end_timestamp )
{
    // check whether incoming abuts with last and we want to compress
    if ( _compress_contiguous && !_hostIndex.empty() &&
            _hostIndex.back().id == pid  &&
            _hostIndex.back().logical_offset +
            (off_t)_hostIndex.back().length == offset) {
        _hostIndex.back().end_timestamp = end_timestamp;
        _hostIndex.back().length += length;
        _physical_offsets[pid] += length;
        /* printf("Merged new write with last at offset %ld."
             " New length is %d.\n",
             (long)_hostIndex.back().logical_offset,
             (int)_hostIndex.back().length );
        */
    } else {
        // create a new index entry for this write
        HostEntry entry;
        memset(&entry,0,sizeof(HostEntry)); // suppress valgrind complaint
        entry.logical_offset = offset;
        entry.length         = length;
        entry.id             = pid;
        entry.begin_timestamp = begin_timestamp;
        // valgrind complains about this line as well:
        // Address 0x97373bc is 20 bytes inside a block of size 40 alloc'd
        entry.end_timestamp   = end_timestamp;
        // lookup the physical offset
        map<pid_t,off_t>::iterator itr = _physical_offsets.find(pid);
        if ( itr == _physical_offsets.end() ) {
            _physical_offsets[pid] = 0;
        }
        entry.physical_offset = _physical_offsets[pid];
        _physical_offsets[pid] += length;
        _hostIndex.push_back( entry );
        // Needed for our index stream function
        // It seems that we can store this pid for the global entry
    }
}

void
Index::sortEntries()
{
    sort(_hostIndex.begin(), _hostIndex.end(), HostEntry::compareLogical_offset);
}

string
Index::show() const
{
    ostringstream oss;
    vector<HostEntry>::const_iterator it;
    for ( it =  _hostIndex.begin() ;
          it != _hostIndex.end() ;
          ++it ) 
    {
        oss << it->show() << endl;
    }

    return oss.str();
}

pair<map<off_t,GlobalEntry>::iterator,bool> 
Index::insertGlobalEntry( const GlobalEntry *g_entry ) 
{
    return _globalIndex.insert(
               pair<off_t,GlobalEntry>( g_entry->logical_offset,
                                           *g_entry ) );
}

// readIndex reads only ONE physical index file
// The parameter physicalpath is the path of the index file
int
Index::readIndex(string physicalpath)
{
    off_t length = (off_t) -1;
    void *maddr = NULL;
    int fd;

    int ret = mapIndex(&maddr, physicalpath, &fd, &length);
    assert(ret != -1);
    //cout << "succesfully mapped" << endl;

    // read into memory
    HostEntry *h_index = (HostEntry *)maddr;
    size_t entries = length/sizeof(HostEntry);

    for (size_t i = 0; i < entries; ++i) {
        GlobalEntry g_entry;
        HostEntry h_entry = h_index[i];
        //cout << h_entry.show() << endl;
        g_entry.logical_offset = h_entry.logical_offset;
        g_entry.physical_offset = h_entry.physical_offset;
        g_entry.id = h_entry.id;
        g_entry.begin_timestamp = h_entry.begin_timestamp;
        g_entry.end_timestamp = h_entry.end_timestamp;
        g_entry.original_chunk = h_entry.id; //TODO: it is not right
        insertGlobalEntry(&g_entry);
    }

    //cout << "Entries in total:" << entries << endl;

    return 0;
}

// hostindex is a physical path
int
Index::mapIndex(void **ibufp, string physicalpath, int *xfd,
                off_t *length)
{
    *xfd = Util::Open(physicalpath.c_str(), O_RDONLY);
    if ( *xfd == -1 ) {
        *ibufp = NULL;
        *length = 0;
        return -1;
    }

    *length = Util::GetFileSize(*xfd);
    if (*length <= 0) {
        *ibufp = NULL;
        *length = 0;
        return -1;
    }

    //cout << "file size:" << *length << endl;
    
    *ibufp = Util::GetDataBuf(*xfd, *length); 
    return 0;
}

// Merge (insert) global entries of 'other'
// to this index
void
Index::merge(const Index *other)
{
    map<off_t, GlobalEntry>::const_iterator oe_it; // it for other entry
    const map<off_t, GlobalEntry> &og = other->_globalIndex;
    for (oe_it = og.begin(); oe_it != og.end(); ++oe_it) {
        insertGlobalEntry(& oe_it->second ); // TODO: chunk id is not handled
    }
    return;
}

void
Index::mergeEntries()
{
    vector<HostEntry> hent2;
    vector<HostEntry>::const_iterator cit;

    for ( cit = _hostIndex.begin() ;
          cit != _hostIndex.end() ;
          ++cit ) 
    {
        if ( !hent2.empty() &&
                hent2.back().id == cit->id &&
                hent2.back().logical_offset +
                (off_t)hent2.back().length == cit->logical_offset) 
        {
            hent2.back().end_timestamp = cit->end_timestamp;
            hent2.back().length += cit->length;
        } else {
            hent2.push_back(*cit);
        }
    }
    _hostIndex = hent2;
}


