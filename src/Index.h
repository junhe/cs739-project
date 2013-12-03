#ifndef __Index_H__
#define __Index_H__

#include <string>
#include <map>
#include <vector>
#include <utility>

class HostEntry
{
    public:
        off_t  logical_offset;
        off_t  physical_offset;
        size_t length;
        double begin_timestamp;
        double end_timestamp;
        pid_t  id;      // needs to be last so no padding

        std::string show() const;
        static bool compareLogical_offset(
                            const HostEntry &a, const HostEntry &b);
        
        friend class Index;
};

class GlobalEntry: public HostEntry
{
    public:
        pid_t original_chunk;
    
    friend class Index;
}; 



// An Index can only be flushed to one file
// It can hold indices of many pid's. 
class Index {
    public:
        std::vector< HostEntry > _hostIndex;
        std::map< off_t, GlobalEntry > _globalIndex;
        std::string _physical_path; // the physical path of the index file of this index
        int    _index_fd;      // the handle of the index file
        std::map <pid_t, off_t> _physical_offsets;
        bool _compress_contiguous;
       

        Index(const char *physical_path, bool compress_contiguous=false);
        Index(bool compress_contiguous=false); // use this one if you don't want a file open for this index
        ~Index();
        
        void addEntry( const HostEntry &entry );
        void addWrite( off_t offset, size_t length, pid_t pid,
                       double begin_timestamp, double end_timestamp );
        int flush();
        void sortEntries();
        void mergeEntries();
        std::string show() const;
        std::pair<std::map<off_t,GlobalEntry>::iterator,bool> insertGlobalEntry(
                            const GlobalEntry *g_entry);
        int readIndex(std::string physicalpath);
        int mapIndex(void **ibufp, std::string hostindex, int *xfd,
                off_t *length);
        void merge(const Index *other);
};

#endif

