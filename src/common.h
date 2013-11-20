//
// This header file defines all data structures used
// for communicating between processes. 

class PutRequest {
    // process rank_from should send some data
	// to process rank_to.
	// Specifically, the data to be sent is 
	// at from_offset of process rank_from. It
	// should be sent to process rank_to, and be
	// saved at to_offset
    public:
		int rank_from; 
		int rank_to;
		
		//need 64bits for large file
		long long from_offset; 		
		long long from_length;
		long long to_offset;
};

class GetRequest {
	public:
		// This process needs someone to 
		// send it some data so it can fill
		// offset, length
		// But this process has no idea
		// who has this piece of data
		long long offset;
		long long length;
};






