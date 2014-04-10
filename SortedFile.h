#ifndef SORTED_FILE_H_
#define SORTED_FILE_H_

#include <fstream>
#include <string>
#include <stdlib.h>

#include "DBFile.h"
#include "Pipe.h"
#include "BigQ.h"

using namespace std;


/** Meta file format, each item separated by a newline
   *  1 filetype (0,1,2)
   *  2 OrderMaker[numAtts\twhichAtts\twhichTypes]
   *  3 runlength
**/
class SortedFile: protected DBFileBase {
  static const size_t PIPE_BUFFER_SIZE = 128;
  friend class DBFile;
  using DBFileBase::GetNext;

protected:
  SortedFile();
  ~SortedFile();

  int Create (char* fpath, void* startup);
  int Open (char* fpath);
  int Close ();
  void Load (Schema& myschema, char* loadpath);
  void MoveFirst();
  void Add (Record& addme);
  int GetNext (Record& fetchme);
  int GetNext (Record& fetchme, CNF& cnf, Record& literal);

private:
  Pipe *in, *out;
  BigQ *biq;
  enum Mode { READ, WRITE } fMode;
  typedef struct { OrderMaker* omObj; int rLen; } *pOrder;
  OrderMaker* myOrder;    
  int runLength;
  const char* tpath;
  string table;
  //this is used to indicate whether SortInfo is passed or created; default false and set in allocMem()
  bool myOrderAlloc;     

  //Binary search of the records
  int binarySearch(Record& fetchme, OrderMaker& queryOrderMaker, Record& literal, OrderMaker& cnfOrderMaker, ComparisonEngine& cmp);
  //construct a query order to answer query based on the sort information.
  static void buildQuery(const OrderMaker& sortOrder, const CNF& query, OrderMaker& queryOrderMaker, OrderMaker& cnfOrderMaker);
  //find an attribute in the given CNF, -1 if not found
  static int findAttIdx(int att, const CNF& query);   
  
  //Sets the SortedFile to write mode and then facilities to initialize BigQ
  void setWrite();
  //Sets the SortedFile to read mode and then destroys BigQ
  void setRead();
  //Initialize BigQ
  void initBigQ();
  //Destroy BigQ
  void destroyBigQ();
  //merge BigQ and File
  void merge();    
  //meta file name
  const char* metafName() const; 
  SortedFile(const SortedFile&);
  SortedFile& operator=(const SortedFile&);  
};
#endif
