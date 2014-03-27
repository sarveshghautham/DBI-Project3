#ifndef DBFILE_H
#define DBFILE_H
#include <iostream>
#include <string.h>
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

using namespace std;

//Type of file
typedef enum {heap, sorted, tree} fType;

class DBFileBase;

/* DBFile main class will have instances of Class File, Page and Record. 
 * pageNo is maintained to track the last page written to the File. */
class DBFile 
{
public:
  DBFile();
  ~DBFile();
  //Creates a new file and opens it reading
  int Create (char *fpath, fType file_type, void *startup);
  //Opens an already created file
  int Open (char *fpath);
  //Closes the file
  int Close ();
  //Loads all the records from a .tbl file to a .bin file
  void Load (Schema &myschema, char *loadpath);
  //Sets the pageNo = 0 and gets the first page
  void MoveFirst ();
  //Adds a new record to the page and adds a new page to the end of File
  void Add (Record &addme);
  //Scans the records in a file
  int GetNext (Record &fetchme);
  //Scans the records which matches the CNF condition
  int GetNext (Record &fetchme, CNF &cnf, Record &literal);

private:
  DBFileBase* db;
  void createDBInstance(fType ftype);
  DBFile(const DBFile&);
  DBFile& operator=(const DBFile&);
};


/* DBFileBase is the virtual base class that inherits from the DBFile class to
 * abstract its properties such that the Sorted/Heap file implementations are 
 * carried out through this class. Refer to DBFile comments for additional information.*/
class DBFileBase 
{
  friend class DBFile;
  
public:
  off_t pageNo;
  Page myPage;
  File myFile;
  Record myRecord;
  
  DBFileBase();
  virtual ~DBFileBase();
  virtual int Create (char* fpath, void* startup);
  virtual int Open (char* fpath);
  virtual int Close() = 0;
  virtual void Load (Schema& myschema, char* loadpath);
  virtual void MoveFirst ();
  virtual void Add (Record& addme) = 0;
  virtual int GetNext (Record& fetchme);
  virtual int GetNext (Record& fetchme, CNF& cnf, Record& literal) = 0;
  //Return the schema name ignoring the path
  static std::string getTableName(const char*) ;

private:
  DBFileBase(const DBFileBase&);
  DBFileBase& operator=(const DBFileBase&);
};

#endif
