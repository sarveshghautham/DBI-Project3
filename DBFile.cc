#include "DBFile.h"
#include "HeapFile.h"
#include "SortedFile.h"

//Begin DBFile methods
DBFile::DBFile()
{
	db = NULL;
}

DBFile::~DBFile()
{
	delete db; 
}

int DBFile::Create (char* fpath, fType ftype, void* startup) 
{
  if (db != NULL)
  {
       cerr << "File open error" << endl;
       exit(-1);
  }
  createDBInstance(ftype);
  return db->Create(fpath, startup);
}

int DBFile::Open (char* fpath) {

  if (db != NULL)
  {
       cerr << "File open error" << endl;
       exit(-1);
  }
  
  //cout << "Inside DBFile open" << endl;
  //Read meta file to obtain the type
  //TODO: Check whether this has to be changed dynamically or ineffective
  int ftype = heap; 
  string fileName(fpath);
  size_t end = fileName.find_last_of('.');
  fileName = fileName.substr(0, end);
  fileName = fileName+".meta";
  char metaFile[fileName.size()];
  strcpy(metaFile, fileName.c_str());
  FILE *fp = fopen(metaFile, "r");
  if (fp == NULL)
  {
      cout << "Error opening in metafile" << endl;
      exit(-1);
  }
  //Assign type if found
  char str[10];
  fscanf(fp, "%s", str);
  fclose(fp);
  ftype = atoi(str);
  //cout << "DBFILE ftype:"<<endl<<ftype;
  //Create the DBInstance of the type
  createDBInstance(static_cast<fType>(ftype));
  return db->Open(fpath);
}

int DBFile::Close() 
{
  int ret = db->Close();
  db = NULL;
  return ret;
}

void DBFile::Load (Schema& myschema, char* loadpath) 
{
  return db->Load(myschema, loadpath);
}

void DBFile::MoveFirst() 
{
  return db->MoveFirst();
}

void DBFile::Add (Record& addme) 
{
  return db->Add(addme);
}

int DBFile::GetNext (Record& fetchme) 
{
  return db->GetNext(fetchme);
}

int DBFile::GetNext (Record& fetchme, CNF& cnf, Record& literal) 
{ 
  return db->GetNext(fetchme, cnf, literal);
  cout <<"check"<<endl;
}
//End of DBFile methods


//Begin DBFileBase methods

DBFileBase::DBFileBase()
{
}

DBFileBase::~DBFileBase()
{
}

int DBFileBase::Create(char* fpath, void* startup) 
{
  myFile.Open(0, fpath);
  return 1;
}

int DBFileBase::Open (char* fpath) 
{
  myFile.Open(1, fpath);
  return 1;
}

void DBFileBase::Load (Schema& myschema, char* loadpath) 
{
 /* AddPage() takes in param, increments in AddPage() and GetLength() returns value incremented
   For example, passed as 0, in AddPage() increments and then adds as page 1(whichPage=1). GetVal returns whichPage+1 which is 2
 */
    off_t curPageNum=0;
    int retVal = 0;

    Record temp;
    bool firstRec = false;

    FILE *fp = NULL;
    fp = fopen (loadpath, "r");
    if(fp == NULL)
    {
        cerr << "Error opening file" << endl;
        exit(-1);
    }

    /* Suck each record and store it in pages */
    while (myRecord.SuckNextRecord(&myschema, fp) == 1)
    {
        retVal = myPage.Append(&myRecord);
        if (retVal == 0) 
        {
            if(!firstRec)
            {
                /* For the first page, GetLength() is 0. */
                curPageNum = myFile.GetLength();
            }
            else
            {
                /* For the rest of the pages, GetLength() is 1 greater. */
                curPageNum = myFile.GetLength() - 1;
            }
            /* When the page is full, write to the binary file */
            myFile.AddPage (&myPage, curPageNum);
            myPage.EmptyItOut();
            myPage.Append(&myRecord);
            firstRec = true;
        }
    }
    /* If a page is not full and there are no more records, write them out to a file and increment the page counter. */
    if(firstRec)
    {
        ++curPageNum;
    }
    myFile.AddPage (&myPage, curPageNum);
}

void DBFileBase::MoveFirst() 
{
  pageNo = 0;
  myFile.GetPage(&myPage, pageNo);
  if(myFile.GetLength() == 0)
  {
  	cerr << "In MoveFirst : File is empty" << endl;
  }
  myPage.GetFirst(&myRecord);
}

int DBFileBase::GetNext (Record& fetchme) 
{
	/*  1. If pageNo = 0, read one page from file and the totalPages = GetLength().
        2. When page is exhausted, increment pageNo and then read next page.
        3. When pageNo > 0, read the next page from file and the totalPages = GetLength() - 1.
        4. Stopping criteria for first page (pageNo = 0) is totalPages == 2 and pageNo == totalPages - 1
        5. Stopping criteria for pageNo > 0 is pageNo == totalPages -1 (default criteria; the 
     */
    off_t totalPages=0;
    totalPages = myFile.GetLength();
    if(pageNo == 0)
    {
        myFile.GetPage(&myPage, pageNo);
        pageNo++;
    }
    if (myPage.GetFirst(&fetchme) == 0)
    {
        myPage.EmptyItOut();
        if(totalPages == 2 && pageNo == totalPages-1)
        {
            return 0;
        }
        else if(pageNo == totalPages -1)
        {
            return 0;
        }
        myFile.GetPage(&myPage, pageNo);
        pageNo++;
        myPage.GetFirst(&fetchme);
    }
    return 1;
}

//Create a file type using fType to assign to the DBFileBase object
void DBFile::createDBInstance(fType ftype)
{
  switch (ftype) 
  {
	case heap  : db = new HeapFile; 
		          break;
	case sorted: db = new SortedFile; 
		          break;
	default    : cout << "Invalid file type" << endl;
		          exit(-1);
  }
}

//Returns the schema name ignoring the path
std::string DBFileBase::getTableName(const char* fpath) 
{
	std::string path(fpath);
	size_t start = path.find_last_of('/'),
		   end   = path.find_last_of('.');
	return path.substr(start+1, end-start-1);
}
//End DBFileBase methods
