#include "SortedFile.h"
#include "HeapFile.h"

extern char* catalog_path;
extern char* dbfile_dir; 
extern char* tpch_dir;

//Initialize SortedFile mode
SortedFile::SortedFile()
{
	fMode = READ;
	myOrder = NULL;
	runLength = 0;
	in = NULL;
	out = NULL;
	biq = NULL;
	myOrderAlloc = false;
}

SortedFile::~SortedFile()
{
}

//Set the file to READ mode. Perform operations for the leftover records and destroy BigQ
void SortedFile::setRead() 
{
  //cout << "Setting write mode" << endl;
  if (fMode==READ) 
	return;
  fMode = READ;
  merge();
  destroyBigQ();
}

//Set the file to WRITE mode and initialize BigQ
void SortedFile::setWrite() 
{
  //cout << "Setting write mode" << endl;
  if (fMode==WRITE) 
	return;
  fMode = WRITE;
  initBigQ();
}

//Initialize BigQ
void SortedFile::initBigQ()
{
  in = new Pipe(PIPE_BUFFER_SIZE);
  out = new Pipe(PIPE_BUFFER_SIZE);
  biq = new BigQ(*in, *out, *myOrder, runLength);
}

//Destroy BigQ
void SortedFile::destroyBigQ()
{
  delete biq; 
  delete in; 
  delete out;
  biq = NULL; 
  in = NULL;
  out = NULL;
}

//Create the sorted file and perform misc operations like reading the runlength, ordermaker from the structure
int SortedFile::Create (char* fpath, void* startup) 
{
  //cout << "Inside create" << endl;
  tpath = fpath;
  table = getTableName(tpath);
  pOrder po = (pOrder)startup;
  myOrder = po -> omObj;
  runLength = po -> rLen;
  return DBFileBase::Create(fpath, startup);
}

//Open the sorted file and read the meta data to facilitate open
int SortedFile::Open (char* fpath) 
{
  if (myOrder != NULL)
  {
      cerr << "File opened"<<endl;
      exit(-1);
  }
  myOrder = new OrderMaker();
  myOrderAlloc = true;
  
  //Meta file operations
  tpath = fpath;
  table = getTableName(tpath);
  int ftype;
  ifstream ifs(metafName());
  if (!ifs)
  {
      cout << "Meta file missing" << endl;
      exit(-1);
  }
  ifs >> ftype >> *myOrder >> runLength;
  ifs.close();
  
  return DBFileBase::Open(fpath);
}

//Close the sorted file and write the meta data
int SortedFile::Close() 
{
  //Meta file operations
  ofstream ofs(metafName());
  ofs << "1\n" << *myOrder << '\n' << runLength << endl;
  ofs.close();
  if(fMode==WRITE) 
	merge();
  if (myOrderAlloc) 
  {
	delete myOrder; 
	myOrder = NULL;
  }
  myOrderAlloc = false;
  return myFile.Close();
}

//Set the mode to read and then MoveFirst
void SortedFile::MoveFirst () 
{
  setRead();
  DBFileBase::MoveFirst();
}

//Set the mode to write and then Add
void SortedFile::Add (Record& addme) 
{
  setWrite();
  in->Insert(&addme);
}

//Set the mode to write and then Load
void SortedFile::Load (Schema& myschema, char* loadpath) 
{
  setWrite();
  DBFileBase::Load(myschema, loadpath);
}

//GetNext from the sorted file
int SortedFile::GetNext (Record& fetchme) 
{
  return DBFileBase::GetNext(fetchme);
}

//GetNext from the sorted file using the CNF. Perform binary search and comparison to obtain the value
int SortedFile::GetNext (Record& fetchme, CNF& cnf, Record& literal) 
{
  OrderMaker queryOrderMaker, cnfOrderMaker;
  //Form the query OrderMaker object
  buildQuery(*myOrder, cnf, queryOrderMaker, cnfOrderMaker);
  ComparisonEngine cmp;
  //Start the binary search operation
  if (!binarySearch(fetchme, queryOrderMaker, literal, cnfOrderMaker, cmp)) 
	return 0;
  do 
  {
    if (cmp.Compare(&fetchme, &queryOrderMaker, &literal, &cnfOrderMaker)) 
	{
		//cout << "Not a match in GetNext2 " << endl;
		return 0;
	}
    if (cmp.Compare(&fetchme, &literal, &cnf)) 
	{
		//cout << "Success in GetNext2 " << endl;
		return 1;
	}
  }while(GetNext(fetchme));
  //If the value not found, return 0
  return 0;
}

//Builds the query ordermaker object
void SortedFile::buildQuery(const OrderMaker& sortOrder, const CNF& query, OrderMaker& queryOrderMaker, OrderMaker& cnfOrderMaker)
{
  //cout << "Inside buildQuery" <<endl;
  queryOrderMaker.numAtts = 0;
  cnfOrderMaker.numAtts = 0;
  for (int i=0; i < sortOrder.numAtts; ++i) 
  {               
	typeof(sortOrder.whichTypes[i]) & att = sortOrder.whichTypes[i];           
	typeof(sortOrder.whichTypes[i]) & type = sortOrder.whichTypes[i];
    int i = findAttIdx(att, query);
    if (i>=0) 
	{     
	  //cout << "Found attr" <<endl;
      queryOrderMaker.whichAtts[queryOrderMaker.numAtts] = att;
	  queryOrderMaker.whichTypes[queryOrderMaker.numAtts] = type;
      cnfOrderMaker.whichAtts[cnfOrderMaker.numAtts] = i;
	  cnfOrderMaker.whichTypes[cnfOrderMaker.numAtts] = type;
      ++queryOrderMaker.numAtts, ++cnfOrderMaker.numAtts;
    } 
	else 
		return;
  }
}

//Finds the index of attr in CNF; -1 if not found
int SortedFile::findAttIdx(int att, const CNF& query) 
{
  for (size_t i=0; i<(query.numAnds); ++i) 
  {               
	  typeof(query.orList[i]) & clause = query.orList[i];           
	  typeof(query.orLens[i]) & orLen = query.orLens[i];
	  if (orLen==1) 
	  {
		const Comparison& cmp = clause[0];
		if (cmp.op == Equals && (((cmp.whichAtt1==att) && (cmp.operand2==Literal)) ||(cmp.whichAtt2==att) && (cmp.operand1==Literal)))
		{
		  //cout << "findAttIdx found! " << i << endl;
		  return i;
		}
	  }
  }
  return -1;
}

//Merge routine
void SortedFile::merge() 
{
  /*
	1. Shutdown the input pipe.
	2. Create a temporary file to append.
	3. Compare the file and the pipe data using Compare(,,). Add the necessary result to the file.
	4. Ensure that when either record runs out, add the remaining record.
  */
  in->ShutDown();
  Record fromFile, fromPipe;
  ComparisonEngine ce;
  HeapFile tmp;
  char tempFName[strlen(tpath)];
  bool fileNotEmpty = !(myFile.GetLength()==0), pipeNotEmpty = out->Remove(&fromPipe);
  strcpy(tempFName, tpath);
  strcat(tempFName,".tmp");
  tmp.Create(tempFName, NULL);
  //Move to the first page if file not empty
  if (fileNotEmpty) 
  {
    //cout << "Created file" << endl;
    DBFileBase::MoveFirst();
    fileNotEmpty = GetNext(fromFile);
  }

  //Merge logic to compare and write to the tmp file
  while (fileNotEmpty || pipeNotEmpty)
    if (!fileNotEmpty || (pipeNotEmpty && ce.Compare(&fromFile, &fromPipe, myOrder) > 0)) 
	{
      tmp.Add(fromPipe);
      pipeNotEmpty = out->Remove(&fromPipe);
    } 
	else if (!pipeNotEmpty || (fileNotEmpty && ce.Compare(&fromFile, &fromPipe, myOrder) <= 0)) 
	{
      tmp.Add(fromFile);
      fileNotEmpty = GetNext(fromFile);
    } 
    else 
	{
        cout << "Two-way merge failed"<<endl;
        exit(-1);
    }
  tmp.Close();
  if (rename(tempFName, tpath))
  {
      cout << "Merge write failed" <<endl;
      exit(-1);
  }
}

//Binary search for the particular page and then linear search
int SortedFile::binarySearch(Record& fetchme, OrderMaker& queryOrderMaker, Record& literal, OrderMaker& cnfOrderMaker, ComparisonEngine& cmp) 
{
  /* 1. If the sorted order does not match, quit and return.
	 2. Perform binary search to find the range/page where the record belongs.
	 3. Perform linear scan to find the record.
  */

  //Edge cases to check if the sorted order matches; else return.
  if (!GetNext(fetchme)) 
	return 0;
  int result = cmp.Compare(&fetchme, &queryOrderMaker, &literal, &cnfOrderMaker);
  if (result > 0) 
	return 0;
  else if (result == 0) 
	return 1;

  //Binary search logic
  off_t t = myFile.GetLength();
  off_t low=pageNo;
  off_t high=(t>0)?t-2:0;
  off_t mid=(low+high)/2;
  for (; low<mid; mid=(low+high)/2) 
  {
    myFile.GetPage(&myPage, mid);
    if (!GetNext(fetchme))
    {
        cerr << "Error in Binary search while fetching page" << endl;
        exit(-1);
    }
    result = cmp.Compare(&fetchme, &queryOrderMaker, &literal, &cnfOrderMaker);
    if (result<0) 
	{
		//cout << "Moving to right" << mid << " " << high << endl;
		low = mid;
	}
    else if (result>0)
	{
		//cout << "Moving to left" << left << " " << mid-1 << endl;
		high = mid-1;
	}
    else 
	{
		//cout << "Found!!!" << endl;
		high = mid;
	}
  }

  //cout << "Page number" << low << endl;
  //Linear scan logic
  myFile.GetPage(&myPage, low);
  do 
  {   
    if (!GetNext(fetchme)) 
		return 0;
    result = cmp.Compare(&fetchme, &queryOrderMaker, &literal, &cnfOrderMaker);
  }while (result<0);
  return result==0;
}

const char* SortedFile::metafName() const 
{
  string p(dbfile_dir);
  return (p+table+".meta").c_str();
}
