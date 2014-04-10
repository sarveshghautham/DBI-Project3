#include "BigQ.h"

using namespace std;

/*
   Implementing TPMMS algorithm.
   Phase 1:
   -> The records are read from the input pipe and put in a vector until the input pipe is empty.
   -> The vector of records is divided into runs and each run is sorted.

   Phase 2:
   -> Perform run merging operations on the runs generated from phase 1.
   -> Initialize priority queue by adding the first record from each of the runs.
   -> Perform pop/remove minimum operations on the priority queue and add the next record on the priority queue based on the popped run number.
   -> Add the popped out records to the output pipe.
 */


BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortOrder, int runLength):
inputPipe(in),outputPipe(out),sortOrder(sortOrder),runLength(runLength)
{
    if (runLength == 0)
    {
        cerr <<"Run length is 0."<<endl;
        exit(-1);
    }
    //pthread_t bigQThread;
    pthread_create(&bigQThread, NULL, readRecordsFromPipe, (void *) this);
}

int GetRecordSize (Record* r)
{
    char* b = r->GetBits();
    int v = ((int *)b)[0];
    return v;
}

void * readRecordsFromPipe(void *arg)
{
    BigQ *obj = (BigQ *)arg;
    Pipe &in = obj->inputPipe;
    Pipe &out = obj->outputPipe;
    OrderMaker *sortOrder = &(obj->sortOrder);
    int runLength = obj->runLength;
    
    int runRecordCount[100] = {0};
    int j = 0, idxRun = 0, curSize = 0, totalRecordCount = 0, pageCount = 0;
    vector<Record *> records;
    bool phase1 = false;
    int totRunSize = PAGE_SIZE * runLength;
    vector<pair<int,int> >pageLimits; //stores start and end page numbers
    
    Record * temp = NULL;
    Page page;
    File f1;
    //f1.Open(0, "bq.bin");
	stringstream ss;
	ss<<rand();
	string ff = "bq_" + ss.str() + ".bin";
	char * fileNameStr = (char *)ff.c_str(); 

	f1.Open(0,fileNameStr);
    
    while(1)
    {   
        while(1)
        {
            temp = new(std::nothrow) Record;
            //Remove from pipe
            if (in.Remove(temp))
            {
                curSize += GetRecordSize(temp);
                //stop if total runlength achieved
                if(curSize > totRunSize)
                    break;
                //continue pushing the records to the list
                //handle the orphan record case at end
                records.push_back(temp);
                continue;
            }
            else
            {
                //Phase I ends
                phase1 = true;
                temp = NULL;
                break;
            }
        }
        
        totalRecordCount += records.size();
        
        //sort records
        sort(records.begin(),records.end(),recordCompare(sortOrder));
        
        //Add the records to the pages
        int startPageNum = pageCount;
        for(int i =0 ; i<records.size(); i++)
        {
            //full page does not consume the record
            if(!page.Append(records[i]))
            {
                //cout << "Adding new page" << pageCount+1 << endl;
                f1.AddPage(&page,pageCount);
                page.EmptyItOut();
                page.Append(records[i]);
                pageCount++;
            }
        }
        //write incomplete pages; if any
        f1.AddPage(&page,pageCount);
        //store the start and end page numbers
        pageLimits.push_back(make_pair(startPageNum,pageCount));
        pageCount++;
        page.EmptyItOut();
        records.clear();
        curSize = 0;
        
        //Move to phase II if done
        if(phase1)
            break;
        
        //Add orphan record
        records.push_back(temp);
        curSize += GetRecordSize(temp);
    }
    
    f1.Close();
    
    //Phase II
    //construct priority queue over sorted runs and dump sorted data into the out pipe
    priority_queue<pair<Record *,int>,vector<pair<Record *,int> >, recordComparePQ >pqRecords(sortOrder);
    
    //open f1 and get the right page numbers in memory
    File f2;
    f2.Open(1, fileNameStr);
    int numPagesInFile = f2.GetLength();
    //total number of runs
    int runCount = pageLimits.size();
    int numRecordsInCurrentRun = 0 ;
    int curPageCount[runCount], i = 0;
    
    //the page number that is needed to be brought in memory for merging for each run index.
    Page pagesNeeded[runCount];
    //create records for each run to be allocated for priority queue
    Record tempRec[runCount];
    
	//Initialize priority queue
    for(i=0;i<runCount;i++)
    {
        int startPageNum = pageLimits[i].first;
        f2.GetPage(&pagesNeeded[i],startPageNum);
        pagesNeeded[i].GetFirst(&tempRec[i]);
        //Insert pair into priority queue {record, runNo}
        pqRecords.push(make_pair(&tempRec[i],i));
        curPageCount[i] = startPageNum;
    }
	int id =0;
    int retVal = 0;
    //int outRecCount = 0;
    //Operations on priority queue
    //Removemin
    while(!pqRecords.empty())
    {
        id++;
        //Priority queue records min value, runlength
        Record * record = pqRecords.top().first;
        int runNo = pqRecords.top().second;
        //Removemin and insert into pipe
        //cout << "Popped out run " << runNo << endl;
        pqRecords.pop();
	//outRecCount++;
        out.Insert(record);
        j = runNo;
        
        retVal = pagesNeeded[j].GetFirst(&tempRec[j]);
        
        if(retVal == 1)
        {
            pqRecords.push(make_pair(&tempRec[j],j));
        }
        else
        {    
            int tempPageNo = curPageCount[runNo];
            ++tempPageNo;
            //insert the record from the popped out run number
            if((tempPageNo >= numPagesInFile-1) || tempPageNo > pageLimits[runNo].second) 
            {
                continue;
            }
            curPageCount[j] = tempPageNo;
            
            //load the next page in memory for current Run
            f2.GetPage(&pagesNeeded[j],tempPageNo);
            //push record from next page
            pagesNeeded[j].GetFirst(&tempRec[j]);
            pqRecords.push(make_pair(&tempRec[j],j));
            
        }
    }
    //cout << "OutRec count: " << outRecCount << endl;
    f2.Close();
    out.ShutDown();
    return NULL;
}

BigQ::~BigQ () {
}
