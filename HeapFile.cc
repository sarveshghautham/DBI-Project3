#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"
#include "HeapFile.h"
#include <stdlib.h>

HeapFile::HeapFile() {}

int HeapFile::Close () {  
  return myFile.Close();
}

void HeapFile::Add (Record& addme) {
	off_t curPageNum=0;
    int ret=0;
    /* GetLength() always returns 1 greater than the number of pages in the file.
     * Add records to the end of the file. */
    //myPage.EmptyItOut();
    
    if (&addme == NULL)
    {
        cout<<"NULL";
        exit(-1);
    }
    curPageNum = myFile.GetLength();

    if (curPageNum == 0)
    {
        myPage.Append(&addme);
        myFile.AddPage(&myPage, curPageNum);
        //myPage.EmptyItOut();
    }
    else
    {
        //cout << "File not empty. Reading page " << curPageNum - 2 << endl;
        myFile.GetPage(&myPage, curPageNum-2);
        //cout << "Appending record to the page..." << endl;
        ret = myPage.Append(&addme);
        if (ret == 0)
        {
            //cout << "Page full...adding new page" << endl;
            myPage.EmptyItOut();
            myPage.Append(&addme);
            curPageNum++;
        }
        //cout << "Adding page..." << endl;
        myFile.AddPage(&myPage, curPageNum-2);
    }
}

int HeapFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
  ComparisonEngine comp;
	while(GetNext(fetchme))
	{
        /* The filtered records which matched the CNF are stored in fetchme */
        if (comp.Compare (&fetchme, &literal, &cnf))
        {	
            return 1;
        }
	}
	return 0;	
}
