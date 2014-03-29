#include "RelOp.h"

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
   
    /* Box parameters in a struct and call the thread routine */ 
//    SelectFromPipeParams spParams = {&inPipe, &outPipe, &selOp, &literal};
    spParams = new SelectFromPipeParams;
    spParams->inPipe = &inPipe;
    spParams->outPipe = &outPipe;
    spParams->selOp = &selOp;
    spParams->literal = &literal;
       
    /* Create thread */
    pthread_create(&SelectPipeThread, NULL, SelectFromPipe, (void *) &spParams);
}

void * SelectFromPipe (void *spParams) {
    
    SelectFromPipeParams *sp = (SelectFromPipeParams *)spParams;
    
    Record temp;
    ComparisonEngine cmp;

    while ((sp->inPipe)->Remove(&temp)) {
        if (cmp.Compare(&temp, sp->literal, sp->selOp)) {
            (sp->outPipe)->Insert(&temp);
        }
    }
    (sp->outPipe)->ShutDown();
}

void SelectPipe::WaitUntilDone() {

    pthread_join(SelectPipeThread, NULL);
    delete spParams;
}

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {

//Record& fetchme, CNF& cnf, Record& literal
   
    /* Box parameters in a struct and call the thread routine */ 

    //SelectFromFileParams sfParams = {&inFile, &outPipe, &selOp, &literal};
    sfParams = new SelectFromFileParams;
    sfParams->inFile = &inFile;
    sfParams->outPipe = &outPipe;
    sfParams->selOp = &selOp;
    sfParams->literal = &literal;

    /* Create thread */
    pthread_create(&SelectFileThread, NULL, SelectFromFile, (void *)sfParams);
}

void * SelectFromFile (void *sfParams) {
  
    SelectFromFileParams *sf = (SelectFromFileParams *)sfParams;
   
    if (sf->inFile == NULL || sf->outPipe == NULL || sf->selOp == NULL || sf->literal == NULL) {
        cout << "NULL exception at SelectFromFile!!!" <<endl;
        exit(-1);
    }
    
    Record temp;
    ComparisonEngine cmp;
    DBFile *inFile = sf->inFile;

    inFile->MoveFirst();

    while (inFile->GetNext(temp, *(sf->selOp), *(sf->literal))) {
        (sf->outPipe)->Insert(&temp);
    }

    (sf->outPipe)->ShutDown();
}

void SelectFile::WaitUntilDone() {

    pthread_join(SelectFileThread, NULL);
    //cout <<""<<endl;
    //delete sfParams;
}

void SelectFile::Use_n_Pages (int runlen) {
}

void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {

    pParams = new ProjectParams;
	pParams->inPipe = &inPipe;
	pParams->outPipe = &outPipe;
	pParams->keepMe = keepMe;
	pParams->numAttsInput = numAttsInput;
	pParams->numAttsOutput = numAttsOutput;
	
    pthread_create(&ProjectThread, NULL, ProjectRoutine, (void *) pParams);

}

void * ProjectRoutine (void *pParams) {

    ProjectParams *pp = (ProjectParams *)pParams;
    Record temp;

	if (pp->inPipe == NULL || pp->outPipe == NULL || pp->keepMe == NULL || pp->numAttsInput == NULL || pp->numAttsOutput == NULL) {
        cout << "NULL exception at ProjectRoutine!!!" <<endl;
        exit(-1);
    }
	int _recCount = 0;
    while ((pp->inPipe)->Remove(&temp)) {
		cout << "Record " << ++_recCount << endl;
        temp.Project (pp->keepMe, pp->numAttsOutput, pp->numAttsInput);
        (pp->outPipe)->Insert(&temp);
    }
    (pp->outPipe)->ShutDown();

}

void Project::WaitUntilDone() {
    pthread_join (ProjectThread, NULL);
    delete pParams;
}

void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema ) {
   
    /* Box parameters in a struct and call the thread routine */ 
//    SelectFromPipeParams spParams = {&inPipe, &outPipe, &selOp, &literal};
    drParams = new DuplicateRemovalParams;
    drParams->inPipe = &inPipe;
    drParams->outPipe = &outPipe;
    drParams->mySchema = &mySchema;   

    /* Create thread */
    pthread_create(&DuplicateRemovalThread, NULL, DuplicateRemovalRoutine, (void *) &drParams);
}

void * DuplicateRemovalRoutine (void *drParams) {
    
    DuplicateRemovalParams *dr = (DuplicateRemovalParams *)drParams;
    Pipe tempOutPipe(100);
    OrderMaker sortOrder (dr->mySchema);
    
    BigQ bq(*(dr->inPipe), tempOutPipe, sortOrder, 4); 
    
    Record temp1;
    Record temp2;
    ComparisonEngine cmp;
    bool firstRec = true;

    while (tempOutPipe.Remove (&temp1)) {
        if (firstRec) {
            (dr->outPipe)->Insert(&temp1);
            firstRec = false;
            temp2.Copy(&temp1);
        }
        else {
            if (cmp.Compare (&temp1, &temp2, &sortOrder)) {
                (dr->outPipe)->Insert(&temp1);
                temp2.Copy(&temp1);
            }
        }
    }

    (dr->outPipe)->ShutDown();
}

void DuplicateRemoval::WaitUntilDone() {

    pthread_join(DuplicateRemovalThread, NULL);
    delete drParams;
}

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) {

    woParams = new WriteOutParams;
    woParams->inPipe = &inPipe;
    woParams->outFile = outFile;
    woParams->mySchema =  &mySchema;

    pthread_create (&WriteOutThread, NULL, WriteOutToFile, (void *) &woParams);
}

void * WriteOutToFile (void *woParams) {
    
    WriteOutParams *wo = (WriteOutParams *)woParams;
    Record temp;

    while ((wo->inPipe)->Remove(&temp)) {
        temp.PrintToFile(wo->mySchema, wo->outFile);
    }
}

void WriteOut::WaitUntilDone() {

    pthread_join(WriteOutThread, NULL);
    delete woParams;

}
