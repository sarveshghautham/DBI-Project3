#include "RelOp.h"

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
   
    /* Box parameters in a struct and call the thread routine */ 
    SelectFromPipeParams spParams = {&inPipe, &outPipe, &selOp, &literal};
    /* Create thread */
    SelectPipe sp;
    pthread_create(&(sp.SelectPipeThread), NULL, SelectFromPipe, (void *) &spParams);
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

    SelectPipe sp;
    pthread_join(sp.SelectPipeThread, NULL);

}

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {

//Record& fetchme, CNF& cnf, Record& literal
   
    /* Box parameters in a struct and call the thread routine */ 

    //SelectFromFileParams sfParams = {&inFile, &outPipe, &selOp, &literal};
    SelectFromFileParams *sfParams = new SelectFromFileParams;
    sfParams->inFile = &inFile;
    sfParams->outPipe = &outPipe;
    sfParams->selOp = &selOp;
    sfParams->literal = &literal;

    /* Ctrate thread */
    SelectFile sf;
    pthread_create(&(sf.SelectFileThread), NULL, SelectFromFile, (void *)sfParams);
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

    pthread_join(this->SelectFileThread, NULL);

}

void SelectFile::Use_n_Pages (int runlen) {
}

void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {

    ProjectParams *pParams = new ProjectParams;
	pParams->inPipe = &inPipe;
	pParams->outPipe = &outPipe;
	pParams->keepMe = keepMe;
	pParams->numAttsInput = numAttsInput;
	pParams->numAttsOutput = numAttsOutput;
	
    Project p;
    pthread_create(&(p.ProjectThread), NULL, ProjectRoutine, (void *) pParams);

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
    pthread_join (this->ProjectThread, NULL);
}

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) {

    WriteOutParams woParams = {&inPipe, outFile, &mySchema};

    WriteOut wo;
    pthread_create (&(wo.WriteOutThread), NULL, WriteOutToFile, (void *) &woParams);
}

void * WriteOutToFile (void *woParams) {
    
    WriteOutParams *wo = (WriteOutParams *)woParams;
    Record temp;

    while ((wo->inPipe)->Remove(&temp)) {

        temp.Print(wo->mySchema, wo->outFile);
    }
}

void WriteOut::WaitUntilDone() {

    WriteOut wo;
    pthread_join(wo.WriteOutThread, NULL);

}









