#include "RelOp.h"

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
   
    /* Box parameters in a struct and call the thread routine */ 
    //SelectFromPipeParams spParams = {&inPipe, &outPipe, &selOp, &literal};
    
    SelectFromPipeParams *spParams = new SelectFromPipeParams;
    spParams->inPipe = &inPipe;
    spParams->outPipe = &outPipe;
    spParams->selOp = &selOp;
    spParams->literal = &literal;
    
    /* Ctrate thread */
    SelectPipe sp;
    pthread_create(&(sp.SelectPipeThread), NULL, SelectFromPipe, (void *)spParams);
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

    /* Ctrate thread */
    SelectFile sf;
    pthread_create(&(sf.SelectFileThread), NULL, SelectFromFile, (void *)sfParams);
}

void * SelectFromFile (void *sfParams) {
  
    SelectFromFileParams *sf = (SelectFromFileParams *)sfParams;
    DBFile *inFile = sf->inFile;
   
    if (inFile == NULL || sf->outPipe == NULL || sf->selOp == NULL || sf->literal == NULL) {
        cout << "something's wrong" <<endl;
        exit(-1);
    }
    
    Record temp;
    ComparisonEngine cmp;

    inFile->MoveFirst();

    while (inFile->GetNext(temp, *(sf->selOp), *(sf->literal))) {
        (sf->outPipe)->Insert(&temp);
    }

    (sf->outPipe)->ShutDown();
}

void SelectFile::WaitUntilDone() {

    pthread_join(SelectFileThread, NULL);
    //Throws seg fault. commenting for now. makes inFile NULL.
 //   delete sfParams; 
}

void SelectFile::Use_n_Pages (int runlen) {
}

void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {

    ProjectParams pParams = {&inPipe, &outPipe, keepMe, numAttsInput, numAttsOutput};
    Project p;
    pthread_create(&(p.ProjectThread), NULL, ProjectRoutine, (void *) &pParams);

}

void * ProjectRoutine (void *pParams) {

    ProjectParams *pp = (ProjectParams *)pParams;
    Record temp;

    while ((pp->inPipe)->Remove(&temp)) {
        
        temp.Project (pp->keepMe, pp->numAttsOutput, pp->numAttsInput);
        (pp->outPipe)->Insert(&temp);
    }
    (pp->outPipe)->ShutDown();

}

void Project::WaitUntilDone() {
    Project p;
    pthread_join (p.ProjectThread, NULL);
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
