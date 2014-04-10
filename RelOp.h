#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "BigQ.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"

<<<<<<< HEAD
=======
typedef struct {
    Pipe *inPipe;
    Pipe *outPipe;
    CNF *selOp;
    Record *literal;
}SelectFromPipeParams;

typedef struct {
    DBFile *inFile;
    Pipe *outPipe;
    CNF *selOp;
    Record *literal;
}SelectFromFileParams;

typedef struct {
    Pipe *inPipe;
    Pipe *outPipe;
    Schema *mySchema;
}DuplicateRemovalParams;

typedef struct {
    Pipe *inPipe;
    FILE *outFile;
    Schema *mySchema;
}WriteOutParams;

typedef struct {
    Pipe *inPipe;
    Pipe *outPipe;
    int *keepMe;
    int numAttsInput;
    int numAttsOutput;
}ProjectParams;

void * SelectFromPipe (void *);
void * SelectFromFile (void *);
void * ProjectRoutine (void *);
void * DuplicateRemovalRoutine (void *);
void * WriteOutToFile (void *);

>>>>>>> 20da8dd0dad71eab0825773117f7a6e752492852
class RelationalOp {
	public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp { 

	private:
	// pthread_t thread;
	// Record *buffer;

	public:

	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);

};

class SelectPipe : public RelationalOp {
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) { }
	void WaitUntilDone () { }
	void Use_n_Pages (int n) { }
};
class Project : public RelationalOp { 
	public:
<<<<<<< HEAD
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) { }
	void WaitUntilDone () { }
=======
    ProjectParams *pParams;
    pthread_t ProjectThread; 
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void WaitUntilDone ();
>>>>>>> 20da8dd0dad71eab0825773117f7a6e752492852
	void Use_n_Pages (int n) { }
};
class Join : public RelationalOp { 
	public:
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) { }
	void WaitUntilDone () { }
	void Use_n_Pages (int n) { }
};
class DuplicateRemoval : public RelationalOp {
    friend void * DuplicateRemovalRoutine (void *);
	public:
    DuplicateRemovalParams *drParams;
    pthread_t DuplicateRemovalThread;
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n) { }
};
class Sum : public RelationalOp {
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) { }
	void WaitUntilDone () { }
	void Use_n_Pages (int n) { }
};
class GroupBy : public RelationalOp {
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) { }
	void WaitUntilDone () { }
	void Use_n_Pages (int n) { }
};
class WriteOut : public RelationalOp {
	public:
<<<<<<< HEAD
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) { }
	void WaitUntilDone () { }
=======
    WriteOutParams *woParams;
    pthread_t WriteOutThread;

	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void WaitUntilDone (); 
>>>>>>> 20da8dd0dad71eab0825773117f7a6e752492852
	void Use_n_Pages (int n) { }
};
#endif
