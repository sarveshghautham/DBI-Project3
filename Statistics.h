#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <map>
#include <string>

using namespace std;

typedef struct Relation
{
	int numTuples;
	//map<attrname, uniquecount>
	map<string, int> attributes;
}Relation;


class Statistics
{
	//set<Relname, Relation>
	map<string, Relation> RelationSet;

public:
	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();


	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);
	
	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

};

#endif
