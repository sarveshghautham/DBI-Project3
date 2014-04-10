#include <iostream>
#include <fstream>
#include <sstream>
#include "Statistics.h"

Statistics::Statistics()
{
}
Statistics::Statistics(Statistics &copyMe)
{
}
Statistics::~Statistics()
{
}

void Statistics::AddRel(char *relName, int numTuples)
{
	Relation t;
	t.numTuples = numTuples;
	string relnm(relName);
	RelationSet.insert(make_pair(relnm, t));
}
void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
	Relation tempRel;
	string t_RelName(relName);
	tempRel = RelationSet[t_RelName];
	string t_AttName(attName);
	(tempRel.attributes).insert(make_pair(t_AttName, numDistincts));
	RelationSet[t_RelName] = tempRel;
}
void Statistics::CopyRel(char *oldName, char *newName)
{
	string s_newName(newName), s_oldName(oldName);
	Relation tempRel = RelationSet[s_oldName];
	Relation newRel;
	newRel.numTuples = tempRel.numTuples;
	map<string, int>::iterator it;
	for(it = (tempRel.attributes).begin(); it != (tempRel.attributes).end(); it++)
	{
		(newRel.attributes).insert(make_pair(it->first, it->second));
	}
}
	
void Statistics::Read(char *fromWhere)
{
	ifstream in(fromWhere);
	string t;
	while(getline(in, t))
	{
		istringstream relIss(t);
		Relation tempRel;
		string relName;
		int numTuples;
		relIss >> relName;
		relIss >> numTuples;
		string line1, line2;
		getline(in, line1);
		getline(in, line2);
		istringstream attrLine(line1);
		istringstream dcntLine(line2);
		map<string, int> attrs;
		do
		{
		   string attr;
		   int dcnt;
		   if(!(attrLine >> attr))
			break;
		   dcntLine >> dcnt;
		   attrs.insert(make_pair(attr, dcnt));			
		}while(attrLine);
		tempRel.attributes = attrs;
		RelationSet.insert(make_pair(relName, tempRel));
	}
}

void Statistics::Write(char *fromWhere)
{
	ofstream out(fromWhere);
	map<string, Relation>::iterator relIterator;
	for(relIterator = RelationSet.begin(); relIterator != RelationSet.end(); relIterator++)
	{
		out << relIterator->first << " " << (relIterator->second).numTuples << endl;
		map<string, int>::iterator attrIterator;
		for(attrIterator = ((relIterator->second).attributes).begin(); attrIterator != ((relIterator->second).attributes).end(); attrIterator++)
			out << attrIterator->first << " ";
		out << endl;
		for(attrIterator = ((relIterator->second).attributes).begin(); attrIterator != ((relIterator->second).attributes).end(); attrIterator++)
			out << attrIterator -> second << " ";
	}	
	out.close();
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
}
double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
}

