#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <tr1/unordered_map>
#include <tr1/unordered_set>
#include <fstream>
#include <string>
#include <vector>


using namespace std;
using namespace std::tr1;

class Relation_info;
typedef unsigned long long big_number;
typedef unordered_map < string, big_number > str_to_bign_number_map;
typedef unordered_map < string, vector <string> > str_to_strs_map;
typedef unordered_map < string, string > str_to_str_map;
typedef unordered_map < string, double > str_to_double;
typedef unordered_map < string, Relation_info > str_to_rel_info_map;
class Statistics;


class Relation_info
{
private:
	friend class Statistics;
	big_number num_tuples;
	unordered_map < string, big_number > attribute_information;	
	string relation_name;

public:
	Relation_info(string rel_name_in, big_number tuple_count);
	Relation_info(string rel_name_in, Relation_info &rel_info_in, Statistics *Stat_in);
	Relation_info();
	~Relation_info();
	void AddAttr(string attribute_name, big_number count_distinct);
	friend ostream& operator<<(ostream &os, const Relation_info &rel_info_in);
	friend istream& operator>>(ostream &is, const Relation_info &rel_info_in);
};


class Statistics
{
private:
	friend class Relation_info;
	str_to_rel_info_map relation_map;
	str_to_strs_map join_map;
	str_to_str_map att_rel_map;
	

	void CheckRelNameParseTree ( struct AndList *, char **, int );
public:
	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();

	void AddRel(char *relName, int numTuples);
	void Print();
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);

	void Read(char *fromWhere);
	void Write(char *fromWhere);

	big_number ReadAtt(string aName);
	void WriteAtt(string aName, double ratio);

	void  Apply(struct AndList *parseTree, char **relNames, int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin, bool apply);
};



#endif