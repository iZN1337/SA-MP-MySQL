#pragma once
#ifndef INC_CMYSQLQUERY_H
#define INC_CMYSQLQUERY_H


#include <string>
#include <stack>
#include <boost/variant.hpp>

using std::string;
using std::stack;

#include "main.h"


class CMySQLResult;
class CMySQLHandle;
class CMySQLConnection;
class CCallback;
class COrm;


class CMySQLQuery
{
public:
	static CMySQLQuery Create(
		string query, CMySQLConnection *connection,
		string cbname, stack<boost::variant<cell, string>> cbparams
	);


	string Query;

	CMySQLConnection *Connection;
	CMySQLResult *Result;
	struct s_Callback
	{
		stack<boost::variant<cell, string>> Params;
		string Name;
	} Callback;

	//COrm *OrmObject;
	//unsigned short OrmQueryType;
	CMySQLQuery() :
		Connection(NULL),
		Result(NULL)
	{}
	~CMySQLQuery();
	
};


#endif // INC_CMYSQLQUERY_H
