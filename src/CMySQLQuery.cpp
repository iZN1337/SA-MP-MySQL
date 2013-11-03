#pragma once

#include <cstdio>


#include "CMySQLQuery.h"
#include "CMySQLResult.h"
#include "CMySQLHandle.h"
#include "CCallback.h"
#include "COrm.h"
#include "CLog.h"

#include "misc.h"


CMySQLQuery::CMySQLQuery()  :
	Threaded(true),

	ConnHandle(NULL),
	Connection(NULL),
	Result(NULL),
	Callback(NULL),

	OrmObject(NULL),
	OrmQueryType(0)
{ 
	CLog::Get()->LogFunction(LOG_DEBUG, "CMySQLQuery::CMySQLQuery()", "constructor called");
}

CMySQLQuery::~CMySQLQuery() {
	delete Result;
	delete Callback;

	CLog::Get()->LogFunction(LOG_DEBUG, "CMySQLQuery::~CMySQLQuery()", "deconstructor called");
}

CMySQLQuery *CMySQLQuery::Create(
	const char *query, CMySQLHandle *connhandle, 
	const char *cbname,
	bool threaded /* = true */,
	COrm *ormobject /* = NULL */, unsigned short orm_querytype /* = 0 */)
{
	if(connhandle == NULL) 
	{
		CLog::Get()->LogFunction(LOG_ERROR, "CMySQLQuery::Create", "no connection handle specified");
		return static_cast<CMySQLQuery *>(NULL);
	}

	if(query == NULL && ormobject == NULL) 
	{
		CLog::Get()->LogFunction(LOG_ERROR, "CMySQLQuery::Create", "no query and orm object specified");
		return static_cast<CMySQLQuery *>(NULL);
	}
	

	CMySQLQuery *Query = new CMySQLQuery;
	CCallback *Callback = new CCallback;

	if(ormobject != NULL) 
	{
		CLog::Get()->LogFunction(LOG_DEBUG, "CMySQLQuery::Create", "starting query generation");
		switch(orm_querytype) 
		{
		case ORM_QUERYTYPE_SELECT:
			ormobject->GenerateSelectQuery(Query->Query);
			break;
		case ORM_QUERYTYPE_UPDATE:
			ormobject->GenerateUpdateQuery(Query->Query);
			break;
		case ORM_QUERYTYPE_INSERT:
			ormobject->GenerateInsertQuery(Query->Query);
			break;
		case ORM_QUERYTYPE_DELETE:
			ormobject->GenerateDeleteQuery(Query->Query);
			break;
		case ORM_QUERYTYPE_SAVE:
			orm_querytype = ormobject->GenerateSaveQuery(Query->Query);
		}

		CLog::Get()->LogFunction(LOG_DEBUG, "CMySQLQuery::Create", "query successful generated");
	}
	else 
	{
		if(query != NULL)
			Query->Query.assign(query);
	}

	if(cbname != NULL) 
		Callback->Name.assign(cbname);

	Query->Threaded = threaded;
	Query->ConnHandle = connhandle; 
	Query->Connection = threaded == true ? connhandle->GetQueryConnection() : connhandle->GetMainConnection(); 
	Query->Callback = Callback;
	Query->OrmObject = ormobject;
	Query->OrmQueryType = orm_querytype;

	if(Query->Callback->Name.find("FJ37DH3JG") != string::npos) 
	{
		Query->Callback->IsInline = true;
		CLog::Get()->LogFunction(LOG_DEBUG, "CMySQLQuery::Create", "inline function detected");
	}

	return Query;
}

void CMySQLQuery::Destroy() 
{
	delete this;
}

void CMySQLQuery::Execute() 
{
	char log_funcname[128];
	sprintf(log_funcname, "CMySQLQuery::Execute[%s]", Callback->Name.c_str());
	
	CLog::Get()->LogFunction(LOG_DEBUG, log_funcname, "starting query execution");

	Result = NULL;
	MYSQL *sql_connection = Connection->GetMySQLPointer();

	if(sql_connection != NULL) 
	{
		if (mysql_real_query(sql_connection, Query.c_str(), Query.length()) == 0) 
		{
			CLog::Get()->LogFunction(LOG_DEBUG, log_funcname, "query was successful");

			MYSQL_RES *sql_result = mysql_store_result(sql_connection); //this has to be here

			//why should we process the result if it won't and can't be used?
			if(Threaded == false || Callback->Name.length() > 0 || (OrmObject != NULL && (OrmQueryType == ORM_QUERYTYPE_SELECT || OrmQueryType == ORM_QUERYTYPE_INSERT))) 
			{ 
				if (sql_result != NULL) 
				{
					MYSQL_FIELD *sql_field;
					MYSQL_ROW sql_row;

					Result = new CMySQLResult;

					Result->m_WarningCount = mysql_warning_count(sql_connection);

					Result->m_Rows = mysql_num_rows(sql_result);
					Result->m_Fields = mysql_num_fields(sql_result);

					Result->m_Data.reserve((unsigned int)Result->m_Rows+1);
					Result->m_FieldNames.reserve(Result->m_Fields+1);


					while ((sql_field = mysql_fetch_field(sql_result)))
						Result->m_FieldNames.push_back(sql_field->name);
					
				
					while (sql_row = mysql_fetch_row(sql_result)) 
					{
						vector< vector<string> >::iterator It = Result->m_Data.insert(Result->m_Data.end(), vector<string>());
						It->reserve(Result->m_Fields+1);
					
						for (unsigned int a = 0; a < Result->m_Fields; ++a)
							It->push_back(!sql_row[a] ? "NULL" : sql_row[a]);
					}

				}
				else if(mysql_field_count(sql_connection) == 0) //query is non-SELECT query
				{
					Result = new CMySQLResult;
				
					Result->m_WarningCount = mysql_warning_count(sql_connection);
					Result->m_AffectedRows = mysql_affected_rows(sql_connection);
					Result->m_InsertID = mysql_insert_id(sql_connection); 
				}
				else //error
				{
					int ErrorID = mysql_errno(sql_connection);
					string ErrorString(mysql_error(sql_connection));

					CLog::Get()->LogFunction(LOG_ERROR, log_funcname, "an error occured while storing the result: (error #%d) \"%s\"", ErrorID, ErrorString.c_str());
					
					//we clear the callback name and forward it to the callback handler
					//the callback handler free's all memory but doesn't call the callback because there's no callback name
					Callback->Name.clear(); 
				}
			}
			else  //no callback was specified
				CLog::Get()->LogFunction(LOG_DEBUG, log_funcname, "no callback specified, skipping result saving");

			if(sql_result != NULL)
				mysql_free_result(sql_result);
		}
		else  //mysql_real_query failed
		{
			int ErrorID = mysql_errno(sql_connection);
			string ErrorString(mysql_error(sql_connection));

			CLog::Get()->LogFunction(LOG_ERROR, log_funcname, "(error #%d) %s", ErrorID, ErrorString.c_str());
			
			
			if(Connection->GetAutoReconnect() && ErrorID == 2006) 
			{
				CLog::Get()->LogFunction(LOG_WARNING, log_funcname, "lost connection, reconnecting..");

				MYSQL_RES *sql_result;
				if ((sql_result = mysql_store_result(sql_connection)) != NULL)
					mysql_free_result(sql_result);
				
				Connection->Disconnect();
				Connection->Connect();
			}

			if(Threaded == true) 
			{
				//forward OnQueryError(errorid, error[], callback[], query[], connectionHandle);
				//recycle these structures, change some data
				OrmObject = NULL;
				OrmQueryType = 0;

				while(Callback->Parameters.size() > 0)
					Callback->Parameters.pop();

				Callback->Parameters.push(static_cast<cell>(ErrorID));
				Callback->Parameters.push(ErrorString);
				Callback->Parameters.push(Callback->Name);
				Callback->Parameters.push(Query);
				Callback->Parameters.push(static_cast<cell>(ConnHandle->GetID()));

				Callback->Name = "OnQueryError";

				CLog::Get()->LogFunction(LOG_DEBUG, log_funcname, "error will be triggered in OnQueryError");
			}
		}
	}

	if(Threaded == true) 
	{
		//the query gets passed to the callback handler in any case
		//if query successful, it calls the callback and free's memory
		//if not it only free's the memory
		CLog::Get()->LogFunction(LOG_DEBUG, log_funcname, "data being passed to ProcessCallbacks()");
		CCallback::AddQueryToQueue(this);
	}
}
