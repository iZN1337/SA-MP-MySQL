#pragma once

#include "CLog.h"

#include "CMySQLHandle.h"
#include "CMySQLResult.h"
#include "CMySQLQuery.h"
#include "CCallback.h"

#include <chrono>


unordered_map<int, CMySQLHandle *> CMySQLHandle::SQLHandle;
CMySQLHandle *CMySQLHandle::ActiveHandle = nullptr;
CMySQLOptions MySQLOptions;


CMySQLHandle::CMySQLHandle(int id) : 
	m_MyID(id),
	
	m_ActiveResult(nullptr),
	m_ActiveResultID(0),
	
	m_MainConnection(nullptr),

	m_QueryCounter(0),
	m_QueryThreadRunning(true),
	m_QueryStashThread(std::bind(&CMySQLHandle::ExecThreadStashFunc, this))
{
	CLog::Get()->LogFunction(LOG_DEBUG, "CMySQLHandle::CMySQLHandle", "constructor called");
}

CMySQLHandle::~CMySQLHandle() 
{
	for (unordered_map<int, CMySQLResult*>::iterator it = m_SavedResults.begin(), end = m_SavedResults.end(); it != end; it++)
		delete it->second;

	m_MainConnection->Destroy();
	ExecuteOnConnectionPool(&CMySQLConnection::Destroy);

	m_QueryThreadRunning = false;
	m_QueryStashThread.join();

	CLog::Get()->LogFunction(LOG_DEBUG, "CMySQLHandle::~CMySQLHandle", "deconstructor called");
}

void CMySQLHandle::WaitForQueryExec() 
{
	while (!m_QueryQueue.empty())
		std::this_thread::sleep_for(std::chrono::milliseconds(5));
}

CMySQLHandle *CMySQLHandle::Create(string host, string user, string pass, string db, size_t port, size_t pool_size, bool reconnect) 
{
	CMySQLHandle *handle = NULL;
	CMySQLConnection *main_connection = CMySQLConnection::Create(host, user, pass, db, port, reconnect);
	if (MySQLOptions.DuplicateConnections == false && SQLHandle.size() > 0) {
		//code used for checking duplicate connections
		for(unordered_map<int, CMySQLHandle*>::iterator i = SQLHandle.begin(), end = SQLHandle.end(); i != end; ++i) {
			CMySQLConnection *Connection = i->second->m_MainConnection;
			if((*Connection) == (*main_connection))
			{
				CLog::Get()->LogFunction(LOG_WARNING, "CMySQLHandle::Create", "connection already exists");
				handle = i->second;
				break;
			}
		}
	}
	if(handle == NULL) {
			CLog::Get()->LogFunction(LOG_DEBUG, "CMySQLHandle::Create", "creating new connection..");

		int id = 1;
		if(SQLHandle.size() > 0) 
		{
			unordered_map<int, CMySQLHandle*>::iterator itHandle = SQLHandle.begin();
			do 
			{
				id = itHandle->first+1;
				++itHandle;
			} while(SQLHandle.find(id) != SQLHandle.end());
		}


		handle = new CMySQLHandle(id);

		//init connections
		handle->m_MainConnection = main_connection;
	for (size_t i = 0; i < pool_size; ++i)
		handle->m_ConnectionPool.push_front(CMySQLConnection::Create(host, user, pass, db, port, reconnect));

		SQLHandle.insert( unordered_map<int, CMySQLHandle*>::value_type(id, handle) );
	CLog::Get()->LogFunction(LOG_DEBUG, "CMySQLHandle::Create", "connection created with id = %d", id);
	}
	return handle;
}

void CMySQLHandle::Destroy() 
{
	SQLHandle.erase(m_MyID);
	delete this;
}

void CMySQLHandle::ExecuteOnConnectionPool(void(CMySQLConnection::*func)())
{
	for (auto &t : m_ConnectionPool)
		(t->*func)();
}

int CMySQLHandle::SaveActiveResult() 
{
	if(m_ActiveResult != nullptr) 
	{
		if(m_ActiveResultID != 0) //if active cache was already saved
		{
			CLog::Get()->LogFunction(LOG_WARNING, "CMySQLHandle::SaveActiveResult", "active cache was already saved");
			return m_ActiveResultID; //return the id of already saved cache
		}
		else 
		{
			int id = 1;
			if(!m_SavedResults.empty()) 
			{
				unordered_map<int, CMySQLResult*>::iterator itHandle = m_SavedResults.begin();
				do 
				{
					id = itHandle->first+1;
					++itHandle;
				} 
				while(m_SavedResults.find(id) != m_SavedResults.end());
			}

			m_ActiveResultID = id;
			m_SavedResults.insert( std::map<int, CMySQLResult*>::value_type(id, m_ActiveResult) );
			
			CLog::Get()->LogFunction(LOG_DEBUG, "CMySQLHandle::SaveActiveResult", "cache saved with id = %d", id);
			return id; 
		}
	}
	
	return 0;
}

bool CMySQLHandle::DeleteSavedResult(int resultid) 
{
	if(resultid > 0) 
	{
		if(m_SavedResults.find(resultid) != m_SavedResults.end()) 
		{
			CMySQLResult *ResultHandle = m_SavedResults.at(resultid);
			if(m_ActiveResult == ResultHandle) 
			{
				m_ActiveResult = nullptr;
				m_ActiveResultID = 0;
			}
			delete ResultHandle;
			m_SavedResults.erase(resultid);
			CLog::Get()->LogFunction(LOG_DEBUG, "CMySQLHandle::DeleteSavedResult", "result deleted");
			return true;
		}
	}
	
	CLog::Get()->LogFunction(LOG_WARNING, "CMySQLHandle::DeleteSavedResult", "invalid result id ('%d')", resultid);
	return false;
}

bool CMySQLHandle::SetActiveResult(int resultid) 
{
	if(resultid > 0) 
	{
		if(m_SavedResults.find(resultid) != m_SavedResults.end()) 
		{
			CMySQLResult *cResult = m_SavedResults.at(resultid);
			if(cResult != NULL) 
			{
				if(m_ActiveResult != NULL)
					if (m_ActiveResultID == 0) //if cache not saved
						delete m_ActiveResult; //delete unsaved cache
				
				m_ActiveResult = cResult; //set new active cache
				m_ActiveResultID = resultid; //new active cache was stored previously
				ActiveHandle = this;
				CLog::Get()->LogFunction(LOG_DEBUG, "CMySQLHandle::SetActiveResult", "result is now active");
			}
		}
		else
			CLog::Get()->LogFunction(LOG_ERROR, "CMySQLHandle::SetActiveResult", "result not found");
	}
	else 
	{
		if (m_ActiveResultID == 0) //if cache not saved
			delete m_ActiveResult; //delete unsaved cache
		m_ActiveResult = nullptr;
		m_ActiveResultID = 0;
		ActiveHandle = nullptr;
		CLog::Get()->LogFunction(LOG_DEBUG, "CMySQLHandle::SetActiveResult", "invalid result id specified, setting active result to zero");
	}
	return true;
}

void CMySQLHandle::ClearAll()
{
	for(auto i = SQLHandle.begin(); i != SQLHandle.end(); ++i)
		i->second->Destroy();
	
	SQLHandle.clear();
}

void CMySQLHandle::SetActiveResult(CMySQLResult *result)
{
	m_ActiveResult = result;
	m_ActiveResultID = 0;
}


void CMySQLHandle::ExecThreadStashFunc()
{
	m_QueryThreadRunning = true;
	while (m_QueryThreadRunning)
	{
		while (!m_QueryQueue.empty())
		{
			function<CMySQLQuery(CMySQLConnection*)> QueryFunc (std::move(m_QueryQueue.front()));
			m_QueryQueue.pop();
			bool func_executed = false;
			do
			{
				for (auto &c : m_ConnectionPool)
				{
					CMySQLConnection *connection = c;
					if (connection->GetState() == false)
					{
						connection->ToggleState(true);

						std::future<CMySQLQuery> fut = std::async(std::launch::async, QueryFunc, connection);
						CCallback::AddQueryToQueue(std::move(fut), this);
						func_executed = true;
						m_QueryCounter++;
						break;
					}
				}
				std::this_thread::sleep_for(std::chrono::milliseconds(10));
			} 
			while (func_executed == false);
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
}



CMySQLConnection *CMySQLConnection::Create(string &host, string &user, string &passwd, string &db, unsigned int port, bool auto_reconnect)
{
	return new CMySQLConnection(host, user, passwd, db, port, auto_reconnect);
}

void CMySQLConnection::Destroy()
{
	delete this;
}

void CMySQLConnection::Connect() 
{
	if(m_Connection == NULL) 
	{
		m_Connection = mysql_init(NULL);
		if (m_Connection == NULL)
			CLog::Get()->LogFunction(LOG_ERROR, "CMySQLConnection::Connect", "MySQL initialization failed");
	}

	if (!m_IsConnected && !mysql_real_connect(m_Connection, m_Host.c_str(), m_User.c_str(), m_Passw.c_str(), m_Database.c_str(), m_Port, NULL, NULL)) 
	{
		CLog::Get()->LogFunction(LOG_ERROR, "CMySQLConnection::Connect", "(error #%d) %s", mysql_errno(m_Connection), mysql_error(m_Connection));

		m_IsConnected = false;
	} 
	else 
	{
		CLog::Get()->LogFunction(LOG_DEBUG, "CMySQLConnection::Connect", "connection was successful");

		my_bool reconnect = m_AutoReconnect;
		mysql_options(m_Connection, MYSQL_OPT_RECONNECT, &reconnect);
		CLog::Get()->LogFunction(LOG_DEBUG, "CMySQLConnection::Connect", "auto-reconnect has been %s", m_AutoReconnect == true ? "enabled" : "disabled");
		
		m_IsConnected = true;
	}
}

void CMySQLConnection::Disconnect() 
{
	if (m_Connection == NULL)
		CLog::Get()->LogFunction(LOG_WARNING, "CMySQLConnection::Disconnect", "no connection available");
	else 
	{
		mysql_close(m_Connection);
		m_Connection = NULL;
		m_IsConnected = false;
		CLog::Get()->LogFunction(LOG_DEBUG, "CMySQLConnection::Disconnect", "connection was closed");
	}
}

void CMySQLConnection::EscapeString(const char *src, string &dest)
{
	if(src != NULL && m_IsConnected) 
	{
		size_t SrcLen = strlen(src);
		char *tmpEscapedStr = (char *)malloc((SrcLen*2 + 1) * sizeof(char));

		size_t EscapedLen = mysql_real_escape_string(m_Connection, tmpEscapedStr, src, SrcLen);
		dest.assign(tmpEscapedStr);

		free(tmpEscapedStr);
	}
}
