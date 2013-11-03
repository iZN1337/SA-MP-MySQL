#pragma once

#include "main.h"
#include "CCallback.h"
#include "CMySQLHandle.h"
#include "CMySQLQuery.h"
#include "CMySQLResult.h"
#include "COrm.h"
#include "CLog.h"

#include "misc.h"
#include <cstdio>


boost::lockfree::queue<
		CMySQLQuery*, 
		boost::lockfree::fixed_sized<true>,
		boost::lockfree::capacity<8192>
	> CCallback::m_CallbackQueue;

list<AMX *> CCallback::m_AmxList;


void CCallback::ProcessCallbacks() 
{
	CMySQLQuery *Query = NULL;
	while( (Query = GetNextQuery()) != NULL) 
	{
		CCallback *Callback = Query->Callback;
		 
		if(Callback != NULL && (Callback->Name.length() > 0 || Query->OrmObject != NULL) ) 
		{
			if(Query->OrmObject != NULL) //orm, update the variables with the given result
			{
				switch(Query->OrmQueryType) 
				{
					case ORM_QUERYTYPE_SELECT:
						Query->OrmObject->ApplySelectResult(Query->Result);
						break;

					case ORM_QUERYTYPE_INSERT:
						Query->OrmObject->ApplyInsertResult(Query->Result);
						break;
				}
			}

			for (list<AMX *>::iterator a = m_AmxList.begin(), end = m_AmxList.end(); a != end; ++a) 
			{
				AMX *amx = (*a);
				cell amx_ret;
				int amx_index;
				cell amx_mem_addr = -1;

				if (amx_FindPublic(amx, Callback->Name.c_str(), &amx_index) == AMX_ERR_NONE) 
				{
					CLog::Get()->StartCallback(Callback->Name.c_str());

					while(!Callback->Parameters.empty())
					{
						cell tmpAddress = -1;
						boost::variant<cell, string> &param_value = Callback->Parameters.top();
						if(param_value.type() == typeid(cell))
						{
							if(Query->Callback->IsInline == false)
								amx_Push(amx, boost::get<cell>(param_value));
							else
								amx_PushArray(amx, &tmpAddress, NULL, static_cast<cell*>(&boost::get<cell>(param_value)), 1);
						}
						else
							amx_PushString(amx, &tmpAddress, NULL, boost::get<string>(param_value).c_str(), 0, 0);
						
						if(tmpAddress != -1 && amx_mem_addr < NULL)
							amx_mem_addr = tmpAddress;

						Callback->Parameters.pop();
					}


					Query->ConnHandle->SetActiveResult(Query->Result);
					Query->Result = NULL;
					CMySQLHandle::ActiveHandle = Query->ConnHandle;

					amx_Exec(amx, &amx_ret, amx_index);
					if (amx_mem_addr >= NULL)
						amx_Release(amx, amx_mem_addr);

					CMySQLHandle::ActiveHandle = NULL;

					if(Query->ConnHandle->IsActiveResultSaved() == false)
						Query->ConnHandle->GetActiveResult()->Destroy();

					Query->ConnHandle->SetActiveResult((CMySQLResult *)NULL);

					CLog::Get()->EndCallback();
					
					break; //we have found our callback, exit loop
				}
			}
		}
		Query->Destroy();
	}
}



void CCallback::AddAmx( AMX *amx ) 
{
	m_AmxList.push_back(amx);
}

void CCallback::EraseAmx( AMX *amx ) 
{
	for (list<AMX *>::iterator a = m_AmxList.begin(); a != m_AmxList.end(); ++a) 
	{
		if ( (*a) == amx) 
		{
			m_AmxList.erase(a);
			break;
		}
	}
}

void CCallback::ClearAll() {
	CMySQLQuery *query = NULL;
	while(m_CallbackQueue.pop(query))
		query->Destroy();
}

void CCallback::FillCallbackParams(AMX* amx, cell* params, const char *param_format, const int ConstParamCount) 
{
	if(param_format == NULL)
		return ;

	unsigned int param_idx = 1;

	do
	{
		cell *AddressPtr = NULL;
		char *StrBuf = NULL;

		switch(*param_format)
		{
			case 'i':
			case 'd':
			case 'f':
				amx_GetAddr(amx, params[ConstParamCount + param_idx++], &AddressPtr);
				Parameters.push(*AddressPtr);
				break;

			case 'z':
			case 's':
				amx_StrParam(amx, params[ConstParamCount + param_idx++], StrBuf);
				Parameters.push(StrBuf != NULL ? StrBuf : string());
				break;

			default:
				Parameters.push("NULL");
		} 

	} while(*(++param_format));
}
