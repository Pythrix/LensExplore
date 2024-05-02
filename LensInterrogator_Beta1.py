# %%
import inspect
import itertools
import json
import logging
import pandas as pd
from pandas import json_normalize
import re
import requests
import time


# %%
logger=logging.getLogger('lens_api')
logger.setLevel(logging.DEBUG)
#logger.setLevel(logging.WARNING)
fh=logging.FileHandler('ScriptLog.log')
fh.setLevel(logging.DEBUG)
#fh.setLevel(logging.WARNING)
log_form=logging.Formatter('%(levelname)-8s -- %(asctime)s -- %(message)s')
fh.setFormatter(log_form)
logger.addHandler(fh)

# %%
class Request_Lens:
    entries_default=["lens_id", "external_ids", "date_published_parts", "authors.last_name",
        "authors.first_name", "author_count", "title", "publication_type", "source","fields_of_study", 
        "abstract", "scholarly_citations_count", "references", "references_count"]
    

    def chunk(it, size):
        it = iter(it)
        return iter(lambda: tuple(itertools.islice(it, size)), ())
    
    
    
    def __init__(self,api_url,api_token, db_ids,api_method='infos',db_entries=entries_default):
        methods=['infos','citing']
        if api_method not in methods:
            raise ValueError("Invalid api_method. Expected one of: %s" % methods)
        self.url=api_url
        self.token=api_token
        self.ids=db_ids
        self.method=api_method
        self.entries=db_entries
        logger.info(f'\n\t\t Api urls is: {self.url}; \n\t\t Ids Looked is/are: {self.ids}; \n\t\t Method is: {self.method}; \n\t\t Retrieved entries are: {self.entries} \n')
    


    def display_args(self):
        print(f'\t\t--------- *** --------- \n Api urls is: {self.url}; \n Authorization token is: {self.token}; \n Ids Looked is/are: {self.ids}; \n Method is: {self.method}; \n Retrieved entries are: {self.entries} \n \t\t--------- *** ---------')
    
    
    def query_formater(self, start=1,size=1000):  
        def query_dict(api_ids,dict_keys,facet='term',subfacet='lens_id'):
            formated_query={}
            formated_query["query"]={facet:{subfacet:api_ids}}
            formated_query["include"]=dict_keys
            formated_query["size"]=1000
            formated_query["scroll"]= "1m"
            return formated_query
        
        if self.method == 'infos':
            if type(self.ids) is list and len(self.ids) >1:
                #logger.info(f"Infos method asked for {len(self.ids)} publications")
                return json.dumps(query_dict(self.ids,self.entries,'terms'))
            elif type(self.ids) is list and len(self.ids) == 1:
                #logger.info(f"Infos method asked for a single publication")
                return json.dumps(query_dict(self.ids[0],self.entries))
            else:
                #logger.info(f"Infos method asked for a single publication")
                return json.dumps(query_dict(self.ids,self.entries))
        else:
            if type(self.ids) is list :
                #logger.info("Value Error: Citing method were asked for a list of ids instead a single id")
                raise ValueError(f"Method Citing only work for one id at a time")
            else:
                citing_dict=query_dict(self.ids,self.entries,'match','reference.lens_id')
                del citing_dict['size']
                del citing_dict['scroll']
                citing_dict['from']=start
                citing_dict['size']=size

                return json.dumps(citing_dict)

    def get_apiresp(self,dataframe=False):
        
        
        def get_resp(url,auth,query, get_total=False):
            headers = {'Authorization': auth, 'Content-Type': 'application/json'}
            response = requests.post(url, data=query, headers=headers)
            if response.status_code == requests.codes.ok:
                myjson = response.json()
                logger.info(f'''API answeared.''')
                if get_total:
                    return myjson['total']
                else:
                    return myjson['data']
            else:
                myjson = response.json()
                logger.warning(f"Error chatting with API. Returned code: {myjson['code']}, meaning {myjson['message']}")
                raise ValueError(f"Error chatting with API. Returned code: {myjson['code']}, meaning {myjson['message']}")
        
        if self.method == 'infos':
            logger.info(f"Infos method asked")
            if type(self.ids) == list and len(self.ids) <=1000 or type(self.ids) == str:
               myquery=self.query_formater()
               api_results=get_resp(self.url,self.token,myquery)
               if dataframe:
                logger.info(f'''Obtained {len(api_results)} publication(s)''')
                return json_normalize(api_results)
               else:
                logger.info(f'''Obtained {len(api_results)} publication(s)''')
                return api_results


            elif type(self.ids) == list and len(self.ids) >= 1000:
                respres=[]
                sublists=list(chunk(self.ids,1000))
                sublists=[list(x) for x in sublists]
                for sub in sublists:
                    self.ids=sub
                    myquery=self.query_formater()
                    respres.append(get_resp(self.url,self.token,myquery))
                    time.sleep(3)
                return respres

        else:
            logger.info(f"Citing method asked")
            if type(self.ids) is list :
                logger.warning("Method Citing only work for one id at a time")
                raise ValueError(f"Method Citing only work for one id at a time")
            else:
                myquery=self.query_formater()
                nbpub=get_resp(self.url,self.token,myquery,True)
                logger.info(f"{nbpub} Publications to retrieve")
                if nbpub <= 1000:
                    if dataframe:
                        logger.info(f"Results asked as dataframe")
                        return json_normalize(get_resp(self.url,self.token,myquery))
                    else:
                        logger.info(f"Results asked as list of json files")
                        return get_resp(self.url,self.token,myquery)
                else:
                    #print(f'Total of {nbpub} to retrieve')
                    parts=[]
                    chunks=[range(nbpub)[i+1:i+1000] for i in range(0,len(range(nbpub)), 1000)]
                    for chk in chunks:
                        myquery=self.query_formater(chk[0],1000)
                        print(myquery)
                        results=get_resp(self.url,self.token,myquery)
                        parts.append(results)
                        time.sleep(3)
                    jsons_results=[ref for jsonlist in parts for ref in jsonlist]
                    if dataframe:
                        logger.info(f"Results asked as dataframe")
                        return json_normalize(jsons_results)
                    else:
                        logger.info(f"Results asked as list of json files")
                        return jsons_results
            #logger.info(f"Method Citing Publication not yet implemented")
            #raise ValueError(f"Method Citing Publication not yet implemented")
        #print(print(response.json()))
    
    def get_cited(self,dataframe=False):
        api_res=self.get_apiresp()
        all_refs=[pub['references'] for pub in api_res]
        all_refs=[pub for publist in all_refs for pub in publist]
        all_refs=list(dict.fromkeys([pub['lens_id'] for pub in all_refs]))
        self.ids=all_refs
        print(len(self.ids))
        return self.get_apiresp(dataframe)

        


# %%

print(inspect.getfullargspec(Request_Lens.__init__))

# %%
### entries can be modified, just choose one from the includ fields available with api
myentries=["lens_id","external_ids","date_published_parts","authors.last_name","authors.first_name","author_count"]
#### enter a lens_id list or on lens_id
myids=['here come and id list']
lens_obj=Request_Lens('https://api.lens.org/scholarly/search','HerecomesYourPersonnal token',myids,'infos')

# %%
#### resume the query sent to API, the token used and others additional infos
lens_obj.display_args()

# %%

lens_obj.query_formater()



