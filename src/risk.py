"""
    Steve L. Nyemba & Brad Malin
    Health Information Privacy Lab.

    This code is proof of concept as to how risk is computed against a database (at least a schema).
    The engine will read tables that have a given criteria (patient id) and generate a dataset by performing joins.
    Because joins are process intensive we decided to add a limit to the records pulled.
    
    TL;DR:
        This engine generates a dataset and computes risk (marketer and prosecutor)    
    Assumptions:
        - We assume tables that reference patients will name the keys identically (best practice). This allows us to be able to leverage data store's that don't support referential integrity
        
    Usage :
        
    Limitations
        - It works against bigquery for now
        @TODO:    
            - Need to write a transport layer (database interface)
            - Support for referential integrity, so one table can be selected and a dataset derived given referential integrity
            - Add support for journalist risk
"""
import pandas as pd
import numpy as np
from google.cloud import bigquery as bq
import time
from params import SYS_ARGS
class utils :
    """
        This class is a utility class that will generate SQL-11 compatible code in order to run the risk assessment
        
        @TODO: plugins for other data-stores
    """
    def __init__(self,**args):
        # self.path = args['path']
        self.client = args['client']
    
    def get_tables(self,**args): #id,key='person_id'):
        """
            This function returns a list of tables given a key. The key is the name of the field that uniquely designates a patient/person
            in the database. The list of tables are tables that can be joined given the provided field.

            @param key  name of the patient field
            @param dataset   dataset name
            @param client   initialized bigquery client ()
            @return [{name,fields:[],row_count}]
        """
        dataset = args['dataset']
        client  = args['client']
        key     = args['key']
        r = []
        ref = client.dataset(dataset)
        tables = list(client.list_tables(ref))
        TERMS = ['type','unit','count','refills','stop','supply','quantity']
        for table in tables :
            
            if table.table_id.strip() in ['people_seed','measurement','drug_exposure','procedure_occurrence','visit_occurrence','condition_occurrence','device_exposure']:
                print ' skiping ...'
                continue
            ref = table.reference
            table = client.get_table(ref)
            schema = table.schema
            rows = table.num_rows
            if rows == 0 :
                continue
            
            names = [f.name for f in schema if len (set(TERMS) & set(f.name.strip().split("_"))) == 0 ]
            
            x = list(set(names) & set([key]))
            if x  :
                full_name = ".".join([dataset,table.table_id])
                r.append({"name":table.table_id,"fields":names,"row_count":rows,"full_name":full_name})
        return r
    
    def get_field_name(self,alias,field_name,index):
        """
            This function will format the a field name given an index (the number of times it has occurred in projection)
            The index is intended to avoid a "duplicate field" error (bigquery issue)

            @param alias    alias of the table
            @param field_name   name of the field to be formatted
            @param index    the number of times the field appears in the projection
        """
        name = [alias,field_name]
        if index > 0 :
            return ".".join(name)+" AS :field_name:index".replace(":field_name",field_name).replace(":index",str(index))
        else:
            return ".".join(name)
    def get_filtered_table(self,table,key):
        """
            This function will return a table with a single record per individual patient
        """
        return """
            
            SELECT :table.* FROM (
                SELECT row_number() over () as top, * FROM :full_name ) as :table
                
            
            INNER JOIN (
            SELECT MAX(top) as top, :key FROM ( 
                SELECT row_number() over () as top,:key from :full_name ) GROUP BY :key
                
            )as filter
            ON filter.top = :table.top and filter.:key = :table.:key 

        """.replace(":key",key).replace(":full_name",table['full_name']).replace(":table",table['name'])
    
    def get_sql(self,**args):
        """
            This function will generate that will join a list of tables given a key and a limit of records
            @param tables   list of tables
            @param  key     key field to be used in the join. The assumption is that the field name is identical across tables (best practice!)
            @param limit    a limit imposed, in case of ristrictions considering joins are resource intensive
        """
        tables  = args['tables'] 
        key     = args['key']
        limit   = args['limit'] if 'limit' in args else 10000
        limit   = str(limit) 
        SQL = [
            """ 
            SELECT :fields 
            FROM 
            """]
        fields = []
        prev_table = None
        for table in tables :
            name = table['full_name'] #".".join([self.i_dataset,table['name']])
            alias= table['name']
            index = tables.index(table)
            sql_ = """ 
                (select * from :name ) as :alias
            """.replace(":limit",limit)
            # sql_ = " ".join(["(",self.get_filtered_table(table,key)," ) as :alias"])
            sql_ = sql_.replace(":name",name).replace(":alias",alias).replace(":limit",limit)
            fields += [self.get_field_name(alias,field_name,index) for field_name in table['fields'] if field_name != key or  (field_name==key and  tables.index(table) == 0) ]
            if tables.index(table) > 0 :
                join = """
                    INNER JOIN :sql ON :alias.:field = :prev_alias.:field
                """.replace(":name",name)
                join = join.replace(":alias",alias).replace(":field",key).replace(":prev_alias",prev_alias)
                sql_ = join.replace(":sql",sql_)
                # sql_ = " ".join([sql_,join])
            SQL += [sql_]
            if index == 0:
                prev_alias = str(alias)
        
        return " ".join(SQL).replace(":fields"," , ".join(fields))

class SQLRisk :
    """
        This class will handle the creation of an SQL query that computes marketer and prosecutor risk (for now)
    """
    def __init__(self):
        pass
    def get_sql(self,**args) :
        """
            This function returns the SQL Query that will compute marketer and prosecutor risk
            @param key      key fields (patient identifier)
            @param table    table that is subject of the computation
        """
        key     = args['key']
        table   = args['table']
        fields  = list(set(table['fields']) - set([key]))
        #-- We need to select n-fields max 64
        k = len(fields)        
        if 'field_count' in args :
            n   = np.random.randint(2, int(args['field_count']) ) #-- number of random fields we are picking
        else:
            n = np.random.randint(2,k)  #-- how many random fields are we processing
        ii = np.random.choice(k,n,replace=False)
        stream = np.zeros(len(fields) + 1) 
        stream[ii] = 1
        stream = pd.DataFrame(stream.tolist()).T
        stream.columns = args['table']['fields']
        fields = list(np.array(fields)[ii])

        sql = """
            SELECT COUNT(g_size) as group_count,SUM(g_size) as patient_count, COUNT(g_size)/SUM(g_size) as marketer, 1/ MIN(g_size) as prosecutor, :n as field_count
            FROM (
                SELECT COUNT(*) as g_size,:fields
                FROM :full_name
                GROUP BY :fields
            )
        """.replace(":fields", ",".join(fields)).replace(":full_name",table['full_name']).replace(":key",key).replace(":n",str(n))
        return {"sql":sql,"stream":stream}
    

        
class UtilHandler :
    def __init__(self,**args) :
        """
            @param path path to the service account file
            @param dataset    input dataset name
            @param key_field    key_field (e.g person_id)
            @param key_table

        """
        self.path        = args['path']
        self.client      = bq.Client.from_service_account_json(self.path)
        dataset   = args['dataset']
        self.key         = args['key_field'] 

        self.mytools = utils(client = self.client)
        self.tables  = self.mytools.get_tables(dataset=dataset,client=self.client,key=self.key)
        index = [ self.tables.index(item) for item in self.tables if item['name'] == args['key_table']] [0]
        if index != 0 :
            first = self.tables[0]
            aux = self.tables[index]
            self.tables[0] = aux
            self.tables[index] = first
        if 'filter' in args :
            self.tables = [item for item in self.tables if item['name'] in args['filter']]


    def create_table(self,**args):
        """
            @param path absolute filename to save the create statement

        """
        create_sql = self.mytools.get_sql(tables=self.tables,key=self.key) #-- The create statement
        # o_dataset = SYS_ARGS['o_dataset']
        # table = SYS_ARGS['table']
        if 'path' in args:
            f = open(args['path'],'w')
            f.write(create_sql)
            f.close()
        return create_sql
    def migrate_tables(self,**args):
        """
            This function will migrate a table from one location to another
            The reason for migration is to be able to reduce a candidate table to only represent a patient by her quasi-identifiers.
            @param dataset  target dataset
        """
        o_dataset = args['dataset'] if 'dataset' in args else None
        p = []
        for table in self.tables:
            sql = " ".join(["SELECT ",",".join(table['fields']) ," FROM (",self.mytools.get_filtered_table(table,self.key),") as ",table['name']])        
            p.append(sql)
            if o_dataset :
                job = bq.QueryJobConfig()
                job.destination = self.client.dataset(o_dataset).table(table['name'])
                job.use_query_cache = True
                job.allow_large_results = True 
                job.priority = 'INTERACTIVE'
                job.time_partitioning = bq.table.TimePartitioning(type_=bq.table.TimePartitioningType.DAY)

                r = self.client.query(sql,location='US',job_config=job) 

                print [table['full_name'],' ** ',r.job_id,' ** ',r.state]
        return p

# if 'action' in SYS_ARGS and  SYS_ARGS['action'] in ['create','compute','migrate'] :

#     path = SYS_ARGS['path']
#     client = bq.Client.from_service_account_json(path)
#     i_dataset = SYS_ARGS['i_dataset']
#     key = SYS_ARGS['key'] 

#     mytools = utils(client = client)
#     tables = mytools.get_tables(dataset=i_dataset,client=client,key=key)
#     # print len(tables)
#     # tables = tables[:6]

#     if SYS_ARGS['action'] == 'create' :
#         #usage:
#         #   create --i_dataset <in dataset> --key <patient id> --o_dataset <out dataset> --table <table|file> [--file] --path <bq JSON account file>
#         #
#         create_sql = mytools.get_sql(tables=tables,key=key) #-- The create statement
#         o_dataset = SYS_ARGS['o_dataset']
#         table = SYS_ARGS['table']
#         if 'file' in SYS_ARGS :
#             f = open(table+'.sql','w')
#             f.write(create_sql)
#             f.close()
#         else:
#             job = bq.QueryJobConfig()
#             job.destination = client.dataset(o_dataset).table(table)
#             job.use_query_cache = True
#             job.allow_large_results = True 
#             job.priority = 'BATCH'
#             job.time_partitioning = bq.table.TimePartitioning(type_=bq.table.TimePartitioningType.DAY)

#             r = client.query(create_sql,location='US',job_config=job) 
            
#             print [r.job_id,' ** ',r.state]
#     elif SYS_ARGS['action'] == 'migrate' :
#         #
#         #

#         o_dataset = SYS_ARGS['o_dataset']
#         for table in tables:
#             sql = " ".join(["SELECT ",",".join(table['fields']) ," FROM (",mytools.get_filtered_table(table,key),") as ",table['name']])
#             print ""
#             print sql
#             print ""
#             # job = bq.QueryJobConfig()
#             # job.destination = client.dataset(o_dataset).table(table['name'])
#             # job.use_query_cache = True
#             # job.allow_large_results = True 
#             # job.priority = 'INTERACTIVE'
#             # job.time_partitioning = bq.table.TimePartitioning(type_=bq.table.TimePartitioningType.DAY)

#             # r = client.query(sql,location='US',job_config=job) 
            
#             # print [table['full_name'],' ** ',r.job_id,' ** ',r.state]


#         pass
#     else:
#         #
#         #
#         tables  = [tab for tab in tables if tab['name'] == SYS_ARGS['table'] ]  
#         limit   = int(SYS_ARGS['limit']) if 'limit' in SYS_ARGS else 1
#         if tables :            
#             risk= risk()
#             df  = pd.DataFrame()
#             dfs = pd.DataFrame()
#             np.random.seed(1)
#             for i in range(0,limit) :
#                 r = risk.get_sql(key=SYS_ARGS['key'],table=tables[0])
#                 sql = r['sql']
#                 dfs = dfs.append(r['stream'],sort=True)
#                 df = df.append(pd.read_gbq(query=sql,private_key=path,dialect='standard').join(dfs))
#                 # df = df.join(dfs,sort=True)
#                 df.to_csv(SYS_ARGS['table']+'.csv')
#                 # dfs.to_csv(SYS_ARGS['table']+'_stream.csv') 
#                 print [i,' ** ',df.shape[0],pd.DataFrame(r['stream']).shape]
#                 time.sleep(2)
                
    
# else:
#     print 'ERROR'
#     pass

# # r = risk(path='/home/steve/dev/google-cloud-sdk/accounts/vumc-test.json', i_dataset='raw',o_dataset='risk_o',o_table='mo')
# # tables = r.get_tables('raw','person_id')
# # sql = r.get_sql(tables=tables[:3],key='person_id')
# # #
# # # let's post this to a designated location
# # #
# # f = open('foo.sql','w')
# # f.write(sql)
# # f.close()
# # r.get_sql(tables=tables,key='person_id')
# # p = r.compute()
# # print p
# # p.to_csv("risk.csv")
# # r.write('foo.sql')
