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
        for table in tables :
            
            if table.table_id.strip() in ['people_seed']:
                print ' skiping ...'
                continue
            ref = table.reference
            table = client.get_table(ref)
            schema = table.schema
            rows = table.num_rows
            if rows == 0 :
                continue
            names = [f.name for f in schema]
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
    def get_sql(self,**args):
        """
            This function will generate that will join a list of tables given a key and a limit of records
            @param tables   list of tables
            @param  key     key field to be used in the join. The assumption is that the field name is identical across tables (best practice!)
            @param limit    a limit imposed, in case of ristrictions considering joins are resource intensive
        """
        tables  = args['tables'] 
        key     = args['key']
        limit   = args['limit'] if 'limit' in args else 300000
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
                (select * from :name limit :limit) as :alias
            """.replace(":limit",limit)
            sql_ = sql_.replace(":name",name).replace(":alias",alias)
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

class risk :
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
        n = np.random.randint(2,24)  #-- how many random fields are we processing
        ii = np.random.choice(k,n,replace=False)
        fields = list(np.array(fields)[ii])

        sql = """
            SELECT COUNT(g_size) as group_count, SUM(g_size) as patient_count, COUNT(g_size)/SUM(g_size) as marketer, 1/ MIN(g_size) as prosecutor
            FROM (
                SELECT COUNT(*) as g_size,:key,:fields
                FROM :full_name
                GROUP BY :key,:fields
            )
        """.replace(":fields", ",".join(fields)).replace(":full_name",table['full_name']).replace(":key",key).replace(":n",str(n))
        return sql
    

        


if 'action' in SYS_ARGS and  SYS_ARGS['action'] in ['create','compute'] :

    path = SYS_ARGS['path']
    client = bq.Client.from_service_account_json(path)
    i_dataset = SYS_ARGS['i_dataset']
    key = SYS_ARGS['key'] 

    mytools = utils(client = client)
    tables = mytools.get_tables(dataset=i_dataset,client=client,key=key)
    # print len(tables)
    # tables = tables[:6]

    if SYS_ARGS['action'] == 'create' :
        #usage:
        #   create --i_dataset <in dataset> --key <patient id> --o_dataset <out dataset> --table <table|file> [--file] --path <bq JSON account file>
        #
        create_sql = mytools.get_sql(tables=tables,key=key) #-- The create statement
        o_dataset = SYS_ARGS['o_dataset']
        table = SYS_ARGS['table']
        if 'file' in SYS_ARGS :
            f = open(table+'.sql','w')
            f.write(create_sql)
            f.close()
        else:
            job = bq.QueryJobConfig()
            job.destination = client.dataset(o_dataset).table(table)
            job.use_query_cache = True
            job.allow_large_results = True 
            job.priority = 'BATCH'
            job.time_partitioning = bq.table.TimePartitioning(type_=bq.table.TimePartitioningType.DAY)

            r = client.query(create_sql,location='US',job_config=job) 
            
            print [r.job_id,' ** ',r.state]
    else:
        #
        #
        tables = [tab for tab in tables if tab['name'] == SYS_ARGS['table'] ]  
        if tables :            
            risk = risk()
            df = pd.DataFrame()
            for i in range(0,10) :
                sql = risk.get_sql(key=SYS_ARGS['key'],table=tables[0])
                df = df.append(pd.read_gbq(query=sql,private_key=path,dialect='standard'))
                df.to_csv(SYS_ARGS['table']+'.csv')
                print [i,' ** ',df.shape[0]]
                time.sleep(2)
                
    pass
else:
    print 'ERROR'
    pass

# r = risk(path='/home/steve/dev/google-cloud-sdk/accounts/vumc-test.json', i_dataset='raw',o_dataset='risk_o',o_table='mo')
# tables = r.get_tables('raw','person_id')
# sql = r.get_sql(tables=tables[:3],key='person_id')
# #
# # let's post this to a designated location
# #
# f = open('foo.sql','w')
# f.write(sql)
# f.close()
# r.get_sql(tables=tables,key='person_id')
# p = r.compute()
# print p
# p.to_csv("risk.csv")
# r.write('foo.sql')