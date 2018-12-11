"""
    Health Information Privacy Lab
    Steve L. Nyemba & Brad. Malin


    This is an extension to the pandas data-frame that will perform a risk assessment on a variety of attributes
    This implementation puts the responsibility on the user of the framework to join datasets and load the final results into a pandas data-frame.

    The code will randomly select fields and compute the risk (marketer and prosecutor) and perform a given number of runs.

    Usage:
    from pandas_risk import *

    mydataframe = pd.DataFrame('/myfile.csv')
    risk = mydataframe.deid.risk(id=<name of patient field>,num_runs=<number of runs>)


    @TODO:
        - Provide a selected number of fields and risk will be computed for those fields.
        - include journalist risk

"""
import pandas as pd
import numpy as np
import time
@pd.api.extensions.register_dataframe_accessor("deid")
class deid :
    """
        This class is a deidentification class that will compute risk (marketer, prosecutor) given a pandas dataframe
    """
    def __init__(self,df):
        self._df = df.fillna(' ')
    def explore(self,**args):
        """
        This function will perform experimentation by performing a random policies (combinations of attributes)
        This function is intended to explore a variety of policies and evaluate their associated risk.

        @param pop|sample   data-frame with popublation reference
        @param id       key field that uniquely identifies patient/customer ...
        """
        # id = args['id']
        pop= args['pop'] if 'pop' in args else None
        # if 'columns' in args :
        #     cols = args['columns']
        #     params = {"sample":args['data'],"cols":cols}
        #     if pop is not None :
        #         params['pop'] = pop
        #     return self.evaluate(**params)
        # else :
        #
        # Policies will be generated with a number of runs
        #
        RUNS = args['num_runs'] if 'num_runs' in args else 5
        
        sample = args['sample'] if 'sample' in args else pd.DataFrame(self._df)
        
        k = sample.columns.size -1 if 'field_count' not in args else int(args['field_count'])
        columns = list(set(sample.columns.tolist()) - set([id]))
        o = pd.DataFrame()
        # pop = args['pop'] if 'pop' in args else None
        for i in np.arange(RUNS):
            n = np.random.randint(2,k)
            
            cols = np.random.choice(columns,n,replace=False).tolist()            
            params = {'sample':sample,'cols':cols}
            if pop is not None :
                params['pop'] = pop
            r = self.evaluate(**params)
            #
            # let's put the policy in place
            p =  pd.DataFrame(1*sample.columns.isin(cols)).T
            p.columns = sample.columns
            o = o.append(r.join(p))
            
        o.index = np.arange(o.shape[0]).astype(np.int64)

        return o
    def evaluate(self,**args) :
        """
        This function will compute the marketer, if a population is provided it will evaluate the marketer risk relative to both the population and sample
        @param smaple  data-frame with the data to be processed
        @param policy   the columns to be considered.
        @param pop      population dataset
        @params flag    user defined flag (no computation use)
        """
        if (args and 'sample' not in args) or not args :
            x_i = pd.DataFrame(self._df)
        elif args and 'sample' in args :
            x_i = args['sample']
        if not args  or 'cols' not in args:
            cols = x_i.columns.tolist()
            # cols = self._df.columns.tolist()
        elif args and 'cols' in args :
            cols = args['cols']
        flag = args['flag'] if 'flag' in args else 'UNFLAGGED'
        MIN_GROUP_SIZE = args['min_group_size'] if 'min_group_size' in args else 1
        # if args and 'sample' in args :
            
        #     x_i     = pd.DataFrame(self._df)
        # else :
        #     cols    = args['cols'] if 'cols' in args else self._df.columns.tolist()
        # x_i     = x_i.groupby(cols,as_index=False).size().values 
        x_i_values = x_i.groupby(cols,as_index=False).size().values
        SAMPLE_GROUP_COUNT = x_i_values.size        
        SAMPLE_FIELD_COUNT = len(cols)
        SAMPLE_POPULATION  = x_i_values.sum()
        UNIQUE_REC_RATIO = np.divide(np.sum(x_i_values <= MIN_GROUP_SIZE) , np.float64( SAMPLE_POPULATION))
        SAMPLE_MARKETER    = SAMPLE_GROUP_COUNT / np.float64(SAMPLE_POPULATION)
        SAMPLE_PROSECUTOR  = 1/ np.min(x_i_values).astype(np.float64)
        if 'pop' in args :
            Yi = args['pop']            
            y_i= pd.DataFrame({"group_size":Yi.groupby(cols,as_index=False).size()}).reset_index()
            UNIQUE_REC_RATIO  = np.sum(y_i.group_size < MIN_GROUP_SIZE) , np.float64(Yi.shape[0])
            # y_i['group'] = pd.DataFrame({"group_size":args['pop'].groupby(cols,as_index=False).size().values}).reset_index()
            # x_i = pd.DataFrame({"group_size":x_i.groupby(cols,as_index=False).size().values}).reset_index()
            x_i = pd.DataFrame({"group_size":x_i.groupby(cols,as_index=False).size()}).reset_index()
            SAMPLE_RATIO = int(100 * x_i.size/args['pop'].shape[0])
            r = pd.merge(x_i,y_i,on=cols,how='inner')
            r['marketer'] = r.apply(lambda row: (row.group_size_x / np.float64(row.group_size_y)) /np.sum(x_i.group_size) ,axis=1)
            r['sample %'] = np.repeat(SAMPLE_RATIO,r.shape[0])
            r['tier'] = np.repeat(flag,r.shape[0])
            r['sample marketer'] =  np.repeat(SAMPLE_MARKETER,r.shape[0])
            r = r.groupby(['sample %','tier','sample marketer'],as_index=False).sum()[['sample %','marketer','sample marketer','tier']]
        else:
            r = pd.DataFrame({"marketer":[SAMPLE_MARKETER],"flag":[flag],"prosecutor":[SAMPLE_PROSECUTOR],"field_count":[SAMPLE_FIELD_COUNT],"group_count":[SAMPLE_GROUP_COUNT]})
        r['unique_row_ratio'] = np.repeat(UNIQUE_REC_RATIO,r.shape[0])
        return r
    
    def _risk(self,**args):
        """
            @param  id          name of patient field            
            @params num_runs    number of runs (default will be 100)
	    @params quasi_id 	list of quasi identifiers to be used (this will only perform a single run)
        """
        
        id  = args['id']
        if 'quasi_id' in args :
            num_runs = 1
            columns = list(set(args['quasi_id'])- set(id) )
        else :
            num_runs  = args['num_runs'] if 'num_runs' in args else 100
            columns = list(set(self._df.columns) - set([id]))
        
        r   = pd.DataFrame()    
        k = len(columns)
        N = self._df.shape[0]
        tmp = self._df.fillna(' ')
        np.random.seed(int(time.time()) )
        for i in range(0,num_runs) :
            
            #
            # let's chose a random number of columns and compute marketer and prosecutor risk
            # Once the fields are selected we run a groupby clause
            #
            if 'quasi_id' not in args :
                if 'field_count' in args :
                    #
                    # We chose to limit how many fields we passin
                    n   = np.random.randint(2,int(args['field_count'])) #-- number of random fields we are picking    
                else :
                    n   = np.random.randint(2,k) #-- number of random fields we are picking
                ii = np.random.choice(k,n,replace=False)
                cols = np.array(columns)[ii].tolist()
                policy = np.zeros(k)
                policy [ii]  = 1
                policy =  pd.DataFrame(policy).T

            else:
                cols 	= columns
                policy = np.ones(k)
                policy = pd.DataFrame(policy).T
            n 	= len(cols)
            policy.columns = columns
            N = tmp.shape[0]

            x_ = tmp.groupby(cols).size().values
            # print [id,i,n,k,self._df.groupby(cols).count()]
            r = r.append(
                pd.DataFrame(
                    [
                        {
                            "group_count":x_.size,
                            
                            "patient_count":N,
                            "field_count":n,
                            "marketer": x_.size / np.float64(np.sum(x_)),
                            "prosecutor":1 / np.float64(np.min(x_))

                        }
                    ]
                ).join(policy)
            )
            # g_size = x_.size
            # n_ids = np.float64(np.sum(x_))  
            # sql = """
            #  SELECT COUNT(g_size) as group_count, :patient_count as patient_count,SUM(g_size) as rec_count, COUNT(g_size)/SUM(g_size) as marketer, 1/ MIN(g_size) as prosecutor, :n as field_count
            #     FROM (
            #     SELECT COUNT(*) as g_size,:key,:fields
            #     FROM :full_name
            #     GROUP BY :fields
            # """.replace(":n",str(n)).replace(":fields",",".join(cols)).replace(":key",id).replace(":patient_count",str(N))
            # r.append(self._df.query(sql.replace("\n"," ").replace("\r"," ") ))

        return r


# df = pd.read_gbq("select * from deid_risk.risk_30k",private_key='/home/steve/dev/google-cloud-sdk/accounts/curation-test.json')
# r =  df.deid.risk(id='person_id',num_runs=200)
# print r[['field_count','patient_count','marketer','prosecutor']]


