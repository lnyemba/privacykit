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

@pd.api.extensions.register_dataframe_accessor("deid")
class deid :
    """
        This class is a deidentification class that will compute risk (marketer, prosecutor) given a pandas dataframe
    """
    def __init__(self,df):
        self._df = df
    
    def risk(self,**args):
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
        np.random.seed(1)
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


