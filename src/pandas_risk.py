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
        """
        
        id  = args['id']
        
        num_runs  = args['num_runs'] if 'num_runs' in args else 100
        r   = pd.DataFrame()
        
        columns = list(set(self._df.columns) - set([id]))
        k = len(columns)
        for i in range(0,num_runs) :
            #
            # let's chose a random number of columns and compute marketer and prosecutor risk
            # Once the fields are selected we run a groupby clause
            #

            n   = np.random.randint(2,k) #-- number of random fields we are picking
            ii = np.random.choice(k,n,replace=False)
            cols = np.array(columns)[ii].tolist()
            x_ = self._df.groupby(cols).count()[id].values
            r = r.append(
                pd.DataFrame(
                    [
                        {
                            "selected":n,
                            "marketer": x_.size / np.float64(np.sum(x_)),
                            "prosecutor":1 / np.float64(np.min(x_))

                        }
                    ]
                )
            )
            g_size = x_.size
            n_ids = np.float64(np.sum(x_))

        return r


import pandas as pd
import numpy as np
from io import StringIO
csv = """
id,sex,age,profession,drug_test
1,M,37,doctor,-
2,F,28,doctor,+
3,M,37,doctor,-
4,M,28,doctor,+
5,M,28,doctor,-
6,M,37,doctor,-
"""
f = StringIO()
f.write(unicode(csv))
f.seek(0)
df = pd.read_csv(f)     
print df.deid.risk(id='id',num_runs=1)   