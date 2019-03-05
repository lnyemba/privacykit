"""
    Health Information Privacy Lab
    Brad. Malin, Weiyi Xia, Steve L. Nyemba

    This framework computes re-identification risk of a dataset assuming the data being shared can be loaded into a dataframe (pandas)
    The framework will compute the following risk measures:
        - marketer
        - prosecutor
        - pitman

    References :
        https://www.scb.se/contentassets/ff271eeeca694f47ae99b942de61df83/applying-pitmans-sampling-formula-to-microdata-disclosure-risk-assessment.pdf

    This framework integrates pandas (for now) as an extension and can be used in two modes :
    Experimental mode
        Here the assumption is that we are not sure of the attributes to be disclosed, the framework will explore a variety of combinations and associate risk measures every random combinations

    Evaluation mode
        The evaluation mode assumes the set of attributes given are known and thus will evaluate risk for a subset of attributes.

    features :
        - determine viable fields (quantifiable in terms of uniqueness). This is a way to identify fields that can act as identifiers.
        - explore and evaluate risk of a sample dataset against a known population dataset
        - explore and evaluate risk on a sample dataset
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
import logging	
import json
from datetime import datetime
import sys

sys.setrecursionlimit(3000)
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
        id = args['id']
        pop= args['pop'] if 'pop' in args else None
        
        if 'pop_size' in args :
            pop_size = np.float64(args['pop_size'])
        else:
            pop_size = -1
        
        
        #
        # Policies will be generated with a number of runs
        #
        RUNS = args['num_runs'] if 'num_runs' in args else 5
        
        sample = args['sample'] if 'sample' in args else pd.DataFrame(self._df)
        
        k = sample.columns.size -1 if 'field_count' not in args else int(args['field_count'])
        columns = list(set(sample.columns.tolist()) - set([id]))
        o = pd.DataFrame()
        
        for i in np.arange(RUNS):
            n = np.random.randint(2,k)
            
            cols = np.random.choice(columns,n,replace=False).tolist()            
            params = {'sample':sample,'cols':cols}
            if pop is not None :
                params['pop'] = pop
            if pop_size > 0  :
                params['pop_size'] = pop_size

            r = self.evaluate(**params)
            #
            # let's put the policy in place
            p =  pd.DataFrame(1*sample.columns.isin(cols)).T
            p.columns = sample.columns
            o = o.append(r.join(p))
            
        o.index = np.arange(o.shape[0]).astype(np.int64)

        return o
    def evaluate(self, **args):
        """
        This function has the ability to evaluate risk associated with either a population or a sample dataset
        :sample sample dataset
        :pop    population dataset
        :cols   list of columns of interest or policies
        :flag   user provided flag for the context of the evaluation
        """
        if 'sample' in args :
            sample = pd.DataFrame(args['sample'])
        else:
            sample = pd.DataFrame(self._df)

        if not args  or 'cols' not in args:
            cols = sample.columns.tolist()
        elif args and 'cols' in args:
            cols = args['cols']
        flag = 'UNFLAGGED' if 'flag' not in args else args['flag']
        #
        # @TODO: auto select the columns i.e removing the columns that will have the effect of an identifier
        #
        # if 'population' in args :
        #     pop = pd.DataFrame(args['population'])
        r = {"flag":flag}
        # if sample :

        handle_sample   = Sample()
        xi              = sample.groupby(cols,as_index=False).size().values

        handle_sample.set('groups',xi)
        if 'pop_size' in args :
            pop_size = np.float64(args['pop_size'])
        else:
            pop_size = -1
        #
        #-- The following conditional line is to address the labels that will be returned
        # @TODO: Find a more elegant way of doing this.
        #
        if 'pop' in args :
            r['sample marketer']   = handle_sample.marketer()
            r['sample prosecutor'] = handle_sample.prosecutor()
            r['sample unique ratio']     = handle_sample.unique_ratio()
            r['sample group count'] = xi.size
        else:
            r['marketer']   = handle_sample.marketer()
            r['prosecutor'] = handle_sample.prosecutor()
            r['unique ratio']     = handle_sample.unique_ratio()
            r['group count'] =  xi.size
            if pop_size > 0 :
                handle_sample.set('pop_size',pop_size)
                r['pitman risk'] = handle_sample.pitman()
        if 'pop' in args :
            xi = pd.DataFrame({"sample_group_size":sample.groupby(cols,as_index=False).size()}).reset_index()
            yi = pd.DataFrame({"population_group_size":args['population'].groupby(cols,as_index=False).size()}).reset_index()
            merged_groups = pd.merge(xi,yi,on=cols,how='inner')
            handle_population= Population()            
            handle_population.set('merged_groups',merged_groups)
            
            r['pop. marketer'] = handle_population.marketer()            
            r['pitman risk'] = handle_population.pitman()
            r['pop. group size'] = np.unique(yi.population_group_size).size
        #
        # At this point we have both columns for either sample,population or both
        #
        r['field count'] = len(cols)
        return pd.DataFrame([r])

class Risk :
    """
    This class is an abstraction of how we chose to structure risk computation i.e in 2 sub classes:
        - Sample        computes risk associated with a sample dataset only
        - Population    computes risk associated with a population
    """
    def __init__(self):
        self.cache = {}        
    def set(self,key,value):        
        if id not in self.cache :
            self.cache[id] = {}
        self.cache[key] = value

class Sample(Risk):
    """
    This class will compute risk for the sample dataset: the marketer and prosecutor risk are computed by default.
    This class can optionally add pitman risk if the population size is known.
    """
    def __init__(self):
        Risk.__init__(self)
    def marketer(self):
        """
        computing marketer risk for sample dataset
        """
        groups = self.cache['groups']
        group_count = groups.size
        row_count   = groups.sum()
        return group_count / np.float64(row_count)

    def prosecutor(self):
        """
        The prosecutor risk consists in determining 1 over the smallest group size
        It identifies if there is at least one record that is unique
        """
        groups = self.cache['groups']
        return 1 / np.float64(groups.min())
    def unique_ratio(self):
        groups = self.cache['groups']        
        row_count = groups.sum()
        return groups[groups == 1].sum() / np.float64(row_count)

    def pitman(self):
        """
        This function will approximate pitman de-identification risk based on pitman sampling
        """
        groups = self.cache['groups']
        si = groups[groups == 1].size
        u = groups.size
        alpha = np.divide(si , np.float64(u) )
        f = np.divide(groups.sum(), np.float64(self.cache['pop_size']))
        return np.power(f,1-alpha)

class Population(Sample):
    """
    This class will compute risk for datasets that have population information or datasets associated with them.
    This computation includes pitman risk (it requires minimal information about population)
    """
    def __init__(self,**args):
        Sample.__init__(self)

    def set(self,key,value):
        Sample.set(key,value)
        if key == 'merged_groups' :
            Sample.set('pop_size',np.float64(r.population_group_sizes.sum()) )
    """
    This class will measure risk and account for the existance of a population
    :merged_groups {sample_group_size, population_group_size} is a merged dataset with group sizes of both population and sample
    """
    def marketer(self):
        """
        This function requires
        """
        r = self.cache['merged_groups']
        sample_row_count = r.sample_group_size.sum() 
        #
        # @TODO : make sure the above line is size (not sum)
        # sample_row_count = r.sample_group_size.size
        return r.apply(lambda row: (row.sample_group_size / np.float64(row.population_group_size)) /np.float64(sample_row_count) ,axis=1).sum()
