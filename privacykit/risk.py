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
    resp = mydataframe.risk.evaluate(id=<name of patient field>,num_runs=<number of runs>,cols=[])
    resp = mydataframe.risk.explore(id=<name of patient field>,num_runs=<number of runs>,cols=[])


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

from itertools import combinations
# class Compute:
#     pass
# class Population(Compute):
#     pass

@pd.api.extensions.register_dataframe_accessor("risk")
class deid :

    """
    This class is a deidentification class that will compute risk (marketer, prosecutor) given a pandas dataframe
    """
    def __init__(self,df):
        self._df = df.fillna(' ')
        #
        # Let's get the distribution of the values so we know what how unique the fields are
        #
        values = df.apply(lambda col: col.unique().size / df.shape[0])
        self._dinfo = dict(zip(df.columns.tolist(),values))
        # self.sample = self._df
        self.init(sample=self._df)
    def init(self,**_args):
        _sample = _args['sample'] if 'sample' in _args else self._df
        _columns = [] if 'columns' not in _args else _args['columns']
        if _columns :
            self._compute = Compute(sample = _sample,columns=_columns)
        else:
            self._comput = Compute(sample=_sample)
        self._pcompute= Population()  

    def explore(self,**args):
        """
        This function will perform experimentation by performing a random policies (combinations of attributes)
        This function is intended to explore a variety of policies and evaluate their associated risk.

        :pop|sample     data-frame with population or sample reference
        :field_count    number of fields to randomly select
        :strict         if set the field_count is exact otherwise field_count is range from 2-field_count
        :num_runs       number of runs (by default 5)
        """
        
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
        
        k = sample.columns.size if 'field_count' not in args else int(args['field_count']) +1
        #
        # remove fields that are unique, they function as identifiers
        #
        if 'id' in args :
            id = args['id']
            columns = list(set(sample.columns.tolist()) - set([id]))
        else:
            columns = sample.columns.tolist()
        
        # If columns are not specified we can derive them from self._dinfo
        #   given the distribution all fields that are < 1 will be accounted for
        # columns = args['cols'] if 'cols' in args else [key for key in self._dinfo if self._dinfo[key] < 1]
        
        o = pd.DataFrame()
        columns = [key for key in self._dinfo if self._dinfo[key] < 1]
        _policy_count = 2 if 'policy_count' not in args else int(args['policy_count'])
        
        _policies = []
        _index = 0
        for size in np.arange(2,len(columns)) :
            p = list(combinations(columns,size))            
            p = (np.array(p)[ np.random.choice( len(p), _policy_count)].tolist())
            
            
            for cols in p :
                flag = 'Policy_'+str(_index)
                r = self.evaluate(sample=sample,cols=cols,flag = flag)
                p =  pd.DataFrame(1*sample.columns.isin(cols)).T
                p.columns = sample.columns
                o = pd.concat([o,r.join(p)])

                o['attributes'] = ','.join(cols)
                # o['attr'] = ','.join(r.apply())
                _index += 1
        #
        # We rename flags to policies and adequately number them, we also have a column to summarize the attributes attr
        #
           
      

            
        o.index = np.arange(o.shape[0]).astype(np.int64)
        o = o.rename(columns={'flag':'policies'})
        return o
    def evaluate(self,**_args):
        _measure = {}

        self.init(**_args)
        _names = ['marketer','journalist','prosecutor'] #+ (['pitman'] if 'pop_size' in _args else [])
        for label in _names :
            _pointer = getattr(self,label)
            _measure[label] = _pointer(**_args)
        
        _measure['fields'] = self._compute.cache['count']['fields']
        _measure['groups'] = self._compute.cache['count']['groups']
        _measure['rows'] = self._compute.cache['count']['rows']
        if 'attr' in _args :
            _measure = dict(_args['attr'],**_measure)

        return pd.DataFrame([_measure])
    def _evaluate(self, **args):
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
            # cols = sample.columns.tolist()
            cols = [key for key in self._dinfo if self._dinfo[key] < 1]
        elif args and 'cols' in args:
            cols = args['cols']
        #
        #
      
        flag = 'UNFLAGGED' if 'flag' not in args else args['flag']
        #
        # @TODO: auto select the columns i.e removing the columns that will have the effect of an identifier
        #
        # if 'population' in args :
        #     pop = pd.DataFrame(args['population'])
        r = {"flag":flag}
        # if sample :
        
        handle_sample   = Compute()        
        xi              = sample.groupby(cols,as_index=False).count().values
        
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
            label_market = 'sample marketer'
            label_prosec = 'sample prosecutor'
            label_groupN = 'sample group count'
            label_unique = 'sample journalist' #'sample unique ratio'
            # r['sample marketer']   = handle_sample.marketer()
            # r['sample prosecutor'] = handle_sample.prosecutor()
            # r['sample unique ratio']     = handle_sample.unique_ratio()
            # r['sample group count'] = xi.size
            # r['sample group count'] = len(xi)
        else:
            label_market = 'marketer'
            label_prosec = 'prosecutor'
            label_groupN = 'group count'
            label_unique = 'journalist' #'unique ratio'
            # r['marketer']   = handle_sample.marketer()
            # r['prosecutor'] = handle_sample.prosecutor()
            # r['unique ratio']     = handle_sample.unique_ratio()
            # r['group count'] =  xi.size
            # r['group count'] =  len(xi)
            if pop_size > 0 :
                handle_sample.set('pop_size',pop_size)
                r['pitman risk'] = handle_sample.pitman()
        r[label_market]   = handle_sample.marketer()
        r[label_unique]     = handle_sample.unique_ratio()
        r[label_prosec] = handle_sample.prosecutor()
        r[label_groupN] =  len(xi)
        
        if 'pop' in args :
            xi = pd.DataFrame({"sample_group_size":sample.groupby(cols,as_index=False).count()}).reset_index()
            yi = pd.DataFrame({"population_group_size":args['pop'].groupby(cols,as_index=False).size()}).reset_index()
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
    
    def marketer(self,**_args):
        """
        This function delegates the calls to compute marketer risk of a given dataset or sample
        :sample     optional sample dataset
        :columns    optional columns of the dataset, if non is provided and inference will be made using non-unique columns
        """
        if 'pop' not in _args :
            if not 'sample' in _args and not 'columns' in _args :
                # _handler =  self._compute
                pass
            else:
                
                self.init(**_args)
                # _handler = Compute(**_args)
            _handler =  self._compute

        else:
            #
            # Computing population estimates for the population
            self._pcompute.init(**_args)
            handler = self._pcompute
        return _handler.marketer()
    def journalist(self,**_args):
        """
        This function delegates the calls to compute journalist risk of a given dataset or sample
        :sample     optional sample dataset
        :columns    optional columns of the dataset, if non is provided and inference will be made using non-unique columns
        """
        if 'pop' not in _args :
            if not 'sample' in _args and not 'columns' in _args :
                _handler =  self._compute
            else:
                self.init(**_args)
                # _handler = Compute(**_args)
            _handler = self._compute
                # return _compute.journalist()
        else:
            self._pcompute.init(**_args)
            _handler = self._pcompute
        return _handler.journalist()
    def prosecutor(self,**_args):
        """
        This function delegates the calls to compute prosecutor risk of a given dataset or sample
        :sample     optional sample dataset
        :columns    optional columns of the dataset, if non is provided and inference will be made using non-unique columns
        """
        if 'pop' not in _args :
            if not 'sample' in _args and not 'columns' in _args :
                # _handler =  self._compute
                pass
            else:
                self.init(**_args)
                # _handler = Compute(**_args)
            _handler =  self._compute
                
        else:
            self._pcompute.init(**_args)
            _handler = self._pcompute
        return _handler.prosecutor()
    def pitman(self,**_args):
        
        if 'population' not in _args :
            pop_size = int(_args['pop_size'])
            self._compute.set('pop_size',pop_size)
            _handler =  self._compute;
        else:
            self._pcompute.init(**_args)
            _handler = self._pcompute
        
        return _handler.pitman()
        
        # xi = pd.DataFrame({"sample_group_size":sample.groupby(cols,as_index=False).count()}).reset_index()
        # yi = pd.DataFrame({"population_group_size":args['pop'].groupby(cols,as_index=False).size()}).reset_index()
        # merged_groups = pd.merge(xi,yi,on=cols,how='inner')
        # handle_population= Population()            
        # handle_population.set('merged_groups',merged_groups)
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

class Compute(Risk):
    """
    This class will compute risk for the sample dataset: the marketer and prosecutor risk are computed by default.
    This class can optionally add pitman risk if the population size is known.
    """
    def __init__(self,**_args):
        super().__init__()
        self._sample = _args['sample'] if 'sample' in _args else pd.DataFrame()
        self._columns= _args['columns'] if 'columns' in _args else None
        self.cache['count']  = {'groups':0,'fields':0,'rows':0}
        if not self._columns :
            values = self._sample.apply(lambda col: col.unique().size / self._sample.shape[0])            
            self._dinfo = dict(zip(self._sample.columns.tolist(),values))
            self._columns = [key for key in self._dinfo if self._dinfo[key] < 1]
        #
        # At this point we have all the columns that are valid candidates even if the user didn't specify them
        self.cache['count']['fields'] = len(self._columns)
        if self._sample.shape[0] > 0 and self._columns:
            _sample = _args ['sample']
            _groups = self._sample.groupby(self._columns,as_index=False).count().values
            self.set('groups',_groups)
    
            self.cache['count']['groups']  = len(_groups)
            self.cache['count']['rows']    = np.sum([_g[-1] for _g in _groups])
            
    def marketer(self):
        """
        computing marketer risk for sample dataset
        """
        
        
        groups = self.cache['groups']
        # group_count = groups.size
        # row_count   = groups.sum()
        # group_count = len(groups)
        group_count = self.cache['count']['groups']
        # row_count = np.sum([_g[-1] for _g in groups])
        row_count = self.cache['count']['rows']
        return group_count / np.float64(row_count)

    def prosecutor(self):
        """
        The prosecutor risk consists in determining 1 over the smallest group size
        It identifies if there is at least one record that is unique
        """
        groups = self.cache['groups']
        _min = np.min([_g[-1] for _g in groups])
        # return 1 / np.float64(groups.min())
        return 1/ np.float64(_min)
    def unique_ratio(self):
        groups = self.cache['groups']        
        # row_count = groups.sum()
        # row_count = np.sum([_g[-1] for _g in groups])
        row_count = self.cache['count']['rows']
        # return groups[groups == 1].sum() / np.float64(row_count)
        values = [_g[-1] for _g in groups if _g[-1] == 1]
        
        return np.sum(values) / np.float64(row_count)
    def journalist(self):
        return self.unique_ratio()
    def pitman(self):
        """
        This function will approximate pitman de-identification risk based on pitman sampling
        """
        
        groups = self.cache['groups']
        print (self.cache['pop_size'])
        si = groups[groups == 1].size
        # u = groups.size
        u = len(groups)
        alpha = np.divide(si , np.float64(u) )
        # row_count = np.sum([_g[-1] for _g in groups])
        row_count = self.cache['count']['rows']

        # f = np.divide(groups.sum(), np.float64(self.cache['pop_size']))
        f = np.divide(row_count, np.float64(self.cache['pop_size']))
        return np.power(f,1-alpha)

class Population(Compute):
    """
    This class will compute risk for datasets that have population information or datasets associated with them.
    This computation includes pitman risk (it requires minimal information about population)
    """
    def __init__(self,**_args):
        super().__init__(**_args)

    def init(self,**_args):
        xi = pd.DataFrame({"sample_group_size":self._sample.groupby(self._columns,as_index=False).count()}).reset_index()
        yi = pd.DataFrame({"population_group_size":_args['population'].groupby(self._columns,as_index=False).size()}).reset_index()
        merged_groups = pd.merge(xi,yi,on=self._columns,how='inner')                   
        self.set('merged_groups',merged_groups)

    def set(self,key,value):
        self.set(self,key,value)
        if key == 'merged_groups' :  
               
            self.set(self,'pop_size',np.float64(value.population_group_size.sum()) )
            self.set(self,'groups',value.sample_group_size)
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


