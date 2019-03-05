"""
(c) 2019, Health Information Privacy Lab
Brad. Malin, Weiyi Xia, Steve L. Nyemba

This framework computes re-identification risk of a dataset assuming the data being shared can be loaded into a dataframe (pandas)
The framework will compute the following risk measures:
    - marketer
    - prosecutor
    - pitman

References :
    https://www.scb.se/contentassets/ff271eeeca694f47ae99b942de61df83/applying-pitmans-sampling-formula-to-microdata-disclosure-risk-assessment.pdf

This framework integrates pandas (for now) as an extension and can be used in two modes :
1. explore:
    Here the assumption is that we are not sure of the attributes to be disclosed, 
    The framework will explore a variety of combinations and associate risk measures every random combinations it can come up with

2. evaluation
    Here the assumption is that we are clear on the sets of attributes to be used and we are interested in computing the associated risk.


Four risk measures are computed :
    - Marketer risk
    - Prosecutor risk
    - Journalist risk
    - Pitman Risk

Usage:
import numpy as np
import pandas as pd
from pandas_risk import *

mydf = pd.DataFrame({"x":np.random.choice( np.random.randint(1,10),50),"y":np.random.choice( np.random.randint(1,10),50) })
print mydf.risk.evaluate()



#
# computing journalist and pitman
#   - Insure the population size is much greater than the sample size 
#   - Insure the fields are identical in both sample and population
#
pop = pd.DataFrame({"x":np.random.choice( np.random.randint(1,10),150),"y":np.random.choice( np.random.randint(1,10),150) ,"q":np.random.choice( np.random.randint(1,10),150)})
mydf.risk.evaluate(pop=pop)

@TODO:
    - Evaluation of how sparse attributes are (the ratio of non-null over rows)
    - Have a smart way to drop attributes (based on the above in random policy search)
"""
from risk import risk
