# Re-Identification Risk

This framework computes re-identification risk of a dataset by extending pandas. It works like a pandas **add-on** 
The framework will compute the following risk measures: marketer, prosecutor, journalist and pitman risk. References for the risk measures can be found on [http://ehelthinformation.ca] (http://www.ehealthinformation.ca/wp-content/uploads/2014/08/2009-De-identification-PA-whitepaper1.pdf) and [https://www.scb.se/contentassets](https://www.scb.se/contentassets/ff271eeeca694f47ae99b942de61df83/applying-pitmans-sampling-formula-to-microdata-disclosure-risk-assessment.pdf)



There are two modes available :
    
**explore:**

Here the assumption is that we are not sure of the attributes to be disclosed, the framework will randomly generate random combinations of attributes and evaluate them accordingly as it provides all the measures of risk. 

**evaluation**

Here the assumption is that we are clear on the sets of attributes to be used and we are interested in computing the associated risk.


### Four risk measures are computed :

- Marketer risk
- Prosecutor risk
- Journalist risk
- Pitman Risk [Video tutorial,by Dr. Weiyi Xia](https://www.loom.com/share/173e109ecac64d37a54f09b103bc6681) and [Publication by Dr. Nobuaki Hoshino](https://www.scb.se/contentassets/ff271eeeca694f47ae99b942de61df83/applying-pitmans-sampling-formula-to-microdata-disclosure-risk-assessment.pdf)

### Usage:

Install this package using pip as follows :

Stable :
    
    pip install git+https://dev.the-phi.com/git/healthcareio/privacykit.git@release
    
    
Latest Development (not fully tested):
    
    pip install git+https://dev.the-phi.com/git/healthcareio/privacykit.git@dev
    
The framework will depend on pandas and numpy (for now). Below is a basic sample to get started quickly.


    import numpy as np
    import pandas as pd
    import privacykit

    mydf = pd.DataFrame({"x":np.random.choice( np.random.randint(1,10),50),"y":np.random.choice( np.random.randint(1,10),50),"z":np.random.choice( np.random.randint(1,10),50),"r":np.random.choice( np.random.randint(1,10),50)  })
    print (mydf.risk.evaluate())



    #
    # computing journalist and pitman
    #   - Insure the population size is much greater than the sample size 
    #   - Insure the fields are identical in both sample and population
    #
    pop = pd.DataFrame({"x":np.random.choice( np.random.randint(1,10),150),"y":np.random.choice( np.random.randint(1,10),150) ,"z":np.random.choice( np.random.randint(1,10),150),"r":np.random.choice( np.random.randint(1,10),150)})
    print (mydf.risk.evaluate(pop=pop))


@TODO:
    - Evaluation of how sparse attributes are (the ratio of non-null over rows)
    - Have a smart way to drop attributes (based on the above in random policy search)
Basic examples that illustrate usage of the the framework are in the notebook folder. The example is derived from 

	
