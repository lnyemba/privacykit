SELECT person.person_id,sex_at_birth,birth_date, race,zip,city,state, gender
FROM 
  (SELECT DISTINCT person_id from deid_tmp.observation order by person_id) as person
FULL JOIN (
  SELECT 
    person_id,MAX(value_as_string) as race 
  FROM deid_tmp.observation   
  WHERE REGEXP_CONTAINS(observation_source_value,'Race_WhatRace')  and value_as_string IS NOT NULL
  
  GROUP BY person_id
  order by person_id
) as lang
ON lang.person_id = person.person_id

FULL JOIN (
 SELECT 
    person_id,MAX(value_as_string) as zip 
  FROM deid_tmp.observation   
  WHERE REGEXP_CONTAINS(observation_source_value,'PIIZIP')  and value_as_string IS NOT NULL
  GROUP BY person_id
  order by person_id
) as work_add
ON work_add.person_id = person.person_id



FULL JOIN (
 SELECT 
    person_id,max(value_as_string) as city 
  FROM deid_tmp.observation   
  WHERE REGEXP_CONTAINS(observation_source_value,'PIICity')  and value_as_string IS NOT NULL
  GROUP BY person_id
  order by person_id
) as u_city
ON u_city.person_id = person.person_id

FULL JOIN (
  SELECT
    person_id,max(value_as_string) as state 
  FROM deid_tmp.observation   
  WHERE REGEXP_CONTAINS(observation_source_value,'PIIState')  and value_as_string IS NOT NULL
  GROUP BY person_id
  order by person_id

) as p_addr_o
ON p_addr_o.person_id = person.person_id

FULL JOIN (
  SELECT
    person_id,max(value_as_string) as gender
  FROM deid_tmp.observation   
  WHERE REGEXP_CONTAINS(observation_source_value,'Gender_GenderIdentity')  and value_as_string IS NOT NULL
  GROUP BY person_id
  order by person_id

) as p_gender
ON p_gender.person_id = person.person_id

FULL JOIN (
  SELECT
    person_id,max(value_as_string) as birth_date
  FROM deid_tmp.observation   
  WHERE REGEXP_CONTAINS(observation_source_value,'PIIBirthInformation_BirthDate')  and value_as_string IS NOT NULL
  GROUP BY person_id
  order by person_id

) as p_birth
ON p_birth.person_id = person.person_id

FULL JOIN (
  SELECT
    person_id,max(value_as_string) as sex_at_birth
  FROM deid_tmp.observation   
  WHERE REGEXP_CONTAINS(observation_source_value,'BiologicalSexAtBirth_SexAtBirth')  and value_as_string IS NOT NULL
  GROUP BY person_id
  order by person_id

) as p_sex
ON p_sex.person_id = person.person_id


ORDER BY person.person_id

