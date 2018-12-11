SELECT * 
FROM (
    SELECT person.person_id,first_name,last_name,birth_date,city,family_history_aware,current_hyper_tension,sex_at_birth, race,state, gender,ethnicity,birth_place,orientation,education,employment_status,
    marital_status,language,home_owner,sd_bloodbank, nhpi, living_situation,income,death_cause, death_date,  active_duty_status,
    gender_identity, insurance_type, work_address_state,consent_18_years_age,person_one_state,person_two_state,sc_site,
    health_abroad_6_months,travel_abroad_6_months
    FROM 
    (SELECT DISTINCT person_id from deid_tmp.observation order by person_id) as person

    
    FULL JOIN (
    SELECT 
        person_id,MAX(value_source_value) as travel_abroad_6_months
    FROM deid_tmp.observation   
    WHERE REGEXP_CONTAINS(observation_source_value,'OutsideTravel6Month_OutsideTravel6MonthWhere')  and value_source_value IS NOT NULL
    GROUP BY person_id
    order by person_id
    ) as te_
    ON te_.person_id = person.person_id

    FULL JOIN (
    SELECT 
        person_id,MAX(value_source_value) as health_abroad_6_months
    FROM deid_tmp.observation   
    WHERE REGEXP_CONTAINS(observation_source_value,'OverallHealth_OutsideTravel6Month')  and value_source_value IS NOT NULL
    GROUP BY person_id
    order by person_id
    ) as he_
    ON he_.person_id = person.person_id

    FULL JOIN (
    SELECT 
        person_id,MAX(value_source_value) as active_duty_status
    FROM deid_tmp.observation   
    WHERE REGEXP_CONTAINS(observation_source_value,'ActiveDuty_AvtiveDutyServeStatus')  and value_source_value IS NOT NULL
    GROUP BY person_id
    order by person_id
    ) as mil_
    ON mil_.person_id = person.person_id
    
    FULL JOIN (
    SELECT 
        person_id,MAX(value_source_value) as sc_site
    FROM deid_tmp.observation   
    WHERE REGEXP_CONTAINS(observation_source_value,'SouthCarolinaSitePairing_EauClaireAppointment')  and value_source_value IS NOT NULL
    GROUP BY person_id
    order by person_id
    ) as sc_
    ON sc_.person_id = person.person_id


    FULL JOIN (
    SELECT 
        person_id,MAX(value_source_value) as person_one_state
    FROM deid_tmp.observation   
    WHERE REGEXP_CONTAINS(observation_source_value,'PersonOneAddress_PersonOneAddressState')  and value_source_value IS NOT NULL
    GROUP BY person_id
    order by person_id
    ) as p1_
    ON p1_.person_id = person.person_id

    FULL JOIN (
    SELECT 
        person_id,MAX(value_source_value) as person_two_state
    FROM deid_tmp.observation   
    WHERE REGEXP_CONTAINS(observation_source_value,'SecondContactsAddress_SecondContactState')  and value_source_value IS NOT NULL
    GROUP BY person_id
    order by person_id
    ) as p2_
    ON p2_.person_id = person.person_id

    FULL JOIN (
    SELECT 
        person_id,MAX(value_as_string) as work_address_state
    FROM deid_tmp.observation   
    WHERE REGEXP_CONTAINS(observation_source_value,'EmploymentWorkAddress_State')  and value_source_value IS NOT NULL
    GROUP BY person_id
    order by person_id
    ) as ws_
    ON ws_.person_id = person.person_id


    FULL JOIN (
    SELECT 
        person_id,MAX(value_source_value) as consent_18_years_age
    FROM deid_tmp.observation   
    WHERE REGEXP_CONTAINS(observation_source_value,'ExtraConsent_18YearsofAge')  and value_source_value IS NOT NULL
    GROUP BY person_id
    order by person_id
    ) as c18_
    ON c18_.person_id = person.person_id


    FULL JOIN (
    SELECT 
        person_id,MAX(value_source_value) as gender_identity
    FROM deid_tmp.observation   
    WHERE REGEXP_CONTAINS(observation_source_value,'Gender_GenderIdentity')  and value_source_value IS NOT NULL
    GROUP BY person_id
    order by person_id
    ) as gi_
    ON gi_.person_id = person.person_id


    FULL JOIN (
    SELECT 
        person_id,MAX(value_source_value) as income
    FROM deid_tmp.observation   
    WHERE REGEXP_CONTAINS(observation_source_value,'Income_AnnualIncome')  and value_source_value IS NOT NULL
    GROUP BY person_id
    order by person_id
    ) as income_
    ON income_.person_id = person.person_id


    FULL JOIN (
    SELECT 
        person_id,MAX(value_source_value) as living_situation
    FROM deid_tmp.observation   
    WHERE REGEXP_CONTAINS(observation_source_value,'LivingSituation_CurrentLiving')  and value_source_value IS NOT NULL
    GROUP BY person_id
    order by person_id
    ) as living_
    ON living_.person_id = person.person_id


    FULL JOIN (
    SELECT 
        person_id,MAX(value_source_value) as nhpi
    FROM deid_tmp.observation   
    WHERE REGEXP_CONTAINS(observation_source_value,'NHPI_NHPISpecific')  and value_source_value IS NOT NULL
    GROUP BY person_id
    order by person_id
    ) as nhpi_
    ON nhpi_.person_id = person.person_id


    FULL JOIN (
    SELECT 
        person_id,MAX(value_as_string) as sd_bloodbank
    FROM deid_tmp.observation   
    WHERE REGEXP_CONTAINS(observation_source_value,'SanDiegoBloodBank')  and value_source_value IS NOT NULL
    GROUP BY person_id
    order by person_id
    ) as sd
    ON sd.person_id = person.person_id


    FULL JOIN (
    SELECT 
        person_id,MAX(value_source_value) as education
    FROM deid_tmp.observation   
    WHERE REGEXP_CONTAINS(observation_source_value,'EducationLevel_HighestGrade')  and value_source_value IS NOT NULL
    GROUP BY person_id
    order by person_id
    ) as edu
    ON edu.person_id = person.person_id


    FULL JOIN (
    SELECT 
        person_id,MAX(value_source_value) as home_owner 
    FROM deid_tmp.observation   
    WHERE REGEXP_CONTAINS(observation_source_value,'HomeOwn_CurrentHomeOwn')  and value_source_value IS NOT NULL
    GROUP BY person_id
    order by person_id
    ) as h_owner
    ON h_owner.person_id = person.person_id



    FULL JOIN (
    SELECT 
        person_id,MAX(value_source_value) as employment_status
    FROM deid_tmp.observation   
    WHERE REGEXP_CONTAINS(observation_source_value,'Employment_EmploymentStatus')  and value_source_value IS NOT NULL
    GROUP BY person_id
    order by person_id
    ) as empl
    ON empl.person_id = person.person_id

    FULL JOIN (
    SELECT 
        person_id,MAX(value_source_value) as marital_status
    FROM deid_tmp.observation   
    WHERE REGEXP_CONTAINS(observation_source_value,'MaritalStatus_CurrentMaritalStatus')  and value_source_value IS NOT NULL
    GROUP BY person_id
    order by person_id
    ) as marital
    ON marital.person_id = person.person_id

    FULL JOIN (
    SELECT 
        person_id,MAX(value_source_value) as language
    FROM deid_tmp.observation   
    WHERE REGEXP_CONTAINS(observation_source_value,'Language_SpokenWrittenLanguage')  and value_source_value IS NOT NULL
    GROUP BY person_id
    order by person_id
    ) as lang_
    ON lang_.person_id = person.person_id


    FULL JOIN (
    SELECT 
        person_id,MAX(value_source_value) as race 
    FROM deid_tmp.observation   
    WHERE REGEXP_CONTAINS(observation_source_value,'Race_WhatRace')  and value_source_value IS NOT NULL
    
    GROUP BY person_id
    order by person_id
    ) as lang
    ON lang.person_id = person.person_id
    FULL JOIN (
        SELECT 
        person_id,MAX(value_source_value) as ethnicity
        FROM deid_tmp.observation   
        WHERE REGEXP_CONTAINS(observation_source_value,'Race_WhatRaceEthnicity')  and value_source_value IS NOT NULL
    
    GROUP BY person_id
    order by person_id
    ) as ethnic 
    ON ethnic.person_id = person.person_id

    FULL JOIN (
    SELECT 
        person_id,MAX(value_source_value) as birth_place
    FROM deid_tmp.observation   
    WHERE REGEXP_CONTAINS(observation_source_value,'TheBasics_Birthplace')  and value_source_value IS NOT NULL
    GROUP BY person_id
    order by person_id
    ) as birthp
    ON birthp.person_id = person.person_id

    FULL JOIN (
    SELECT 
        person_id,MAX(value_source_value) as orientation
    FROM deid_tmp.observation   
    WHERE REGEXP_CONTAINS(observation_source_value,'TheBasics_SexualOrientation')  and value_source_value IS NOT NULL
    GROUP BY person_id
    order by person_id
    ) as sexo
    ON sexo.person_id = person.person_id


    FULL JOIN (
    SELECT
        person_id,max(value_source_value) as state 
    FROM deid_tmp.observation   
    WHERE REGEXP_CONTAINS(observation_source_value,'PIIState')  and value_source_value IS NOT NULL
    GROUP BY person_id
    order by person_id

    ) as p_addr_o
    ON p_addr_o.person_id = person.person_id

    FULL JOIN (
    SELECT
        person_id,max(value_source_value) as gender
    FROM deid_tmp.observation  
    WHERE REGEXP_CONTAINS(observation_source_value,'Gender_GenderIdentity')  and value_source_value IS NOT NULL
    GROUP BY person_id
    order by person_id

    ) as p_gender
    ON p_gender.person_id = person.person_id

    FULL JOIN (
    SELECT
        person_id,max(value_source_value) as sex_at_birth
    FROM deid_tmp.observation   
    WHERE REGEXP_CONTAINS(observation_source_value,'_SexAtBirth')  --and value_source_value IS NOT NULL
    GROUP BY person_id
    order by person_id

    ) as p_sex
    ON p_sex.person_id = person.person_id

    FULL JOIN (
    SELECT
        person_id,max(value_source_value) as insurance_type
    FROM deid_tmp.observation   
    WHERE REGEXP_CONTAINS(observation_source_value,'HealthInsurance_HealthInsuranceType')  and value_source_value IS NOT NULL
    GROUP BY person_id
    order by person_id

    ) as ins_
    ON ins_.person_id = person.person_id

    FULL JOIN (
    SELECT
        person_id,max(value_as_string) as last_name
    FROM deid_tmp.observation   
    WHERE REGEXP_CONTAINS(observation_source_value,'PIIName_Last')  and value_as_string IS NOT NULL
    GROUP BY person_id
    order by person_id

    ) as ln_
    ON ln_.person_id = person.person_id


    FULL JOIN (
    SELECT
        person_id,max(value_as_string) as first_name
    FROM deid_tmp.observation   
    WHERE REGEXP_CONTAINS(observation_source_value,'PIIName_First') 
    GROUP BY person_id
    order by person_id

    ) as fn_
    ON fn_.person_id = person.person_id

    FULL JOIN (
    SELECT
        person_id,max(value_as_string) as current_hyper_tension
    FROM deid_tmp.observation   
    WHERE REGEXP_CONTAINS(observation_source_value,'Circulatory_HypertensionCurrently') 
    GROUP BY person_id
    order by person_id

    ) as cht_
    ON cht_.person_id = person.person_id

    FULL JOIN (
    SELECT
        person_id,max( cast(value_as_string as DATE)) as birth_date
    FROM deid_tmp.observation   
    WHERE REGEXP_CONTAINS(observation_source_value,'PIIBirthInformation_BirthDate') 
    GROUP BY person_id
    order by person_id

    ) as bd_
    ON bd_.person_id = person.person_id


    FULL JOIN (
    SELECT
        person_id,max(value_as_string) as city
    FROM deid_tmp.observation   
    WHERE REGEXP_CONTAINS(observation_source_value,'StreetAddress_PIICity') 
    GROUP BY person_id
    order by person_id

    ) as city_
    ON city_.person_id = person.person_id

    FULL JOIN (
    SELECT
        person_id,max(value_as_string) as family_history_aware
    FROM deid_tmp.observation   
    WHERE REGEXP_CONTAINS(observation_source_value,'FamilyHistory_FamilyMedicalHistoryAware') 
    GROUP BY person_id
    order by person_id

    ) as bro_
    ON bro_.person_id = person.person_id
    FULL JOIN (
        SELECT person_id, max(death_date) AS death_date
        FROM deid_tmp.death
        GROUP BY person_id
        order BY person_id

    ) as death_
    ON death_.person_id = person.person_id

    FULL JOIN (
        SELECT person_id, max(cause_source_value) as death_cause
        FROM deid_tmp.death
        GROUP BY person_id
        order BY person_id


    ) as death_c ON death_c.person_id = person.person_id
    ORDER BY person.person_id
) as frame

-- WHERE first_name is not NULL
