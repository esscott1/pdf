select 
sourcefilename,
jsondata -> 'first_name' ->> 'value' as first_name,
jsondata -> 'last_name' ->> 'value' as last_name,
jsondata -> 'email' ->> 'value' as email,
(jsondata -> 'owner' ->> 'value') ||', '|| (jsondata -> 'business_owner' ->> 'value') ||', '|| (jsondata -> 'exposed' ->> 'value') || ', '|| (jsondata -> 'diagnosed' ->> 'value') as criteria,
jsondata -> 'last_name' ->> 'value' as reg_last_name,
jsondata -> 'first_name' ->> 'value' as reg_first_name,
jsondata -> 'middle_name' ->> 'value' as reg_middle_name,
jsondata -> 'ssn' ->> 'value' as reg_social_security,
jsondata -> 'dob' ->> 'value' as reg_date_of_birth,
jsondata -> 'address_1' ->> 'value' as reg_street_address,
jsondata -> 'apt_suite_1' ->> 'value' as "reg_apt-suite",
jsondata -> 'city_1' ->> 'value' as reg_city,
jsondata -> 'state_1' ->> 'value' as reg_state,
jsondata -> 'zip_1' ->> 'value' as reg_zip_code,
jsondata -> 'address_1_from' ->> 'value' as reg_resided_from,
jsondata -> 'address_1_to' ->> 'value' as reg_resided_to,
(jsondata -> 'address_1_from' ->> 'value' ) || ' - ' ||(jsondata -> 'address_1_to' ->> 'value') as reg_dates_resided_at_this_address,
jsondata -> 'address_2' ->> 'value' as reg_street_address_2,
jsondata -> 'apt_suite_2' ->> 'value' as "reg_apt-suite_2",
jsondata -> 'city_2' ->> 'value' as reg_city_2,
jsondata -> 'state_2' ->> 'value' as reg_state_2,
jsondata -> 'zip_2' ->> 'value' as reg_zip_code_2,
jsondata -> 'address_2_from' ->> 'value' as reg_resided_from_2,
jsondata -> 'address_2_to' ->> 'value' as reg_resided_to_2,
jsondata -> 'address_3' ->> 'value' as reg_street_address_3,
jsondata -> 'apt_suite_3' ->> 'value' as "reg_apt-suite_3",
jsondata -> 'city_3' ->> 'value' as reg_city_3,
jsondata -> 'state_3' ->> 'value' as reg_state_3,
jsondata -> 'zip_3' ->> 'value' as reg_zip_code_3,
jsondata -> 'address_3_from' ->> 'value' as reg_resided_from_3,
jsondata -> 'address_3_to' ->> 'value' as reg_resided_to_3,
jsondata -> 'phone' ->> 'value' as reg_phone_number,
(jsondata -> 'phone_work' ->> 'value') || ' ' || (jsondata -> 'phone_mobile' ->> 'value') ||' '||(jsondata -> 'phone_home' ->> 'value') as reg_phone_type,
jsondata -> 'alt_phone' ->> 'value' as reg_alt_phone_number,
(jsondata -> 'alt__phone_work' ->> 'value') || ' ' || (jsondata -> 'alt_phone_mobile' ->> 'value') ||' '||(jsondata -> 'alt_phone_home' ->> 'value') as reg_alt_phone_type,
jsondata -> 'email' ->> 'value' as reg_email_address,
(jsondata -> 'on_behalf_of_yes' ->> 'value') || ', ' || (jsondata -> 'on_behalf_of_no' ->> 'value') as on_behalf,
(jsondata -> 'rep_spouse' ->> 'value') || ', ' || (jsondata -> 'rep_parent' ->> 'value') ||', '||
(jsondata -> 'rep_stepparent' ->> 'value') || ', ' || (jsondata -> 'rep_adultchild' ->> 'value') ||', '||
(jsondata -> 'rep_adultsibling' ->> 'value') || ', ' || (jsondata -> 'rep_adultaunt' ->> 'value') ||', '||
(jsondata -> 'rep_adultuncle' ->> 'value') || ', ' || (jsondata -> 'rep_grandparent' ->> 'value') ||', '||
(jsondata -> 'rep_legalguardian' ->> 'value') || ', ' || (jsondata -> 'rep_estateadmin' ->> 'value') ||', '|| (jsondata -> 'rep_other' ->> 'value') as rep_relation,

'None' as other_relationship,
jsondata -> 'rep_last_name' ->> 'value' as rep_last_name,
jsondata -> 'rep_first_name' ->> 'value' as rep_first_name,
jsondata -> 'rep_middle_name' ->> 'value' as rep_middle_name,
jsondata -> 'rep_address' ->> 'value' as rep_street_address,
jsondata -> 'rep_apt_suite' ->> 'value' as "rep_apt-suite",
jsondata -> 'rep_city' ->> 'value' as rep_city,
jsondata -> 'rep_state' ->> 'value' as "rep_state",
jsondata -> 'rep_zip' ->> 'value' as rep_zip_code,
jsondata -> 'rep_ssn' ->> 'value' as rep_social_security,
jsondata -> 'rep_dob' ->> 'value' as rep_date_of_birth,
jsondata -> 'dod' ->> 'value' as date_of_death_registrant,
jsondata -> 'rep_phone' ->> 'value' as "rep_phone",
(jsondata -> 'rep_phone_work' ->> 'value') || ', ' || (jsondata -> 'rep_phone_mobile' ->> 'value') ||', '||(jsondata -> 'rep_phone_home' ->> 'value') as rep_phone_type,
jsondata -> 'rep_alt_phone' ->> 'value' as "rep_alt_phone",
(jsondata -> 'rep_alt_phone_work' ->> 'value') || ', ' || (jsondata -> 'rep_alt_phone_mobile' ->> 'value') ||', '||(jsondata -> 'rep_phone_home' ->> 'value') as rep_alt_phone_type,
jsondata -> 'rep_email' ->> 'value' as "rep_email",
(jsondata -> 'attorney_yes' ->> 'value') || ', ' || (jsondata -> 'attorney_no' ->> 'value') as attorney,
jsondata -> 'att_last_name' ->> 'value' as att_last_name,
jsondata -> 'att_first_name' ->> 'value' as att_first_name,
jsondata -> 'att_firm_name' ->> 'value' as firm_name,
jsondata -> 'att_street' ->> 'value' as att_street,
jsondata -> 'att_city' ->> 'value' as att_city,
jsondata -> 'att_state' ->> 'value' as att_state,
jsondata -> 'att_zip' ->> 'value' as att_zip,
jsondata -> 'att_phone' ->> 'value' as att_phone,
jsondata -> 'att_email' ->> 'value' as att_email
from flint_claimant
