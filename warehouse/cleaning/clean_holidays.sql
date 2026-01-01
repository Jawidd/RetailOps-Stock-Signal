-- Clean(fix types, handles nulls, filters bad data) and standardize data 
-- COLUMNS= date,type,locale,locale_name,description,transferred

SELECT 
    CAST(date AS DATE) as holiday_date,
    type as holiday_type,
    locale as holiday_locale,
    locale_name,
    description as holiday_description,
    transferred,

    case 
        when type in ('Holiday', 'Additional', 'Bridge') 
         and transferred = FALSE 
        then TRUE
        else FALSE
    end as is_actual_holiday

from raw.holidays_events
where date is not null;

