-- holidays_COLUMNS= date,type,locale,locale_name,description,transferred



SELECT

    cast(date as date) as holiday_date,
    type as holiday_type,
    locale as holiday_locale,
    locale_name,
    description as holiday_description,
    coalesce(cast(transferred as boolean), false) as holiday_transferred,

    case 
        when type in ('Holiday', 'Additional', 'Bridge')
        and transferred = 'False'   then true
        else false
        end as is_actual_holiday


FROM {{ source('raw', 'holidays_events') }}
where date is not null