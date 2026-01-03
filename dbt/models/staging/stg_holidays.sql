-- holidays_COLUMNS= date,type,locale,locale_name,description,transferred



SELECT

    cast(data as date) as holiday_date,
    type as holiday_type,
    locale as holiday_locale,
    locale_name,
    description as holiday_description,
    coalesce(cast(transferred as boolean), false) as holiday_transferred

FROM {{ source('raw', 'holidays') })
where date is not null;