SELECT
    id,
    ville,
    latitude,
    longitude,
    temperature,
    humidite,
    vitesse_vent,
    code_meteo,
    extraction_timestamp,
    DATE(extraction_timestamp) AS date_extraction
FROM {{ source('raw', 'raw_weather') }}