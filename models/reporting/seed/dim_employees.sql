         select *,
         2 * 3961 * asin(sqrt((sin(radians((lat - 52.398) / 2))) ^ 2 + cos(radians(52.398)) * cos(radians(lat)) * (sin(radians((lon - 4.8776) / 2))) ^ 2)) as distance_from_office
         from {{ ref('seed_employees') }}