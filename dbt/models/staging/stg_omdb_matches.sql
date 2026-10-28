select movie_key, run_end_date, match_status, match_reason,
       json_extract_string(response_json, '$.imdbID') as imdb_id,
       json_extract_string(response_json, '$.Title') as omdb_title,
       try_cast(json_extract_string(response_json, '$.Year') as integer) as release_year,
       nullif(trim(split_part(json_extract_string(response_json, '$.Genre'), ',', 1)), 'N/A') as primary_genre,
       try_cast(json_extract_string(response_json, '$.imdbRating') as double) as imdb_rating,
       try_cast(regexp_extract(json_extract_string(response_json, '$.Runtime'), '^(\d+) min$', 1)
                as integer) as runtime_minutes
from {{ source('python', 'omdb_matches') }}
