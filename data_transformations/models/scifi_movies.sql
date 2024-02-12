{% set scifi_titles = ["Inception", "Avatar", "Blade Runner 2049", "The Matrix", "Jurassic Park"] %}


SELECT *
FROM {{ ref('films') }}
WHERE title IN (
  {% for scifi_title in scifi_titles %}
    '{{ scifi_title }}'{% if not loop.last %},{% endif %}
  {% endfor %}
)