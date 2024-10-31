{{ config(materialized='table') }}

WITH lbl AS (
    SELECT batch
         , prefix
      FROM {{ ref('l3_annot_cnt') }}
     GROUP BY 1, 2
), tmp AS (
    SELECT *
         , CASE WHEN bump_verypoor > 0 THEN 'bump_verypoor_'
                WHEN bump_poor > 0 THEN 'bump_poor_'
                WHEN bump_fair > 0 THEN 'bump_fair_'
                ELSE ''
            END AS bump_status
         , CASE WHEN crack_verypoor > 0 THEN 'crack_verypoor_'
                WHEN crack_poor > 0 THEN 'crack_poor_'
                WHEN crack_fair > 0 THEN 'crack_fair_'
                ELSE ''
            END AS crack_status
         , CASE WHEN depression_verypoor > 0 THEN 'depression_verypoor_'
                WHEN depression_poor > 0 THEN 'depression_poor_'
                WHEN depression_fair > 0 THEN 'depression_fair_'
                ELSE ''
            END AS depression_status
         , CASE WHEN displacement_verypoor > 0 THEN 'displacement_verypoor_'
                WHEN displacement_poor > 0 THEN 'displacement_poor_'
                WHEN displacement_fair > 0 THEN 'displacement_fair_'
                ELSE ''
            END AS displacement_status
         , CASE WHEN vegetation_verypoor > 0 THEN 'vegetation_verypoor_'
                WHEN vegetation_poor > 0 THEN 'vegetation_poor_'
                WHEN vegetation_fair > 0 THEN 'vegetation_fair_'
                ELSE ''
            END AS vegetation_status
         , CASE WHEN uneven_verypoor > 0 THEN 'uneven_verypoor_'
                WHEN uneven_poor > 0 THEN 'uneven_poor_'
                WHEN uneven_fair > 0 THEN 'uneven_fair_'
                ELSE ''
            END AS uneven_status
         , CASE WHEN pothole_verypoor > 0 THEN 'pothole_verypoor_'
                WHEN pothole_poor > 0 THEN 'pothole_poor_'
                WHEN pothole_fair > 0 THEN 'pothole_fair_'
                ELSE ''
            END AS pothole_status
      FROM {{ ref('l3_annot_cnt') }}
), img AS (
    SELECT lbl.batch
         , rhs.prefix
         , rhs.image
         , rhs.latitude
         , rhs.longitude
         , rhs.northing
         , rhs.easting
      FROM {{ ref('l0_2023_exif') }} AS rhs
     INNER
      JOIN lbl
        ON rhs.prefix = lbl.prefix
), ann AS (
    SELECT batch
         , prefix
         , image
         , bump_status ||
           crack_status ||
           depression_status ||
           displacement_status ||
           pothole_status ||
           uneven_status ||
           vegetation_status || image AS fault_image
         , bump_fair
         , bump_poor
         , bump_verypoor
         , crack_fair
         , crack_poor
         , crack_verypoor
         , depression_fair
         , depression_poor
         , depression_verypoor
         , displacement_fair
         , displacement_poor
         , displacement_verypoor
         , vegetation_fair
         , vegetation_poor
         , vegetation_verypoor
         , uneven_fair
         , uneven_poor
         , uneven_verypoor
         , pothole_fair
         , pothole_poor
         , pothole_verypoor
      FROM tmp
)
SELECT img.batch
     , img.prefix
     , img.image
     , img.latitude
     , img.longitude
     , img.northing
     , img.easting
     , coalesce(ann.fault_image, img.image)   AS fault_image
     , coalesce(ann.bump_fair, 0)             AS bump_fair
     , coalesce(ann.bump_poor, 0)             AS bump_poor
     , coalesce(ann.bump_verypoor, 0)         AS bump_verypoor
     , coalesce(ann.crack_fair, 0)            AS crack_fair
     , coalesce(ann.crack_poor, 0)            AS crack_poor
     , coalesce(ann.crack_verypoor, 0)        AS crack_verypoor
     , coalesce(ann.depression_fair, 0)       AS depression_fair
     , coalesce(ann.depression_poor, 0)       AS depression_poor
     , coalesce(ann.depression_verypoor, 0)   AS depression_verypoor
     , coalesce(ann.displacement_fair, 0)     AS displacement_fair
     , coalesce(ann.displacement_poor, 0)     AS displacement_poor
     , coalesce(ann.displacement_verypoor, 0) AS displacement_verypoor
     , coalesce(ann.vegetation_fair, 0)       AS vegetation_fair
     , coalesce(ann.vegetation_poor, 0)       AS vegetation_poor
     , coalesce(ann.vegetation_verypoor, 0)   AS vegetation_verypoor
     , coalesce(ann.uneven_fair, 0)           AS uneven_fair
     , coalesce(ann.uneven_poor, 0)           AS uneven_poor
     , coalesce(ann.uneven_verypoor, 0)       AS uneven_verypoor
     , coalesce(ann.pothole_fair, 0)          AS pothole_fair
     , coalesce(ann.pothole_poor, 0)          AS pothole_poor
     , coalesce(ann.pothole_verypoor, 0)      AS pothole_verypoor
  FROM img
  LEFT
  JOIN ann
    ON img.batch = ann.batch
   AND img.prefix = ann.prefix
   AND img.image = ann.image
