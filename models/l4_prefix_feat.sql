{{ config(materialized='table') }}

SELECT batch
     , prefix
     , feature_id
     , count(1)                        AS cnt_img_feature
     , sum(bump_fair)                  AS bump_fair
     , sum(bump_poor)                  AS bump_poor
     , sum(bump_verypoor)              AS bump_verypoor
     , sum(crack_fair)                 AS crack_fair
     , sum(crack_poor)                 AS crack_poor
     , sum(crack_verypoor)             AS crack_verypoor
     , sum(depression_fair)            AS depression_fair
     , sum(depression_poor)            AS depression_poor
     , sum(depression_verypoor)        AS depression_verypoor
     , sum(displacement_fair)          AS displacement_fair
     , sum(displacement_poor)          AS displacement_poor
     , sum(displacement_verypoor)      AS displacement_verypoor
     , sum(vegetation_fair)            AS vegetation_fair
     , sum(vegetation_poor)            AS vegetation_poor
     , sum(vegetation_verypoor)        AS vegetation_verypoor
     , sum(uneven_fair)                AS uneven_fair
     , sum(uneven_poor)                AS uneven_poor
     , sum(uneven_verypoor)            AS uneven_verypoor
     , sum(pothole_fair)               AS pothole_fair
     , sum(pothole_poor)               AS pothole_poor
     , sum(pothole_verypoor)           AS pothole_verypoor
  FROM {{ ref('l4_img_feat_annot') }}
 GROUP BY 1, 2, 3
