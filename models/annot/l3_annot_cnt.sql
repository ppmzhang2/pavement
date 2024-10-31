{{ config(materialized='table') }}

SELECT 2 AS batch
     , prefix
     , image
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
     , pothole_fair
     , pothole_poor
     , pothole_verypoor
     , uneven_fair
     , uneven_poor
     , uneven_verypoor
     , vegetation_fair
     , vegetation_poor
     , vegetation_verypoor
  FROM {{ ref('l0_annot_gt_cnt') }}
 UNION ALL
SELECT batch
     , prefix
     , image
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
     , pothole_fair
     , pothole_poor
     , pothole_verypoor
     , uneven_fair
     , uneven_poor
     , uneven_verypoor
     , vegetation_fair
     , vegetation_poor
     , vegetation_verypoor
  FROM {{ ref('l3_annot_pred_cnt') }}
