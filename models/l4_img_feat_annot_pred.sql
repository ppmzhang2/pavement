{{ config(materialized='table') }}

SELECT lab.labeled
     , lab.prefix
     , lab.image
     , lab.fault_image
     , lab.latitude
     , lab.longitude
     , lab.easting
     , lab.northing
     , bri.feature_id
     , ftr.wkt
     , bri.distance
     , lab.bump_fair
     , lab.bump_poor
     , lab.bump_verypoor
     , lab.crack_fair
     , lab.crack_poor
     , lab.crack_verypoor
     , lab.depression_fair
     , lab.depression_poor
     , lab.depression_verypoor
     , lab.displacement_fair
     , lab.displacement_poor
     , lab.displacement_verypoor
     , lab.vegetation_fair
     , lab.vegetation_poor
     , lab.vegetation_verypoor
     , lab.uneven_fair
     , lab.uneven_poor
     , lab.uneven_verypoor
     , lab.pothole_fair
     , lab.pothole_poor
     , lab.pothole_verypoor
  FROM {{ ref('l4_img_annot_pred') }} AS lab
 INNER
  JOIN {{ ref('l0_2023_map_image_feature') }} AS bri
    ON lab.prefix = bri.prefix
   AND lab.image = bri.image
 INNER
  JOIN {{ ref('l0_2023_feat') }} AS ftr
    ON bri.feature_id = ftr.id
 WHERE bri.distance IS NOT NULL
   AND bri.distance < 10
