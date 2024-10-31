{{ config(materialized='table') }}

SELECT fct.batch
     , fct.prefix
     , fct.image
     , fct.fault_image
     , fct.latitude
     , fct.longitude
     , fct.easting
     , fct.northing
     , fct.feature_id
     , fea.feature_seq
     , fea.cnt_img_feature
     , fea.composite_score
     , fea.ratio
     , fct.wkt
     , fct.distance
  FROM {{ ref('l4_img_feat_annot') }} AS fct
 INNER
  JOIN {{ ref('l4_feat_score') }} AS fea
    ON fct.batch = fea.batch
   AND fct.feature_id = fea.feature_id
