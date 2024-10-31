{{ config(materialized='table') }}

SELECT sco.labeled
     , sco.feature_id
     , sco.feature_seq
     , sco.cnt_img_feature
     , sco.composite_score
     , sco.ratio
     , ftr.wkt
     , ftr.road
  FROM {{ ref('l4_feat_score') }} AS sco
 INNER
  JOIN {{ ref('l0_2023_feat') }} AS ftr
    ON sco.feature_id = ftr.id
