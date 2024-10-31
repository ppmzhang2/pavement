WITH unk AS (
    SELECT batch
         , feature_id
      FROM {{ ref('l4_img_feat_annot') }}
     GROUP BY 1, 2
), ids AS (
    SELECT batch
         , feature_id
         , ROW_NUMBER() OVER (PARTITION BY batch
                                  ORDER BY feature_id) AS feature_seq
      FROM unk
), tmp AS (
    SELECT batch
         , feature_id
         , sum(cnt_img_feature)       AS cnt_img_feature
         , sum(bump_fair)             AS bump_fair
         , sum(bump_poor)             AS bump_poor
         , sum(bump_verypoor)         AS bump_verypoor
         , sum(crack_fair)            AS crack_fair
         , sum(crack_poor)            AS crack_poor
         , sum(crack_verypoor)        AS crack_verypoor
         , sum(depression_fair)       AS depression_fair
         , sum(depression_poor)       AS depression_poor
         , sum(depression_verypoor)   AS depression_verypoor
         , sum(displacement_fair)     AS displacement_fair
         , sum(displacement_poor)     AS displacement_poor
         , sum(displacement_verypoor) AS displacement_verypoor
         , sum(vegetation_fair)       AS vegetation_fair
         , sum(vegetation_poor)       AS vegetation_poor
         , sum(vegetation_verypoor)   AS vegetation_verypoor
         , sum(uneven_fair)           AS uneven_fair
         , sum(uneven_poor)           AS uneven_poor
         , sum(uneven_verypoor)       AS uneven_verypoor
         , sum(pothole_fair)          AS pothole_fair
         , sum(pothole_poor)          AS pothole_poor
         , sum(pothole_verypoor)      AS pothole_verypoor
      FROM {{ ref('l4_prefix_feat') }}
     GROUP BY 1, 2
), cte AS (
     SELECT *
          , bump_fair / cnt_img_feature             AS bump_fair_r
          , bump_poor / cnt_img_feature             AS bump_poor_r
          , bump_verypoor / cnt_img_feature         AS bump_vpoor_r
          , crack_fair / cnt_img_feature            AS crack_fair_r
          , crack_poor / cnt_img_feature            AS crack_poor_r
          , crack_verypoor / cnt_img_feature        AS crack_vpoor_r
          , depression_fair / cnt_img_feature       AS depression_fair_r
          , depression_poor / cnt_img_feature       AS depression_poor_r
          , depression_verypoor / cnt_img_feature   AS depression_vpoor_r
          , displacement_fair / cnt_img_feature     AS displacement_fair_r
          , displacement_poor / cnt_img_feature     AS displacement_poor_r
          , displacement_verypoor / cnt_img_feature AS displacement_vpoor_r
          , vegetation_fair / cnt_img_feature       AS vegetation_fair_r
          , vegetation_poor / cnt_img_feature       AS vegetation_poor_r
          , vegetation_verypoor / cnt_img_feature   AS vegetation_vpoor_r
          , uneven_fair / cnt_img_feature           AS uneven_fair_r
          , uneven_poor / cnt_img_feature           AS uneven_poor_r
          , uneven_verypoor / cnt_img_feature       AS uneven_vpoor_r
          , pothole_fair / cnt_img_feature          AS pothole_fair_r
          , pothole_poor / cnt_img_feature          AS pothole_poor_r
          , pothole_verypoor / cnt_img_feature      AS pothole_vpoor_r
       FROM tmp
)
SELECT ids.batch
     , ids.feature_id
     , ids.feature_seq
     , cte.cnt_img_feature
     , cte.bump_fair
     , cte.bump_poor
     , cte.bump_verypoor
     , cte.crack_fair
     , cte.crack_poor
     , cte.crack_verypoor
     , cte.depression_fair
     , cte.depression_poor
     , cte.depression_verypoor
     , cte.displacement_fair
     , cte.displacement_poor
     , cte.displacement_verypoor
     , cte.vegetation_fair
     , cte.vegetation_poor
     , cte.vegetation_verypoor
     , cte.uneven_fair
     , cte.uneven_poor
     , cte.uneven_verypoor
     , cte.pothole_fair
     , cte.pothole_poor
     , cte.pothole_verypoor
     , (10 * cte.bump_vpoor_r +
        4 * cte.bump_poor_r +
        cte.bump_fair_r) * 500            AS bump_score
     , (10 * cte.crack_vpoor_r +
        4 * cte.crack_poor_r +
        cte.crack_fair_r) * 500           AS crack_score
     , (10 * cte.depression_vpoor_r +
        4 * cte.depression_poor_r +
        cte.depression_fair_r) * 500      AS depression_score
     , (10 * cte.displacement_vpoor_r +
        4 * cte.displacement_poor_r +
        cte.displacement_fair_r) * 500    AS displacement_score
     , (10 * cte.vegetation_vpoor_r +
        4 * cte.vegetation_poor_r +
        cte.vegetation_fair_r) * 500      AS vegetation_score
     , (10 * cte.uneven_vpoor_r +
        4 * cte.uneven_poor_r +
        cte.uneven_fair_r) * 500          AS uneven_score
     , (10 * cte.pothole_vpoor_r +
        4 * cte.pothole_poor_r +
        cte.pothole_fair_r) * 500         AS pothole_score
     , (10 * cte.bump_vpoor_r +
        10 * cte.crack_vpoor_r +
        10 * cte.depression_vpoor_r +
        10 * cte.displacement_vpoor_r +
        10 * cte.vegetation_vpoor_r +
        10 * cte.uneven_vpoor_r +
        10 * cte.pothole_vpoor_r +
        4 * cte.bump_poor_r +
        4 * cte.crack_poor_r +
        4 * cte.depression_poor_r +
        4 * cte.displacement_poor_r +
        4 * cte.vegetation_poor_r +
        4 * cte.uneven_poor_r +
        4 * cte.pothole_poor_r +
        cte.bump_fair_r +
        cte.crack_fair_r +
        cte.depression_fair_r +
        cte.displacement_fair_r +
        cte.vegetation_fair_r +
        cte.uneven_fair_r +
        cte.pothole_fair_r) * 500         AS composite_score
     , (cte.bump_poor_r +
        cte.crack_poor_r +
        cte.depression_poor_r +
        cte.displacement_poor_r +
        cte.vegetation_poor_r +
        cte.uneven_poor_r +
        cte.pothole_poor_r +
        cte.bump_verypoor +
        cte.crack_verypoor +
        cte.depression_verypoor +
        cte.displacement_verypoor +
        cte.vegetation_verypoor +
        cte.uneven_verypoor +
        cte.pothole_verypoor)             AS ratio
  FROM ids
 INNER
  JOIN cte
    ON ids.batch = cte.batch
   AND ids.feature_id = cte.feature_id
