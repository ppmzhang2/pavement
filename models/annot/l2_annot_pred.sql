{{ config(materialized='table') }}

WITH category AS (
    SELECT labeled
         , prefix
         , image
         , x
         , y
         , w
         , h
         , class
         , label_top1
         , (score_top1 * 100)::INTEGER AS score_top1
         , label_top2
         , (score_top2 * 100)::INTEGER AS score_top2
      FROM {{ ref('l1_annot_pred_cls') }}
     WHERE category = 1
), severity AS (
    SELECT labeled
         , prefix
         , image
         , x
         , y
         , w
         , h
         , class
         , label_top1
         , (score_top1 * 100)::INTEGER AS score_top1
         , label_top2
         , (score_top2 * 100)::INTEGER AS score_top2
      FROM {{ ref('l1_annot_pred_cls') }}
     WHERE category = 0
), map_cat AS (
    SELECT class      AS class
         , label_top1 AS label
      FROM category
     GROUP BY 1, 2
), map_lvl AS (
    SELECT class      AS class
         , label_top1 AS label
      FROM severity
     GROUP BY 1, 2
)
SELECT cat.labeled
     , cat.prefix
     , cat.image
     , cat.x
     , cat.y
     , cat.w
     , cat.h
     , ct1.class      AS category
     , cat.score_top1 AS score_cate_top1
     , ct2.class      AS class_cate_top2
     , cat.score_top2 AS score_cate_top2
     , lv1.class      AS level
     , lvl.score_top1 AS score_lvl_top1
     , lv2.class      AS class_lvl_top2
     , lvl.score_top2 AS score_lvl_top2
  FROM category AS cat
 INNER
  JOIN severity AS lvl
    ON cat.labeled = lvl.labeled
   AND cat.prefix = lvl.prefix
   AND cat.image = lvl.image
   AND cat.x = lvl.x
   AND cat.y = lvl.y
   AND cat.w = lvl.w
   AND cat.h = lvl.h
 INNER
  JOIN map_cat AS ct1
    ON cat.label_top1 = ct1.label
 INNER
  JOIN map_cat AS ct2
    ON cat.label_top2 = ct2.label
 INNER
  JOIN map_lvl AS lv1
    ON lvl.label_top1 = lv1.label
 INNER
  JOIN map_lvl AS lv2
    ON lvl.label_top2 = lv2.label
