{{ config(materialized='table') }}

SELECT 1                                          AS category
     , 1                                          AS batch
     , REGEXP_EXTRACT(image, '_h\d+_(\d+)_', 1)   AS prefix
     , REGEXP_EXTRACT(image, '_\d+_(\w+\..+)', 1) AS image
     , REGEXP_EXTRACT(image, '_y(\d+)_', 1)       AS y
     , REGEXP_EXTRACT(image, '_x(\d+)_', 1)       AS x
     , REGEXP_EXTRACT(image, '_h(\d+)_', 1)       AS h
     , REGEXP_EXTRACT(image, '_w(\d+)_', 1)       AS w
     , class
     , label_top1
     , score_top1
     , label_top2
     , score_top2
  FROM {{ ref('l0_annot_pred_labeled_cls_category') }}
 UNION ALL
SELECT 0                                          AS category
     , 1                                          AS batch
     , REGEXP_EXTRACT(image, '_h\d+_(\d+)_', 1)   AS prefix
     , REGEXP_EXTRACT(image, '_\d+_(\w+\..+)', 1) AS image
     , REGEXP_EXTRACT(image, '_y(\d+)_', 1)       AS y
     , REGEXP_EXTRACT(image, '_x(\d+)_', 1)       AS x
     , REGEXP_EXTRACT(image, '_h(\d+)_', 1)       AS h
     , REGEXP_EXTRACT(image, '_w(\d+)_', 1)       AS w
     , class
     , label_top1
     , score_top1
     , label_top2
     , score_top2
  FROM {{ ref('l0_annot_pred_labeled_cls_severity') }}
 UNION ALL
SELECT 1                                          AS category
     , 0                                          AS batch
     , REGEXP_EXTRACT(image, '_h\d+_(\d+)_', 1)   AS prefix
     , REGEXP_EXTRACT(image, '_\d+_(\w+\..+)', 1) AS image
     , REGEXP_EXTRACT(image, '_y(\d+)_', 1)       AS y
     , REGEXP_EXTRACT(image, '_x(\d+)_', 1)       AS x
     , REGEXP_EXTRACT(image, '_h(\d+)_', 1)       AS h
     , REGEXP_EXTRACT(image, '_w(\d+)_', 1)       AS w
     , class
     , label_top1
     , score_top1
     , label_top2
     , score_top2
  FROM {{ ref('l0_annot_pred_unlabeled_cls_category') }}
 UNION ALL
SELECT 0                                          AS category
     , 0                                          AS batch
     , REGEXP_EXTRACT(image, '_h\d+_(\d+)_', 1)   AS prefix
     , REGEXP_EXTRACT(image, '_\d+_(\w+\..+)', 1) AS image
     , REGEXP_EXTRACT(image, '_y(\d+)_', 1)       AS y
     , REGEXP_EXTRACT(image, '_x(\d+)_', 1)       AS x
     , REGEXP_EXTRACT(image, '_h(\d+)_', 1)       AS h
     , REGEXP_EXTRACT(image, '_w(\d+)_', 1)       AS w
     , class
     , label_top1
     , score_top1
     , label_top2
     , score_top2
  FROM {{ ref('l0_annot_pred_unlabeled_cls_severity') }}
