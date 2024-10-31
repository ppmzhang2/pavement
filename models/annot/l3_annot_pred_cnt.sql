SELECT batch
     , prefix
     , image
     , sum(CASE WHEN category = 'bump'
                 AND level = 'fair' THEN 1
                ELSE 0
            END)::INTEGER                        AS bump_fair
     , sum(CASE WHEN category = 'bump'
                 AND level = 'poor' THEN 1
                ELSE 0
            END)::INTEGER                        AS bump_poor
     , sum(CASE WHEN category = 'bump'
                 AND level = 'verypoor' THEN 1
                ELSE 0
            END)::INTEGER                        AS bump_verypoor
     , sum(CASE WHEN category = 'crack'
                 AND level = 'fair' THEN 1
                ELSE 0
            END)::INTEGER                        AS crack_fair
     , sum(CASE WHEN category = 'crack'
                 AND level = 'poor' THEN 1
                ELSE 0
            END)::INTEGER                        AS crack_poor
     , sum(CASE WHEN category = 'crack'
                 AND level = 'verypoor' THEN 1
                ELSE 0
            END)::INTEGER                        AS crack_verypoor
     , sum(CASE WHEN category = 'depression'
                 AND level = 'fair' THEN 1
                ELSE 0
            END)::INTEGER                        AS depression_fair
     , sum(CASE WHEN category = 'depression'
                 AND level = 'poor' THEN 1
                ELSE 0
            END)::INTEGER                        AS depression_poor
     , sum(CASE WHEN category = 'depression'
                 AND level = 'verypoor' THEN 1
                ELSE 0
            END)::INTEGER                        AS depression_verypoor
     , sum(CASE WHEN category = 'displacement'
                 AND level = 'fair' THEN 1
                ELSE 0
            END)::INTEGER                        AS displacement_fair
     , sum(CASE WHEN category = 'displacement'
                 AND level = 'poor' THEN 1
                ELSE 0
            END)::INTEGER                        AS displacement_poor
     , sum(CASE WHEN category = 'displacement'
                 AND level = 'verypoor' THEN 1
                ELSE 0
            END)::INTEGER                        AS displacement_verypoor
     , sum(CASE WHEN category = 'vegetation'
                 AND level = 'fair' THEN 1
                ELSE 0
            END)::INTEGER                        AS vegetation_fair
     , sum(CASE WHEN category = 'vegetation'
                 AND level = 'poor' THEN 1
                ELSE 0
            END)::INTEGER                        AS vegetation_poor
     , sum(CASE WHEN category = 'vegetation'
                 AND level = 'verypoor' THEN 1
                ELSE 0
            END)::INTEGER                        AS vegetation_verypoor
     , sum(CASE WHEN category = 'uneven'
                 AND level = 'fair' THEN 1
                ELSE 0
            END)::INTEGER                        AS uneven_fair
     , sum(CASE WHEN category = 'uneven'
                 AND level = 'poor' THEN 1
                ELSE 0
            END)::INTEGER                        AS uneven_poor
     , sum(CASE WHEN category = 'uneven'
                 AND level = 'verypoor' THEN 1
                ELSE 0
            END)::INTEGER                        AS uneven_verypoor
     , sum(CASE WHEN category = 'pothole'
                 AND level = 'fair' THEN 1
                ELSE 0
            END)::INTEGER                        AS pothole_fair
     , sum(CASE WHEN category = 'pothole'
                 AND level = 'poor' THEN 1
                ELSE 0
            END)::INTEGER                        AS pothole_poor
     , sum(CASE WHEN category = 'pothole'
                 AND level = 'verypoor' THEN 1
                ELSE 0
            END)::INTEGER                        AS pothole_verypoor
  FROM {{ ref('l2_annot_pred') }}
 GROUP BY 1, 2, 3
