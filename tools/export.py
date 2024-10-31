"""Export tables as TSVs."""

import argparse

import duckdb


def main(db_path: str) -> None:
    """Get the annotation from category and severity TSVs."""
    con = duckdb.connect(db_path)
    con.execute("""
    COPY (SELECT prefix
               , image
               , y
               , x
               , h
               , w
               , category
               , level
            FROM l2_annot_pred
           WHERE labeled = 1)
      TO 'data/l2_annot_pred_labeled.csv' (HEADER, DELIMITER '\t');
    """)
    con.execute("""
    COPY (SELECT prefix
               , image
               , y
               , x
               , h
               , w
               , category
               , level
            FROM l2_annot_pred
           WHERE labeled = 0)
      TO 'data/l2_annot_pred_unlabeled.csv' (HEADER, DELIMITER '\t');
    """)

    con.execute("""
    COPY (SELECT * FROM l5_feat_wkt WHERE labeled = 0)
      TO 'data/l5_feat_wkt_pred_unlabeled.csv' (HEADER, DELIMITER '\t');
    """)
    con.execute("""
    COPY (SELECT * FROM l5_feat_wkt WHERE labeled = 1)
      TO 'data/l5_feat_wkt_pred_labeled.csv' (HEADER, DELIMITER '\t');
    """)

    con.execute("""
    COPY (SELECT * FROM l5_img_wkt WHERE labeled = 0)
      TO 'data/l5_img_wkt_pred_unlabeled.csv' (HEADER, DELIMITER '\t');
    """)
    con.execute("""
    COPY (SELECT * FROM l5_img_wkt WHERE labeled = 1)
      TO 'data/l5_img_wkt_pred_labeled.csv' (HEADER, DELIMITER '\t');
    """)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("db_path", type=str)
    args = parser.parse_args()
    main(args.db_path)
