from db_easyner import EasyNerDB
from db_statistics import (
    calc_article_counts, add_pmid_to_entity_occurrences, update_db_with_entity_occurrence_term_fq, update_tf_idf
)


def main():
    # ----------------- paths -----------------
    # db_path = "/proj/berzelius-2021-21/users/x_caoll/EasyNer_ner_output/database6.db"
    db_path = "/proj/berzelius-2021-21/users/x_caoll/EasyNer_ner_output/pnm_dis.db"
    cooc_path = "/proj/berzelius-2021-21/users/x_caoll/EasyNer_ner_output/cooc50.csv"
    entity_fq_path = (
        "/home/x_caoll/EasyNer/results/analysis/analysis_phenoma/entity_fq.csv"
    )

    # ----------------- DB Setup -----------------
    db = EasyNerDB(db_path)
    db.create_indexes() # Optimize DB for faster queries
    # db.backup_db()

    # ----------------- DB Manipulation -----------------
    # db.update_entity_name("phenomenon", "PNM")
    # db.update_entity_name("disease", "DIS")

    # db.lowercase_column("entity_occurrences", "entity_text")     # Might not be needed anymore

    # ----------------- DB Statistics -----------------
    # print(get_number_of_articles(db.conn))
    # calc_article_counts(db.conn)
    # add_pmid_to_entity_occurrences(db.conn)

    # update_db_with_entity_occurrence_term_fq(db.conn)

    db.record_entity_cooccurrences("DIS", "PNM")

    # Run statistics to populate tf-idf values
    update_tf_idf(db.conn)


    # db.sum_cooccurences()


    # db.export_cooccurrences(cooc_path, top_n=5000)
    # db.count_entity_fq()

    # db.export_entity_fq(entity_fq_path, "PNM")

    # db.create_indexes()
if __name__ == "__main__":
    main()