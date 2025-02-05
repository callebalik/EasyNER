from db_easyner import EasyNerDB
from db_statistics import (
    calc_article_counts, add_pmid_to_entity_occurrences, update_db_with_entity_occurrence_term_fq, update_tf_idf, get_all_entity_occurrences
)

from database.entity_analysis import record_sentence_entity_cooccurences, get_sentence_ids, get_number_of_sentences, get_sentence_entity_cooccurences


def main():
    # ----------------- paths -----------------
    # db_path = "/proj/berzelius-2021-21/users/x_caoll/EasyNer_ner_output/database6.db"
    db_path = "/proj/berzelius-2021-21/users/x_caoll/EasyNer_ner_output/pnm_dis.db"
    entities_csv_path = "/proj/berzelius-2021-21/users/x_caoll/EasyNer_ner_output/entities.csv"
    cooc_path = "/proj/berzelius-2021-21/users/x_caoll/EasyNer_ner_output/cooc50.csv"
    entity_fq_path = (
        "/home/x_caoll/EasyNer/results/analysis/analysis_phenoma/entity_fq.csv"
    )

    # ----------------- DB Setup -----------------
    db = EasyNerDB(db_path)
    db.create_indexes() # Optimize DB for faster queries
    # db.backup_db()

    # db.optimize_db_performance_parameters()
    # db.optimize_db_performance_for_read()


    # ----------------- DB Manipulation -----------------
    # db.update_entity_name("phenomenon", "PNM")
    # db.update_entity_name("disease", "DIS")

    # db.lowercase_column("entity_occurrences", "entity_text")     # Might not be needed anymore

    # ----------------- DB Statistics -----------------
    # print(get_number_of_articles(db.conn))
    # calc_article_counts(db.conn)
    # add_pmid_to_entity_occurrences(db.conn)

    # update_db_with_entity_occurrence_term_fq(db.conn)
    # get_all_entity_occurrences(db.conn).to_csv(entities_csv_path, index=False)
    # db.record_entity_cooccurrences("DIS", "PNM")

    # get_sentence_entity_cooccurences(db.conn)
    # Run statistics to populate tf-idf values
    # update_tf_idf(db.conn)

    # record_sentence_entity_cooccurences(db.conn, sentence_batch_size=100000)

    # db.sum_cooccurences()
    # print(get_number_of_sentences(db.conn))
    # sentence_batches = get_sentence_ids(conn=db.conn, sentence_batch_size=100000)
    # # Save sentence batches to a file
    # with open("sentence_batches.txt", "w") as f:
    #      for sentence_batch in sentence_batches:
    #          f.write(f"{sentence_batch}\n")

    cooc_path = "cooc50.csv"
    get_sentence_entity_cooccurences(db.conn, named_entity_class_1="PNM", named_entity_class_2="DIS", number_of_entities=50).to_csv(cooc_path, index=False)
    # db.export_cooccurrences(cooc_path, top_n=5000)
    # db.count_entity_fq()

    # db.export_entity_fq(entity_fq_path, "PNM")

    # db.create_indexes()

    db.close()
if __name__ == "__main__":
    main()