from .db_main import EasyNerDBHandler

db = EasyNerDBHandler()
# db.analysis.merge_duplicate_aggregated_eos_batched_with_progressbar()
# db.analysis.optimized_update_entity_summary_ids(queue_size=60, batch_size=50000, num_reader_threads=2)
# db.analysis.count_entity_cooccurrences_multithreaded()
# db.analysis.aggregate_entity_cooccurrences_multithreaded()

db.co.update_unique_document_counts()
# db.analysis.co_calc_pmi()