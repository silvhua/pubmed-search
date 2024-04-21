from haystack import Pipeline
from haystack_integrations.components.retrievers.chroma import ChromaEmbeddingRetriever
from haystack.components.embedders import SentenceTransformersTextEmbedder
from haystack_integrations.document_stores.chroma import ChromaDocumentStore
import sys
sys.path.append(r"/home/silvhua/custom_python")
from silvhua import *
from Custom_Logger import *

class Retrieve_Docs:

    def __init__(
            self, collection_name, document_store=None, top_k=1,
            logger=None, logging_level=logging.INFO
        ):
        self.logger = create_function_logger(__name__, parent_logger=logger, level=logging_level)

        self.logger.info(f'***Instantiating `Retrieve_Docs`***')
        if document_store is None:
            document_store = ChromaDocumentStore( # https://docs.haystack.deepset.ai/reference/integrations-chroma#chromadocumentstore
                collection_name=collection_name, 
                persist_path='../data/processed/'
                )
        # https://docs.haystack.deepset.ai/docs/chromaembeddingretriever
        retriever = ChromaEmbeddingRetriever(document_store=document_store, top_k=top_k)
        self.retrieval_pipeline = Pipeline()
        self.retrieval_pipeline.add_component("text_embedder", SentenceTransformersTextEmbedder(
            # model="thenlper/gte-large"
            ))
        self.retrieval_pipeline.add_component("retriever_with_embeddings", retriever)
        self.retrieval_pipeline.connect("text_embedder", "retriever_with_embeddings")

    def run(self, query):
        """
        Get the n_results nearest neighbor embeddings for provided query.
        The `score` attribute is the distance between embeddings. https://docs.trychroma.com/reference/Collection#query
        """
        self.logger.info(f'***Running retrieval pipeline***')
        result = self.retrieval_pipeline.run({"text_embedder": {"text": query}})
        return result

def get_unique_dicts(my_list):
    """
    Returns a list of unique dictionaries from a list of dictionaries.

    Parameters:
    - my_list (list): A list of dictionaries.

    Returns:
    - unique_dicts (list): A list of unique dictionaries.
    """
    unique_elements = list(set(tuple(d.items()) for d in my_list))
    unique_dicts = [dict(e) for e in unique_elements]
    return unique_dicts

if __name__ == "__main__":
    logger = create_function_logger(__name__, parent_logger=None, level=logging.INFO)
    logger.info(f'System arguments: {sys.argv[1:]}')
    collection_name = sys.argv[2] if len(sys.argv) > 2 else 'test_set_5'
    retriever = Retrieve_Docs(collection_name, top_k=10, logger=logger)
    max_queries = 5
    query_number = 0
    # allow for user interaction
    while True & (query_number < max_queries):
        query = input('Enter query: ')
        if query == 'exit':
            break
        results_list = retriever.run(query)
        logger.info(f'Results list: {results_list}')
        parsed_results_list = []
        for index, result in enumerate(results_list['retriever_with_embeddings'].get('documents', [])):
            parsed_result = result.meta
            parsed_result.pop('source_id')
            parsed_result['score'] = result.score
            parsed_results_list.append(parsed_result)
        parsed_results_list = get_unique_dicts(parsed_results_list)
        logger.info('\n'.join(parsed_results_list))
        query_number += 1
        
