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
            self, collection_name, document_store=None, logger=None, logging_level=logging.INFO
        ):
        self.logger = create_function_logger(__name__, parent_logger=logger, level=logging_level)

        self.logger.info(f'***Instantiating `Retrieve_Docs`***')
        if document_store is None:
            document_store = ChromaDocumentStore( # https://docs.haystack.deepset.ai/reference/integrations-chroma#chromadocumentstore
                collection_name=collection_name, 
                persist_path='../data/processed/'
                )
        # https://docs.haystack.deepset.ai/docs/chromaembeddingretriever
        retriever = ChromaEmbeddingRetriever(document_store=document_store, top_k=3)
        self.retrieval_pipeline = Pipeline()
        self.retrieval_pipeline.add_component("text_embedder", SentenceTransformersTextEmbedder(
            # model="thenlper/gte-large"
            ))
        self.retrieval_pipeline.add_component("retriever_with_embeddings", retriever)
        self.retrieval_pipeline.connect("text_embedder", "retriever_with_embeddings")

    def run(self, query):
        self.logger.info(f'***Running retrieval pipeline***')
        result = self.retrieval_pipeline.run({"text_embedder": {"text": query}})
        return result
    
if __name__ == "__main__":
    logger = create_function_logger(__name__, parent_logger=None, level=logging.INFO)
    logger.info(f'System arguments: {sys.argv[1:]}')
    collection_name = sys.argv[2] if len(sys.argv) > 2 else 'test_set'
    retriever = Retrieve_Docs(collection_name, logger=logger)
    max_queries = 5
    query_number = 0
    # allow for user interaction
    while True & (query_number < max_queries):
        query = input('Enter query: ')
        if query == 'exit':
            break
        results_list = retriever.run(query)
        logger.info(f'Results list: {results_list}')
        messages = []
        for index, result in enumerate(results_list['retriever_with_embeddings'].get('documents', [])):
            messages.append(f'Result {index+1}: {result}')
        logger.info('\n'.join(messages))
        query_number += 1
        
