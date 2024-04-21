from django.shortcuts import render
import sys
sys.path.append(r"/home/silvhua/custom_python")
from retrieval_pipeline import *
from django.http import HttpResponse

# Create your views here.
def retrieve(request, query, collection_name='test_set_5'):
    logger = create_function_logger(__name__, parent_logger=None, level=logging.INFO)
    retriever = Retrieve_Docs(collection_name, top_k=10, logger=logger)
    
    results_list = retriever.run(query)
    logger.info(f'Results list: {results_list}')
    parsed_results_list = []
    for index, result in enumerate(results_list['retriever_with_embeddings'].get('documents', [])):
        parsed_result = result.meta
        parsed_result.pop('source_id')
        parsed_result['score'] = result.score
        parsed_results_list.append(parsed_result)
    parsed_results_list = get_unique_dicts(parsed_results_list, keys_to_ignore=['score'])
    
    # logger.info('\n'.join(parsed_results_list))
    return HttpResponse(parsed_results_list)
    # return HttpResponse('Hello')

def index(request):
    return HttpResponse("Hello, world. You're at the semantic search index.")