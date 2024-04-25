from django.shortcuts import get_object_or_404, render
import sys
sys.path.append(r"/home/silvhua/custom_python")
# from retrieval_pipeline import *
from django.http import HttpResponse
from django.http import Http404
from django.template import loader

# Create your views here.
def retrieve(request, query, collection_name='test_set_5'):
    # logger = create_function_logger(__name__, parent_logger=None, level=logging.INFO)
    # retriever = Retrieve_Docs(collection_name, top_k=50, logger=logger)
    
    # results_list = retriever.run(query)
    # logger.info(f'Results list: {results_list}')
    # parsed_results_list = []
    # for index, result in enumerate(results_list['retriever_with_embeddings'].get('documents', [])):
    #     parsed_result = result.meta
    #     parsed_result.pop('source_id')
    #     parsed_result['score'] = result.score
    #     parsed_results_list.append(parsed_result)
    # parsed_results_list = get_unique_dicts(parsed_results_list, keys_to_ignore=['score'])
    
    # # logger.info('\n'.join(parsed_results_list))
    # return HttpResponse(parsed_results_list)
    return HttpResponse('Hello')

def index(request):
    return HttpResponse("Hello, world. You're at the semantic search index.")

def my_custom_page_not_found_view(request, exception): 
    message = 'Page not found!'
    return render(request, "404.html", {"message": message})

def test(request): # this works
    text = 'what is update, dawg?'
    return render(request, 'semantic_search/test.html', {'question': text})