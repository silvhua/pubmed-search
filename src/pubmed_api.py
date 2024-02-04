import sys
sys.path.append(r"C:\Users\silvh\OneDrive\lighthouse\Ginkgo coding\content-summarization\src")
sys.path.append(r"C:\Users\silvh\OneDrive\lighthouse\custom_python")
import re
import os
import string
import pandas as pd
import requests
from article_processing import create_text_dict_from_folder
from orm_summarize import *
api_key = os.getenv('api_ncbi') # Pubmed API key

### These scripts populate data in the sources table with data from the Pubmed API.

def search_article(title, publication, api_key, verbose=False):
    """
    Search for article title in PubMed database.

    Parameters:
    - title (str): article title
    - api_key (str): NCBI API key

    Returns:
    response (str): Article metadata from PubMed database if present. Otherwise, returns list of PMIDs.
    """
    base_url = f'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi'
    title_without_not = re.sub(r'not', '', title)
    if api_key:
        base_url += f'&api_key={api_key}'
    if publication:
        search_term = f'({title_without_not} [ti]) AND ({publication} [ta])'
    else:
        search_term = f'{title_without_not} [ti]'
    params = {
        'db': 'pubmed',
        'term': search_term,
        'retmax': 5,
        'retmode': 'json'
    }
    print(f'Search term: {search_term}')

    response = requests.get(base_url, params=params)
    data = response.json()

    cleaned_title = re.sub(r'</?[ib]>', '', title) # remove bold and italic html tags
    cleaned_title = re.sub(r'[^a-zA-Z0-9 ]', '', cleaned_title).lower().strip()
    cleaned_title = re.sub(r"\u2010", '', cleaned_title)
    try:
        id_list = data['esearchresult']['idlist']
        if id_list:
            for index in range(len(id_list)):
                result = retrieve_citation(id_list[index], api_key).decode('utf-8')
                cleaned_result = re.sub(r'[^a-zA-Z0-9 <>/]', '', result).lower().strip() 
                result_title_match = re.search(r'<articletitle>(.*?)</articletitle>', cleaned_result)
                if result_title_match:
                    result_title = result_title_match.group(1)
                    cleaned_result_title = re.sub(r'</?[ib]>', '', result_title)
                    cleaned_result_title = re.sub(r'/(?![^<>]*>)', '', cleaned_result_title) # Remove any / that is not within html tag
                    cleaned_result_title = re.sub(r'[^a-zA-Z0-9 <>/]', '', cleaned_result_title).lower().strip()
                    if index == 0:
                        first_cleaned_result = cleaned_result
                        first_result_title = result_title
                        first_cleaned_result_title = cleaned_result_title
                else:
                    cleaned_result_title = cleaned_result
                if cleaned_title == cleaned_result_title:
                    if verbose:
                        print(f'Match found for {title}: PMID = {id_list[index]}.')
                    return result
                else:
                    continue
            if cleaned_title != cleaned_result_title:
                print(f'Warning: Article title not found in PMIDs.')
                print(f'Check these PMIDs: {id_list}')
                print(f'\tInput title: {title.lower().strip()}')
                print(f'\tResult title: {first_result_title if first_result_title else first_cleaned_result}')
                print(f'\tCleaned input title: {cleaned_title}')
                print(f'\tCleaned first result title: {first_cleaned_result_title}\n') 
                result = retrieve_citation(id_list[0], api_key).decode('utf-8')
            return result     
    except Exception as error: 
        print(f'Response: \n{data}')
        exc_type, exc_obj, tb = sys.exc_info()
        file = tb.tb_frame
        lineno = tb.tb_lineno
        filename = file.f_code.co_filename
        print(f'\tAn error occurred on line {lineno} in {filename}: {error}')    
        print('Article not found.')
        print(f'\tInput title: {title.lower().strip()}')
        return ''.join([id for id in id_list]) 
    
def retrieve_citation(article_id, api_key):
    """
    Retrieve article metadata from PubMed database.
    """
    base_url = f'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi'
    if api_key:
        base_url += f'&api_key={api_key}'
    params = {
        'db': 'pubmed',
        'id': article_id
    }

    response = requests.get(base_url, params=params)
    return response.content

def extract_pubmed_details(record_string):
    """
    Helper function called by `pubmed_details_by_title` to parse article metadata from PubMed database.
    """
    authors = re.findall(r'<Author ValidYN="Y".*?><LastName>(.*?)</LastName><ForeName>(.*?)</ForeName>', record_string)
    formatted_authors = ', '.join(['{} {}'.format(author[1], author[0]) for author in authors])

    # Extract publication year
    publication_year = re.search(r'<PubDate><Year>(\d{4})</Year>', record_string)
    publication_year = publication_year.group(1) if publication_year else ''
    publication_month = re.search(r'<PubDate>.*?<Month>(Aug)</Month>.*?</PubDate>', record_string)
    publication_month = publication_month.group(1) if publication_month else ''

    # Extract article title
    article_title = re.search(r'<ArticleTitle>(.*?)</ArticleTitle>', record_string)
    article_title = article_title.group(1) if article_title else ''

    # Extract journal title
    journal_title = re.search(r'<Title>(.*?)</Title>', record_string)
    journal_title = journal_title.group(1) if journal_title else ''

    # Extract journal volume
    journal_volume = re.search(r'<Volume>(.*?)</Volume>', record_string)
    journal_volume = journal_volume.group(1) if journal_volume else ''

    # Extract journal issue
    journal_issue = re.search(r'<Issue>(.*?)</Issue>', record_string)
    journal_issue = journal_issue.group(1) if journal_issue else ''

    # Extract start page
    start_page = re.search(r'<StartPage>(.*?)</StartPage>', record_string)
    start_page = start_page.group(1) if start_page else ''

    # Extract end page
    end_page = re.search(r'<EndPage>(.*?)</EndPage>', record_string)
    end_page = end_page.group(1) if end_page else ''

    # Extract ELocationID
    doi = re.search(r'<ELocationID.*?EIdType="doi".*?>(.*?)</ELocationID>', record_string)
    doi = doi.group(1) if doi else ''

    abstract_matches = re.findall(r'(<AbstractText.*?>.*?</AbstractText>)', record_string)
    print(f'Number of abstract sections: {len(abstract_matches)}')
    if len(abstract_matches) > 1:
        cleaned_abstract_sections = []
        for match in abstract_matches:
            clean_match = re.sub(r'<AbstractText.*?((?:Label=".*")?.*?>.*)</AbstractText>', r'\1', match)
            clean_match = re.sub(r'(?: Label="(.*?)")?.*?>(.*)', r'\1: \2', clean_match)
            cleaned_abstract_sections.append(clean_match)
            
        abstract = ''.join([f'{group}<br>' for group in cleaned_abstract_sections])
    else:
        abstract = re.sub(r'<AbstractText.*?>(.*?)</AbstractText>', r'\1', abstract_matches[0])  if abstract_matches else ''
        
    # Extract MeshHeadingList
    MeshHeadingList = re.search(r'<MeshHeadingList>(.*?)</MeshHeadingList>', record_string)
    MeshHeadingList = MeshHeadingList.group(1) if MeshHeadingList else ''
    
    return {
        'pubmed_title': article_title,
        'abstract': abstract,
        'journal': journal_title,
        'authors': formatted_authors,
        'year': publication_year,
        'month': publication_month,
        'pub_volume': journal_volume,
        'pub_issue': journal_issue,
        'start_page': start_page,
        'end_page': end_page,
        'doi': doi,
        'mesh_headings': MeshHeadingList
    }


def pubmed_details_by_title(title, publication, api_key):
    """
    Search for article title in PubMed database and return article details.

    Parameters:
    - title (str): article title
    - api_key (str): NCBI API key

    Returns:
    article_details (dict): Article metadata from PubMed database if present. Otherwise, returns list of PMIDs.
    """
    record_string = search_article(title, publication, api_key)
    # return record_string
    if record_string:
        article_details = extract_pubmed_details(record_string)
        return article_details
    else:
        return None

def add_pubmed_details(text_df, api_key):
    """
    Add the article metadata to a DataFrame containing article title and text.

    Parameters:
    - text_df (pd.DataFrame): DataFrame containing article title and text.
    - api_key (str): NCBI API key

    Returns:
    DataFrame with added PubMed details for each article.
    """
    article_details_list = []
    for index in text_df.index:
        article = text_df.loc[index, 'title']
        text = str(text_df.loc[index, 'body'])
        publication = text_df.loc[index, 'publication']
        article_details = pubmed_details_by_title(article, publication, api_key)
        if article_details:
            article_details['text'] = text
            article_details_list.append(article_details)
        else:
            article_details_list.append({
                'pubmed_title': article,
                'abstract': '',
                'journal': publication,
                'authors': '',
                'year': '',
                'month': '',
                'pub_volume': '',
                'pub_issue': '',
                'start_page': '',
                'end_page': '',
                'doi': '',
                'text': text,
                'mesh_headings': ''
            })
    article_details_df = pd.DataFrame(article_details_list)
    return pd.concat([text_df.reset_index(drop=True), article_details_df], axis=1)

def compare_columns(df, col1='title', col2='pubmed_title'):
    """
    Compare two columns in a DataFrame. Drop the second column if the two columns are identical.
    Otherwise, return the dataframe with new column with the comparison results, 
    where `True` indicates a mismatch.

    Parameters:
    - df (pd.DataFrame): DataFrame containing the two columns to be compared.
    - col1 (str): Name of the first column to be compared.
    - col2 (str): Name of the second column to be compared.

    Returns:
    DataFrame with added column containing the comparison results.
    """
    # Remove punctuation and special characters
    remove_punct = lambda text: re.sub(f'[{string.punctuation}]', '', text)
    col1 = df[col1].apply(remove_punct)
    col2 = df[col2].apply(remove_punct)

    # Convert to lowercase and remove white spaces
    clean_text = lambda text: text.lower().strip()
    col1 = col1.apply(clean_text)
    col2 = col2.apply(clean_text)

    # Perform the comparison
    comparison = col1 != col2
    if sum(comparison) == 0:
        df = df.drop(columns=['pubmed_title'])
    else:
        df['flag_title'] = comparison
        flagged_indices = df[df['flag_title'] == True].index
        for index in flagged_indices:
            print(f'Flagged: ')
            print(f'\tArticle title: {df.loc[index, "title"]}')
            print(f'\tPubMed title: {df.loc[index, "pubmed_title"]}')
            print()
    
    return df

def create_sources_table(text_df, col1='title', col2='pubmed_title'):
    references_df = add_pubmed_details(text_df, api_key)

    references_df = compare_columns(references_df, col1=col1, col2=col2)
    return references_df

######## These functions are no longer needed

def create_feed_table(article_dict, col1='title', col2='pubmed_title', section=None):
    text_df = pd.DataFrame(article_dict).transpose()
    feed_df = add_pubmed_details(text_df, api_key, section=section)

    feed_df = compare_columns(feed_df, col1=col1, col2=col2)
    return feed_df

def initialize_text_df(folder_path, encoding='ISO-8859-1', subset=None):
    """
    Create a DataFrame from a folder containing text files.

    Parameters:
    - folder_path (str): Path to folder containing text files.
    - encoding (str): Encoding of the text files.
    - subset (int): Number of text files to be read. If None, read all files.

    Returns:
    DataFrame containing the text files.
    """
    text_dict = create_text_dict_from_folder(folder_path, encoding, subset)
    text_df = pd.Series(text_dict, index=text_dict.keys())
    return text_df

def parse_fulltext(folder_path, title_pattern=r'^(.*)\n*.+', encoding='ISO-8859-1', subset=None):
    # Initialize empty lists to store the captured groups
    titles = []
    bodies = []
    
    text_df = initialize_text_df(folder_path, encoding, subset)
    # Iterate over each element in the series
    for text in text_df:
        # print(text)
        # Apply the regular expression pattern
        title_match = re.search(title_pattern, text)
        
        # Extract the capture groups and append them to the lists
        if title_match:
            titles.append(title_match.group(1))
            body = re.sub(title_pattern, '', text)
            bodies.append(body.strip())
            
        else:
            titles.append(None)
            bodies.append(None)
    
    # Create a new DataFrame from the captured groups
    df = pd.DataFrame({ 'title': titles, 'text': bodies })
    
    return df

