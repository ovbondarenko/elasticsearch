# Elasticsearch index

An [Elasicsearch](https://www.elastic.co/what-is/elasticsearch) index is a collection of one or more documents (JSON objects) with the same storage settings and data structrue.

Elasticsearch can automatically assign datatypes to the documents' fields. However, when documents have more complex structure, i.e., they are nested, it is importand do define mapping (of data struture of documents hat will be stored in an index) before hand.

In this example we change the way the documents are stored to improve search relevance.

In this tutorial we use an [official Elasticsearch python](https://elasticsearch-py.readthedocs.io/en/master/) wrapper around the Elasticsearch's REST API.


```python
# Import dependencies
from elasticsearch import Elasticsearch
import wikipediaapi
from slugify import slugify
from pprint import pprint
```

Create a connection instance:


```python
client = Elasticsearch("http://localhost:9200")
```

## Working with _indices and _cat APIs

Information about existing indices can be retrieved from Elasticsearch _indices API. Here is how we can get document counts in individual indices, multiple indices and all indicies:


```python
indices = list(client.indices.get_alias("_all").keys())
indices
```




    ['coronaviridae',
     'marvel-comics-editors-in-chief',
     'natural-language-processing',
     'presidents-of-the-united-states',
     '21st-century-american-comedians',
     'pandemics',
     'marvel-comics',
     'american-comics-writers',
     'machine-learning']



Refresh index and count documents in each of existing indices:


```python
client.indices.refresh('marvel-comics')
for i in indices:
    print(i, client.cat.count(i, params={"format": "json"}))
```

    coronaviridae [{'epoch': '1585531259', 'timestamp': '01:20:59', 'count': '16'}]
    marvel-comics-editors-in-chief [{'epoch': '1585531259', 'timestamp': '01:20:59', 'count': '15'}]
    natural-language-processing [{'epoch': '1585531259', 'timestamp': '01:20:59', 'count': '179'}]
    presidents-of-the-united-states [{'epoch': '1585531259', 'timestamp': '01:20:59', 'count': '50'}]
    21st-century-american-comedians [{'epoch': '1585531259', 'timestamp': '01:20:59', 'count': '1969'}]
    pandemics [{'epoch': '1585531259', 'timestamp': '01:20:59', 'count': '1'}]
    marvel-comics [{'epoch': '1585531259', 'timestamp': '01:20:59', 'count': '15'}]
    american-comics-writers [{'epoch': '1585531259', 'timestamp': '01:20:59', 'count': '940'}]
    machine-learning [{'epoch': '1585531259', 'timestamp': '01:20:59', 'count': '219'}]
    


```python
# Count documents in multiple indices
client.cat.count(['marvel-comics', 'coronaviridae'], params={"format": "json"})
```




    [{'epoch': '1585531340', 'timestamp': '01:22:20', 'count': '31'}]




```python
# Count documents in all indices
client.cat.count("_all", params={"format": "json"})
```




    [{'epoch': '1585531352', 'timestamp': '01:22:32', 'count': '3404'}]




```python
# Check an index mapping - default
pprint(client.indices.get_mapping("marvel-comics"))
```

    {'marvel-comics': {'mappings': {'properties': {'page_id': {'type': 'long'},
                                                   'source': {'fields': {'keyword': {'ignore_above': 256,
                                                                                     'type': 'keyword'}},
                                                              'type': 'text'},
                                                   'text': {'fields': {'keyword': {'ignore_above': 256,
                                                                                   'type': 'keyword'}},
                                                            'type': 'text'},
                                                   'title': {'fields': {'keyword': {'ignore_above': 256,
                                                                                    'type': 'keyword'}},
                                                             'type': 'text'}}}}}
    


```python
# Check an index mapping - nested
pprint(client.indices.get_mapping("coronaviridae"))
```

    {'coronaviridae': {'mappings': {'properties': {'page_id': {'type': 'long'},
                                                   'source': {'type': 'text'},
                                                   'text': {'properties': {'section_content': {'type': 'text'},
                                                                           'section_num': {'type': 'integer'},
                                                                           'section_title': {'type': 'text'}},
                                                            'type': 'nested'},
                                                   'title': {'type': 'text'}}}}}
    


```python
# Delete all documents in a single index
client.indices.delete("coronaviridae")
```




    {'acknowledged': True}




```python
# Check currently available indices
client.indices.get_alias("_all")
```




    {'marvel-comics': {'aliases': {}},
     '21st-century-american-comedians': {'aliases': {}},
     'natural-language-processing': {'aliases': {}},
     'presidents-of-the-united-states': {'aliases': {}},
     'machine-learning': {'aliases': {}},
     'marvel-comics-editors-in-chief': {'aliases': {}},
     'american-comics-writers': {'aliases': {}},
     'pandemics': {'aliases': {}}}




```python
client.indices.delete(index=["marvel-comics-editors-in-chief", "21st-century-american-comedians"])
client.indices.get_alias("_all")
```




    {'marvel-comics': {'aliases': {}},
     'machine-learning': {'aliases': {}},
     'pandemics': {'aliases': {}},
     'presidents-of-the-united-states': {'aliases': {}},
     'american-comics-writers': {'aliases': {}},
     'natural-language-processing': {'aliases': {}}}




```python
client.indices.delete("_all")
client.indices.get_alias("_all")
```




    {}



## Create new indices and documents

We will load wikipedia articles from selected categories using [Wikipedia-API](https://pypi.org/project/Wikipedia-API/) and create a few Elasticsearch indices with default index settings and mapping.
Elasticsearch is smart enough to figure out what datatype each field of the document is, as long as the data structure of the document s not too complex.

Let us concider an example of a document with the following structure:

```
        {title='some string'
         page_id='numerical datatype'
         source='string, url'
         text='some string'}

```


```python
categories = ['Presidents of the United States', 
              'Marvel Comics', 
              'American comics writers',
              'Marvel Comics editors-in-chief']
```


```python
class Document:
    
    def __init__(self):
        self.title = ''
        self.page_id = None
        self.source = ''
        self.text = ''
        
    def __if_exists(self, page_id, index=""):
        '''
        A private method to check if the article already exists in the database
        with a goal to avoid duplication
        '''
        
        return client.search(index=index, 
                             body={"query": 
                                   {"match": 
                                    {"page_id": page_id}
                                   }})['hits']['total']['value']
        
    def insert(self, title, page_id, url, text, index):
        ''' Add a new document to the index'''
        
        self.title=title
        self.page_id=page_id
        self.source=url
        self.text=text
        self.body = {'title': self.title,
            'page_id': self.page_id,
            'source':self.source,
            'text': self.text}
        
        if self.__if_exists(page_id) == 0:
        
            try:
                client.index(index=index, body=self.body)
#                 print(f"Sucess! The article {self.title} was added to index {index}")
            except error:
                print("Something went wrong", error)
                
        else:
            print(f"Article {self.title} is already in the database")
```


```python
def simple_wiki_doc(category):
    
    if type(category) is not list: category = [ category ]

    wiki_wiki = wikipediaapi.Wikipedia('en')
    
    for c in category:

        cat = wiki_wiki.page(f"Category:{c}")

        for key in cat.categorymembers.keys():
            page = wiki_wiki.page(key)

            if not "Category:" in page.title:
                
                doc = Document()
                doc.insert(page.title, page.pageid, page.fullurl, page.text, index=slugify(c))
                
simple_wiki_doc(categories)
```

    Article Los Bros Hernandez is already in the database
    Article Andy Wachowski is already in the database
    Article C. B. Cebulski is already in the database
    Article Gerry Conway is already in the database
    Article Tom DeFalco is already in the database
    Article Stan Lee is already in the database
    Article Jim Shooter is already in the database
    Article Joe Simon is already in the database
    Article Roy Thomas is already in the database
    Article Len Wein is already in the database
    Article Marv Wolfman is already in the database
    

Check if the indices were added and what is the document count in each index:


```python
indices1 = list(client.indices.get_alias("_all").keys())
for i in indices1:
    print(i, client.cat.count(i, params={"format": "json"}))
```

    marvel-comics [{'epoch': '1585532669', 'timestamp': '01:44:29', 'count': '14'}]
    presidents-of-the-united-states [{'epoch': '1585532669', 'timestamp': '01:44:29', 'count': '48'}]
    marvel-comics-editors-in-chief [{'epoch': '1585532669', 'timestamp': '01:44:29', 'count': '5'}]
    american-comics-writers [{'epoch': '1585532669', 'timestamp': '01:44:29', 'count': '937'}]
    

# Indexing nested documents


The default data structure may not be sufficient. We need our documents to be store in sections to improve search accuracy, like so:


```
        {title='text'
         page_id='numerical datatype'
         source='text'
         text= [{
                 'section_title': 'text',
                 'section_num': 'integer'
                 'section_content': 'text'
                 },
                 {
                 'section_title': 'text',
                 'section_num': 'integer'
                 'section_content': 'text'
                 },
                 {
                 'section_title': 'text',
                 'section_num': 'integer'
                 'section_content': 'text'
                 },
            }
```
It this a nested data structure. To be able to add such a document to an index, the data structure of the index has to be mapped first. We will havo to create an index before we add documents document with teh following call:

```
    client.indices.create(index='pandemics', body={"mappings":mapping})

```


```python
mapping = {
    "properties": {
        
            "text": {
                "type": "nested",
                "properties":{
                    "section_num": {"type":"integer"},
                    "section_title": {"type":"text"},
                    "section_content": {"type":"text"}
                }
            },
        
            "title": {
                "type": "text"
            },
        
            "source": {
                "type": "text"
            },
        
            "page_id": {
                "type": "long"
            },
            
        }
    }
```

### Create new indices with nested data structure

Some wikipedia articles in categories are large and may have multiple levels of subsections. We chose to parse the data from full text instead of drawing sections and subsections from wikipedia API to achieve uniform depth of nested dictionaries within a single index.


```python
# Load wikipedia articles from the following categories:
categories2 = ['Machine learning',
              'Natural language processing',
              'Coronaviridae',
              '21st-century American comedians',
              'Pandemics']
```


```python
def parse_article(article):
    ''' Parce wikipedia articles from the full article text'''
    
    text = article.text
    # get section titles for the existing sections
    section_titles = [sec.title for sec in article.sections]
    
    # initiate the sections dictionary with a summary (0th section) 
    sections = [{'section_num': 0},
                {'section_title': "Summary"},
                {'section_content': article.summary}]
    
    for i, title in enumerate(section_titles[::-1]):

        num = len(section_titles)-i
        if len(text.split(f"\n\n{title}")) == 2:
            section_dict = {"section_num": num,
                            "section_title": title,
                            "section_content": text.split(f"\n\n{title}")[-1]}
            sections.append(section_dict)
            text = text.split(f"\n\n{title}")[0]
        else:
            pass
            
        
    return sections
```


```python
def search_insert_wiki(category, mapping):
    
    if type(category) is not list: category = [ category ]

    wiki_wiki = wikipediaapi.Wikipedia('en')
    
    for c in category:
        
        try:
                    
            '''Create and empty index with predefined data structure'''
            client.indices.create(index=slugify(c), body={"mappings":mapping})
            
            '''Access the list of wikipedia articles in category c'''
            cat = wiki_wiki.page(f"Category:{c}")
            
            ''' Parse and add articles in the category to database'''
            for key in cat.categorymembers.keys():
                page = wiki_wiki.page(key)

                if not "Category:" in page.title:
                ''' Build a dictionary and add in to the index'''

                    text = parse_article(page)
                    doc = Document()
                    doc.insert(page.title, page.pageid, page.fullurl, text, index=slugify(c))


        except error:
            '''Skip category if it alredy exists in indices'''
            print("Something went wrong", error)
            
search_insert_wiki(categories2, mapping)
```

    Article Training, validation, and test sets is already in the database
    Article Bag-of-words model is already in the database
    Article Deeplearning4j is already in the database
    Article Document classification is already in the database
    Article Documenting Hate is already in the database
    Article Grammar induction is already in the database
    Article Multimodal sentiment analysis is already in the database
    Article Native-language identification is already in the database
    Article Semantic folding is already in the database
    Article Coronavirus is already in the database
    Article Sean Clements is already in the database
    Article Ernest Cline is already in the database
    Article Hayes Davenport is already in the database
    Article Adam Felber is already in the database
    Article Rashida Jones is already in the database
    Article Taran Killam is already in the database
    Article Steve Melcher is already in the database
    Article Patton Oswalt is already in the database
    Article Brian Posehn is already in the database
    Article Carlos Saldaña is already in the database
    Article Edward Savio is already in the database
    Article Rob Schrab is already in the database
    Article Larry Siegel is already in the database
    Article Kevin Smith is already in the database
    Article Johns Hopkins Center for Health Security is already in the database
    


```python
client.indices.get_alias("_all")
```




    {'marvel-comics': {'aliases': {}},
     'presidents-of-the-united-states': {'aliases': {}},
     'marvel-comics-editors-in-chief': {'aliases': {}},
     'american-comics-writers': {'aliases': {}}}




```python
# Delete all documents in a single index
client.indices.delete("machine-learning")
```




    {'acknowledged': True}


