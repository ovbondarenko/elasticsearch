# Improving search accuracy in Elastisearch

Elasticsearch ranks documents found in response to a quary by a score, roughly described as a term frequency normalized by the field length. A detailed explanation of how relevancy scores are calculated in Elasticsearch can be found [here](https://qbox.io/blog/practical-guide-elasticsearch-scoring-relevancy)

In this tutorial we experiment with different types of Elasticsearch queries and their performance with different document indexing approaches. We touch on indexing basics [here](https://ovbondarenko.github.io/elasticsearch/index.html).

Reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-multi-match-query.html


```python
# Import dependencies
from elasticsearch import Elasticsearch
import wikipediaapi
from slugify import slugify
from pprint import pprint
```

Start a new Elasticsearch connection:


```python
client = Elasticsearch("http://localhost:9200")
```

Our small Elasticsearch library contains several indices. One group of indices has a default data structure, and the second has a nested structure with predefined field mappings.

Here is all the indices in the database:


```python
client.indices.get_alias("")
```




    {'presidents-of-the-united-states': {'aliases': {}},
     'machine-learning': {'aliases': {}},
     'pandemics': {'aliases': {}},
     'marvel-comics-editors-in-chief': {'aliases': {}},
     '21st-century-american-comedians': {'aliases': {}},
     'american-comics-writers': {'aliases': {}},
     'coronaviridae': {'aliases': {}},
     'natural-language-processing': {'aliases': {}},
     'marvel-comics': {'aliases': {}}}



The index called 'presidents-of-the-united-states' has the fillowing structure:


```python
client.indices.get_mapping('presidents-of-the-united-states')
```




    {'presidents-of-the-united-states': {'mappings': {'properties': {'page_id': {'type': 'long'},
        'source': {'type': 'text',
         'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}},
        'text': {'type': 'text',
         'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}},
        'title': {'type': 'text',
         'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}}}}}}



And here is an example of an index with nested structure:


```python
client.indices.get_mapping('coronaviridae')
```




    {'coronaviridae': {'mappings': {'properties': {'page_id': {'type': 'long'},
        'source': {'type': 'text'},
        'text': {'type': 'nested',
         'properties': {'section_content': {'type': 'text'},
          'section_num': {'type': 'integer'},
          'section_title': {'type': 'text'}}},
        'title': {'type': 'text'}}}}}



We will show how to build queries for both types of indices to get better text search results.

## General query types

Elaticsearch has three types of queries: match, term and range.

- **Match query is a standard query for full text search. Performed against analyzed text.** We will focus on match queries, because they are most useful for free text search.
- Term query is looking for an eaxact match
- Range is used for finding numerical values

We will start with search in indices that have a default data structure. Let us check what this means in terms of mapping.

## Query indices with single text fields


```python
index1 = ['presidents-of-the-united-states', 
         'marvel-comics-editors-in-chief', 
         'marvel-comics',
         'american-comics-writers']
```


```python
question1 = "When stan Lee was born?"
```

### Simple full text search


```python
body1 = {"query": {"match": {"text": question1}}}
```

_search API response to a query includes several metafields, but we are mostly interest in the relevance score ('_score')


```python
docs = client.search(body1, index=index1)

print("Example data strucure")
print("----------------------")
pprint(docs['hits']['hits'][0].keys())
pprint(docs['hits']['hits'][0]['_source'].keys())
```

    Example data strucure
    ----------------------
    dict_keys(['_index', '_type', '_id', '_score', '_source'])
    dict_keys(['title', 'page_id', 'source', 'text'])
    


```python
print(f"Question: {question1}")
print("")
print("Search results:")
print("----------------------")

docs1 = client.search(body1, index="")

for i, doc in enumerate(docs1["hits"]["hits"]):
    title = doc['_source']['title']
    score = doc['_score']
    idx = doc['_index']
    url = doc['_source']['source']
    print(f'Result {i}: {title} | Relevance score {score} | Index {idx}')
    print(f'    Link: {url}')
```

    Question: When stan Lee was born?
    
    Search results:
    ----------------------
    Result 0: Stan Lee | Relevance score 9.7426195 | Index american-comics-writers
        Link: https://en.wikipedia.org/wiki/Stan_Lee
    Result 1: Leon Lazarus | Relevance score 9.079921 | Index american-comics-writers
        Link: https://en.wikipedia.org/wiki/Leon_Lazarus
    Result 2: Roy Thomas | Relevance score 8.832645 | Index american-comics-writers
        Link: https://en.wikipedia.org/wiki/Roy_Thomas
    Result 3: Jim Salicrup | Relevance score 8.592315 | Index american-comics-writers
        Link: https://en.wikipedia.org/wiki/Jim_Salicrup
    Result 4: Danny Fingeroth | Relevance score 8.055832 | Index american-comics-writers
        Link: https://en.wikipedia.org/wiki/Danny_Fingeroth
    Result 5: Jack Kirby | Relevance score 7.9232183 | Index american-comics-writers
        Link: https://en.wikipedia.org/wiki/Jack_Kirby
    Result 6: Steve Ditko | Relevance score 7.883254 | Index american-comics-writers
        Link: https://en.wikipedia.org/wiki/Steve_Ditko
    Result 7: Daniel Keyes | Relevance score 7.6234007 | Index american-comics-writers
        Link: https://en.wikipedia.org/wiki/Daniel_Keyes
    Result 8: Jim Steranko | Relevance score 7.4930425 | Index american-comics-writers
        Link: https://en.wikipedia.org/wiki/Jim_Steranko
    Result 9: Colleen Doran | Relevance score 7.2435718 | Index american-comics-writers
        Link: https://en.wikipedia.org/wiki/Colleen_Doran
    

In this case, the search returns the most relevant article first. However it is goot to run another test.


```python
question2 = "When Barack Obama was inaugurated?"
body2 = {"query": {"match": {"text": question2}}}

print(f"Question: {question2}")
print("")
print("Search results:")
print("----------------------")

docs2 = client.search(body2, index=index1)

for i, doc in enumerate(docs2["hits"]["hits"]):
    title = doc['_source']['title']
    score = doc['_score']
    idx = doc['_index']
    url = doc['_source']['source']
    print(f'Result {i}: {title} | Relevance score {score} | Index {idx}')
    print(f'    Link: {url}')
```

    Question: When Barack Obama was inaugurated?
    
    Search results:
    ----------------------
    Result 0: Jeff Mariotte | Relevance score 12.4784 | Index american-comics-writers
        Link: https://en.wikipedia.org/wiki/Jeff_Mariotte
    Result 1: Ta-Nehisi Coates | Relevance score 11.486142 | Index american-comics-writers
        Link: https://en.wikipedia.org/wiki/Ta-Nehisi_Coates
    Result 2: Amber Benson | Relevance score 9.384043 | Index american-comics-writers
        Link: https://en.wikipedia.org/wiki/Amber_Benson
    Result 3: Eric Millikin | Relevance score 8.75001 | Index american-comics-writers
        Link: https://en.wikipedia.org/wiki/Eric_Millikin
    Result 4: Jason Rubin | Relevance score 8.663446 | Index american-comics-writers
        Link: https://en.wikipedia.org/wiki/Jason_Rubin
    Result 5: Rashida Jones | Relevance score 7.785064 | Index american-comics-writers
        Link: https://en.wikipedia.org/wiki/Rashida_Jones
    Result 6: Barack Obama | Relevance score 7.342956 | Index presidents-of-the-united-states
        Link: https://en.wikipedia.org/wiki/Barack_Obama
    Result 7: Alex Ross | Relevance score 6.783415 | Index american-comics-writers
        Link: https://en.wikipedia.org/wiki/Alex_Ross
    Result 8: Bill Clinton | Relevance score 6.571047 | Index presidents-of-the-united-states
        Link: https://en.wikipedia.org/wiki/Bill_Clinton
    Result 9: Floyd Gottfredson | Relevance score 6.5376973 | Index american-comics-writers
        Link: https://en.wikipedia.org/wiki/Floyd_Gottfredson
    

As you can see, the highest score calculated by Elasticsearch does not always correspond to the most relevant article. How can we improve the results?

### Match phrase query

* All the terms must appear in the field, and in the same order
* Can add custom query analyzer

It may be very useful of we are looking for a specific set of words, like "Barack Obama".


```python
phrase = "Barack Obama"

body3 = \
    {
      "query": {"match_phrase": 
                    {"title": phrase}
               }
    }
docs3 = client.search(body3, index=index1)

print(f"Question: {phrase}")
print("")
print("Search results:")
print("----------------------")
for i, doc in enumerate(docs3['hits']['hits']):
    title = doc['_source']['title']
    score = doc['_score']
    url = doc['_source']['source']
    print(f'Result {i}: {title} | Relevance score {score}')
    print(f'    Link: {url}')
```

    Question: Barack Obama
    
    Search results:
    ----------------------
    Result 0: Barack Obama | Relevance score 7.8283415
        Link: https://en.wikipedia.org/wiki/Barack_Obama
    


```python
phrase = "When Barack Obama was born?"

body3 = \
    {
      "query": {"match_phrase": 
                    {"title": phrase}
               }
    }
docs3 = client.search(body3, index=index1)

print(f"Question: {phrase}")
print("")
print("Search results:")
print("----------------------")
for i, doc in enumerate(docs3['hits']['hits']):
    title = doc['_source']['title']
    score = doc['_score']
    url = doc['_source']['source']
    print(f'Result {i}: {title} | Relevance score {score}')
    print(f'    Link: {url}')
```

    Question: When Barack Obama was born?
    
    Search results:
    ----------------------
    

Clearly, it does not work when our question is has more natural and uncertain form.

### Multi-field search

We can try to improve the scoring by searching in multiple fields. Our indices in this example have only two text fields - text and title, we will search in both of them.


```python
body4 = \
{
  "query": {
    "multi_match" : {
      "query":    question2, 
      "fields": [ "title", "text" ] 
    }
  }
}

docs4 = client.search(body4, index=index1)

print(f"Question: {question2}")
print("")
print("Search results:")
print("----------------------")
for i, doc in enumerate(docs4["hits"]["hits"]):
    title = doc['_source']['title']
    score = doc['_score']
    url = doc['_source']['source']
    print(f'Result {i}: {title} | Relevance score {score}')
    print(f'    Link: {url}')
```

    Question: When Barack Obama was inaugurated?
    
    Search results:
    ----------------------
    Result 0: Jeff Mariotte | Relevance score 12.4784
        Link: https://en.wikipedia.org/wiki/Jeff_Mariotte
    Result 1: Ta-Nehisi Coates | Relevance score 11.486142
        Link: https://en.wikipedia.org/wiki/Ta-Nehisi_Coates
    Result 2: Amber Benson | Relevance score 9.384043
        Link: https://en.wikipedia.org/wiki/Amber_Benson
    Result 3: Eric Millikin | Relevance score 8.75001
        Link: https://en.wikipedia.org/wiki/Eric_Millikin
    Result 4: Jason Rubin | Relevance score 8.663446
        Link: https://en.wikipedia.org/wiki/Jason_Rubin
    Result 5: Barack Obama | Relevance score 7.8283415
        Link: https://en.wikipedia.org/wiki/Barack_Obama
    Result 6: Rashida Jones | Relevance score 7.785064
        Link: https://en.wikipedia.org/wiki/Rashida_Jones
    Result 7: Alex Ross | Relevance score 6.783415
        Link: https://en.wikipedia.org/wiki/Alex_Ross
    Result 8: Bill Clinton | Relevance score 6.571047
        Link: https://en.wikipedia.org/wiki/Bill_Clinton
    Result 9: Floyd Gottfredson | Relevance score 6.5376973
        Link: https://en.wikipedia.org/wiki/Floyd_Gottfredson
    

While the most relevant "Barack Obama" article is ranked 6th instead of 7th, it is still not at the top of the list. The improvement is pretty minor. This is because other articles may have multiples mentions of Barack Obama too.

### Dynamic boosting

Dynamic boosting allows to boost some document fields during query. We can boost the score calculated from the title field, since we believe that the article that has the keywords from our query in its title is more relevant than the one that doesn't. In our Barack Obama example this is the correct assumption.


```python
body5 = \
    {
      "query": {
        "bool": {
          "should": [
            {
              "match": {
                "title": {
                  "query": question2,
                  "boost": 3
                }
              }
            },
            {
              "match": { 
                "text": question2
              }
            }
          ]
        }
      }
    }

docs5 = client.search(body5, index=index1)

print(f"Question: {question2}")
print("")
print("Search results:")
print("----------------------")

for i, doc in enumerate(docs5["hits"]["hits"]):
    title = doc['_source']['title']
    score = doc['_score']
    url = doc['_source']['source']
    print(f'Result {i}: {title} | Relevance score {score}')
    print(f'    Link: {url}')
```

    Question: When Barack Obama was inaugurated?
    
    Search results:
    ----------------------
    Result 0: Barack Obama | Relevance score 30.827982
        Link: https://en.wikipedia.org/wiki/Barack_Obama
    Result 1: Jeff Mariotte | Relevance score 12.4784
        Link: https://en.wikipedia.org/wiki/Jeff_Mariotte
    Result 2: Ta-Nehisi Coates | Relevance score 11.486142
        Link: https://en.wikipedia.org/wiki/Ta-Nehisi_Coates
    Result 3: Amber Benson | Relevance score 9.384043
        Link: https://en.wikipedia.org/wiki/Amber_Benson
    Result 4: Eric Millikin | Relevance score 8.75001
        Link: https://en.wikipedia.org/wiki/Eric_Millikin
    Result 5: Jason Rubin | Relevance score 8.663446
        Link: https://en.wikipedia.org/wiki/Jason_Rubin
    Result 6: Rashida Jones | Relevance score 7.785064
        Link: https://en.wikipedia.org/wiki/Rashida_Jones
    Result 7: Alex Ross | Relevance score 6.783415
        Link: https://en.wikipedia.org/wiki/Alex_Ross
    Result 8: Bill Clinton | Relevance score 6.571047
        Link: https://en.wikipedia.org/wiki/Bill_Clinton
    Result 9: Floyd Gottfredson | Relevance score 6.5376973
        Link: https://en.wikipedia.org/wiki/Floyd_Gottfredson
    

Much better! Now the most useful article s at the top of the list.

### Multifield search with boosing - short version

This is just another way to make a multi-field query with boosting which is more concise then the first one.


```python
body6 = \
{
  "query": {
    "multi_match" : {
      "query":    question2, 
      "fields": [ "title^3", "text" ] 
    }
  }
}

docs6 = client.search(body6, index="")

print(f"Question: {question2}")
print("")
print("Search results:")
print("----------------------")

for i, doc in enumerate(docs6["hits"]["hits"]):
    title = doc['_source']['title']
    score = doc['_score']
    url = doc['_source']['source']
    print(f'Result {i}: {title} | Relevance score {score}')
    print(f'    Link: {url}')
```

    Question: When Barack Obama was inaugurated?
    
    Search results:
    ----------------------
    Result 0: Barack Obama | Relevance score 23.485025
        Link: https://en.wikipedia.org/wiki/Barack_Obama
    Result 1: Jeff Mariotte | Relevance score 12.4784
        Link: https://en.wikipedia.org/wiki/Jeff_Mariotte
    Result 2: Ta-Nehisi Coates | Relevance score 11.486142
        Link: https://en.wikipedia.org/wiki/Ta-Nehisi_Coates
    Result 3: Amber Benson | Relevance score 9.384043
        Link: https://en.wikipedia.org/wiki/Amber_Benson
    Result 4: Eric Millikin | Relevance score 8.75001
        Link: https://en.wikipedia.org/wiki/Eric_Millikin
    Result 5: Jason Rubin | Relevance score 8.663446
        Link: https://en.wikipedia.org/wiki/Jason_Rubin
    Result 6: Rashida Jones | Relevance score 7.785064
        Link: https://en.wikipedia.org/wiki/Rashida_Jones
    Result 7: Alex Ross | Relevance score 6.783415
        Link: https://en.wikipedia.org/wiki/Alex_Ross
    Result 8: Bill Clinton | Relevance score 6.571047
        Link: https://en.wikipedia.org/wiki/Bill_Clinton
    Result 9: Floyd Gottfredson | Relevance score 6.5376973
        Link: https://en.wikipedia.org/wiki/Floyd_Gottfredson
    

Default Elasticsearch tokenizer is N-gram tokenizer. The tokenizer analyzes the input text and produces letter N-grams with N of 1 and 2:
```
{
  "tokenizer": "ngram",
  "text": "Quick Fox"
}

[ Q, Qu, u, ui, i, ic, c, ck, k, "k ", " ", " F", F, Fo, o, ox, x ]

```

## Querying nested fields

Here is the indices in our library with a nested structure:


```python
index2 = [
    'natural-language-processing',
    'machine-learning',
    '21st-century-american-comedians',
    'coronaviridae',
    'pandemics'
]
```
By confining query to a section that we know to be particularly relevant, in this case Summary, we can get good results without boosting.

```python
question3 = "What is natural language processing?"
```


```python
body7={
    "query": {
        "nested":{
            "path":"text",
            "query":{
                "bool": {
                "should": [
                    {"match":{"text.section_content":question3}},
                    {"match":{"text.section_title":"Summary"}},
                    ]
                }
            }
        }
    }
}


docs7 = client.search(body7, index=index2)

print(f"Question: {question3}")
print("")
print("Search results:")
print("----------------------")

for i, doc in enumerate(docs7["hits"]["hits"]):
    title = doc['_source']['title']
    score = doc['_score']
    url = doc['_source']['source']
    pageid =doc['_source']['page_id']
    ind = doc["_index"]
    print(f'Result {i}: {title} | Relevance score {score} | {pageid} | {ind}')
    print(f'    Link: {url}')
```

    Question: What is natural language processing?
    
    Search results:
    ----------------------
    Result 0: Constrained conditional model | Relevance score 7.2747436 | 28255458 | machine-learning
        Link: https://en.wikipedia.org/wiki/Constrained_conditional_model
    Result 1: Documenting Hate | Relevance score 6.7784877 | 54994687 | machine-learning
        Link: https://en.wikipedia.org/wiki/Documenting_Hate
    Result 2: List of datasets for machine-learning research | Relevance score 6.106736 | 49082762 | machine-learning
        Link: https://en.wikipedia.org/wiki/List_of_datasets_for_machine-learning_research
    Result 3: Native-language identification | Relevance score 6.0271287 | 45627703 | machine-learning
        Link: https://en.wikipedia.org/wiki/Native-language_identification
    Result 4: Keith Wann | Relevance score 6.0171547 | 9679949 | 21st-century-american-comedians
        Link: https://en.wikipedia.org/wiki/Keith_Wann
    Result 5: Outline of natural language processing | Relevance score 5.8773565 | 37764426 | natural-language-processing
        Link: https://en.wikipedia.org/wiki/Outline_of_natural_language_processing
    Result 6: Dennis Roady | Relevance score 5.8700037 | 51804512 | 21st-century-american-comedians
        Link: https://en.wikipedia.org/wiki/Dennis_Roady
    Result 7: Natural language processing | Relevance score 5.3063297 | 21652 | natural-language-processing
        Link: https://en.wikipedia.org/wiki/Natural_language_processing
    Result 8: Data pre-processing | Relevance score 5.2916293 | 12386904 | machine-learning
        Link: https://en.wikipedia.org/wiki/Data_pre-processing
    Result 9: AFNLP | Relevance score 5.2510333 | 2891758 | natural-language-processing
        Link: https://en.wikipedia.org/wiki/AFNLP
    

Unfortunately, it didn't quite work out, because the article titled "Natural language processing" came up only on the 8th place. Let's try the multi-field search with the title field boosting:


```python
body7={
  "query": {
    "multi_match" : {
      "query": question3,
      "type": "best_fields",
      "fields": [ "title^3", "text.section_content"],
    }
  }
}

docs7 = client.search(body7, index=index2)

print(f"Question: {question3}")
print("")
print("Search results:")
print("----------------------")

for i, doc in enumerate(docs7["hits"]["hits"]):
    title = doc['_source']['title']
    score = doc['_score']
    url = doc['_source']['source']
    pageid =doc['_source']['page_id']
    ind = doc["_index"]
    print(f'Result {i}: {title} | Relevance score {score} | {pageid} | {ind}')
    print(f'    Link: {url}')
```

    Question: What is natural language processing?
    
    Search results:
    ----------------------
    Result 0: Natural language processing | Relevance score 21.272564 | 21652 | natural-language-processing
        Link: https://en.wikipedia.org/wiki/Natural_language_processing
    Result 1: Studies in Natural Language Processing | Relevance score 16.254246 | 6650456 | natural-language-processing
        Link: https://en.wikipedia.org/wiki/Studies_in_Natural_Language_Processing
    Result 2: Semantic decomposition (natural language processing) | Relevance score 16.254246 | 57932194 | natural-language-processing
        Link: https://en.wikipedia.org/wiki/Semantic_decomposition_(natural_language_processing)
    Result 3: Outline of natural language processing | Relevance score 16.254246 | 37764426 | natural-language-processing
        Link: https://en.wikipedia.org/wiki/Outline_of_natural_language_processing
    Result 4: History of natural language processing | Relevance score 16.254246 | 27837170 | natural-language-processing
        Link: https://en.wikipedia.org/wiki/History_of_natural_language_processing
    Result 5: Natural language | Relevance score 14.911903 | 21173 | natural-language-processing
        Link: https://en.wikipedia.org/wiki/Natural_language
    Result 6: Empirical Methods in Natural Language Processing | Relevance score 14.539296 | 43771647 | natural-language-processing
        Link: https://en.wikipedia.org/wiki/Empirical_Methods_in_Natural_Language_Processing
    Result 7: Data pre-processing | Relevance score 14.150436 | 12386904 | machine-learning
        Link: https://en.wikipedia.org/wiki/Data_pre-processing
    Result 8: Native-language identification | Relevance score 12.701324 | 45627703 | machine-learning
        Link: https://en.wikipedia.org/wiki/Native-language_identification
    Result 9: Controlled natural language | Relevance score 12.6099615 | 563439 | natural-language-processing
        Link: https://en.wikipedia.org/wiki/Controlled_natural_language
    

Great, this approach helped to bring the most relevant document to the top of our list.


```python

```
