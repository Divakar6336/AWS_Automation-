from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3

host = 'search-test-domain-r2ribjaxm7dmvocqcthyruv4im.us-east-2.es.amazonaws.com'
region = 'us-east-2'
service = 'es'
credentials = boto3.Session().get_credentials()


def operations(es, operation):

    if operation == "fetch":
        query_type = input("Please enter the query type(currently supporting "
                           "-> simple, regexp, match, match_phrase, join etc):")
        if query_type == 'simple':
            get_operation(es)
        else:
            search_operation(es, query_type)
    elif operation == "insert":
        insert_operation(es)
    elif operation == "delete":
        print("Oops! we are sorry. Delete is not recommended")
    else:
        print("Invalid Operation or not supported at the moment")


def get_operation(es):
    index_name = input("Provide Index Name:")
    doc_type = input("Doc Type:")
    doc_id = input("Id:")
    try:
        data = es.get(index=index_name, doc_type=doc_type, id=doc_id)
        print(data['_source'])
    except Exception as e:
        print(e)


def search_operation(es, query_type):
    index_name = input("Provide Index Name:")
    try:
        if query_type in ["match" or "match_phrase" or "term" or "regexp" or "range"]:
            field = input("field or key:")
            value = input("match:")
            data = es.search(index=index_name, body={"query": {query_type: {field: value}}})

        elif query_type == "match_all":
            data = es.search(index=index_name, body={"query": {query_type: {}}})
            for hit in data['hits']['hits']:
                print(hit["_source"])

        elif query_type == "join":
            data = es.search(index=index_name, body={"query": {"bool"}})

        else:
            print("Oops this operations is not supported by script at the moment. We will try to incorporate it.")

        print(data)
    except Exception as e:
        print(e)


def insert_operation(es):

    index_name = input("Provide Index Name:")
    doc_type = input("Doc Type:")
    doc_id = int(input("Id:"))
    for _ in range(int(input("No of records:"))):
        document = {}
        for _ in range(int(input("No of fields:"))):
            k = input("field or key:")
            v = input("value:")
            document[k] = v
            print(document)
        try:
            es.index(index=index_name, doc_type=doc_type, id=doc_id, body=document)
            print('Record inserted successfully')
            doc_id += 1
            continue
        except Exception as e:
            print(e)
            break


def main():
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
    es = Elasticsearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )
    operation = input("Welcome to the Elastic Search in AWS. Please tell us the operation(fetch, insert, delete):")
    operations(es, operation)
    print("Thanks for using Script")


if __name__ == "__main__":
    main()


