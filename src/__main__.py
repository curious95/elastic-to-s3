import time
import traceback

import pandas
from elasticsearch import Elasticsearch

from src.configuration import Config


class ElasticExporter:

    def __init__(self):
        self.config = Config()
        self.start_time = time.time()
        self.total_docs = 1000
        self.es = Elasticsearch(['http://' + self.config.es_user + ':' + self.config.es_pass + '@' + self.config.es_host + ':' + self.config.es_port])

    def fetch_csv(self):
        """
        MAKE API CALL TO CLUSTER AND CONVERT
        THE RESPONSE OBJECT TO A LIST OF
        ELASTICSEARCH DOCUMENTS
        """
        print("\nmaking API call to Elasticsearch for", self.total_docs, "documents.")
        response = self.es.search(
            index=self.config.index,
            body={},
            size=self.total_docs
        )

        # grab list of docs from nested dictionary response
        print("putting documents in a list")
        elastic_docs = response["hits"]["hits"]

        print(elastic_docs)

        """
        GET ALL OF THE ELASTICSEARCH
        INDEX'S FIELDS FROM _SOURCE
        """
        #  create an empty Pandas DataFrame object for docs
        docs = pandas.DataFrame()

        # iterate each Elasticsearch doc in list
        print("\ncreating objects from Elasticsearch data.")
        for num, doc in enumerate(elastic_docs):
            # get _source data dict from document
            source_data = doc["_source"]

            # get _id from document
            _id = doc["_id"]

            # create a Series object from doc dict object
            doc_data = pandas.Series(source_data, name=_id)

            # append the Series object to the DataFrame object
            docs = docs.append(doc_data)

        print("\nexporting Pandas objects to CSV Format")

        # export Elasticsearch documents to a CSV file
        docs.to_csv("csv_out.csv", "|", index_label=False)  # CSV delimited by pipe

        print("\n\ntime elapsed:", time.time() - self.start_time)

    def write_s3(self):
        pass


def main():
    elastic_exporter = ElasticExporter()
    try:
        elastic_exporter.fetch_csv()
        elastic_exporter.write_s3()
    except Exception as e:
        traceback.print_exc()
        pass


if __name__ == '__main__':
    main()
