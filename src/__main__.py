import time
import traceback

import pandas
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

from src.configuration import Config


class ElasticExporter:

    CSV_HEADERS = 'source_record_id|record_id|applicant|project_type|address|postcode|city|state|latitude|longitude|' \
                  'pin|department_id|project_brief|project_name|zoning_classification_pre|zoning_classification_post|' \
                  'status|date|applicant_contact|record_link|document_link|contact_phone_number|contact_email|' \
                  'contact_website|parcel_number|block|lot|owner|authority|owner_address|owner_phone|source'

    ROW_MASK = '{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|' \
               '{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}'

    def __init__(self):
        self.config = Config()
        self.start_time = time.time()
        self.total_docs = 1000
        self.es = Elasticsearch(['http://' + self.config.es_user + ':' + self.config.es_pass + '@' + self.config.es_host + ':' + self.config.es_port])

    def fetch_csv(self, index):

        hits = Search().using(client=self.es).index(index).query()

        time.sleep(30)

        with open("output/{}.csv".format(index), "a") as output_file:
            output_file.write(self.CSV_HEADERS)

            for hit in hits.scan():
                row = self.ROW_MASK.format(hit.id, hit.record_id, hit.applicant, hit.project_type, hit.address, hit.postcode,
                                           hit.city, hit.state, hit.latitude, hit.longitude, hit.pin, hit.department_id,
                                           hit.project_brief, hit.project_name, hit.zoning_classification_pre,
                                           hit.zoning_classification_post, hit.status, hit.date, hit.applicant_contact,
                                           hit.record_link, hit.document_link, hit.contact_phone_number, hit.contact_email,
                                           hit.contact_website, hit.parcel_number, hit.block, hit.lot, hit.owner, hit.authority,
                                           hit.owner_address, hit.owner_phone, hit.source)

                output_file.write(row)

            output_file.close()

    def write_s3(self):
        pass


def main():
    elastic_exporter = ElasticExporter()
    try:
        index = ['au_zoning_data', 'us_zoning_data']

        for indx in index:
            elastic_exporter.fetch_csv(indx)

    except Exception as e:
        traceback.print_exc()
        pass


if __name__ == '__main__':
    main()
