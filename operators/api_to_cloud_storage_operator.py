from flatten_json import flatten
from os import path
import logging
import json
import boa

from airflow.utils.decorators import apply_defaults
from airflow.models import HttpOperator, SkipMixin
from airflow.hooks import S3Hook


class API2CloudStorage(HttpOperator, SkipMixin):
    """
    Base operator for all API to Cloud Storage operators.

    Standard output formatting is newline delimited json.

    Included methods take care of the following:
        - Rate Limiting
        - Pagination
        - Method Wrapping
        - Field Filtering
        - Subtable Mapping
        - Output Formatting

    Currently support Cloud Storage includes:
        - AWS S3
    """

    template_fields = ('cs_bucket', 'cs_key')

    @apply_defaults
    def __init__(self,
                 api_conn_id,
                 api_object,
                 cs_conn_id,
                 cs_bucket,
                 cs_key,
                 cs_type='s3',
                 api_args={},
                 skip_if_null=False):
        self.api_conn_id = api_conn_id
        self.api_object = api_object
        self.cs_conn_id = cs_conn_id
        self.cs_bucket = cs_bucket
        self.cs_key = cs_key
        self.cs_type = cs_type.lower()
        self.api_args = api_args
        self.skip_if_null = skip_if_null

        if cs_type.lower() not in ('s3',):
            raise Exception('Only AWS S3 is currently supported.')

    def execute(self, context):
        self.split = path.splitext(self.s3_key)
        self.total_output_files = 0
        self.context = context

        hook = self.hookMapper()
        output = self.paginate_data(hook, self.methodMapper(self.api_object))

        if len(output) == 0 or output is None:
            pass
        else:
            for e in output:
                for k, v in e.items():
                    if k == 'core':
                        key = '{0}_core_final{1}'.format(self.split[0],
                                                         self.split[1])
                    else:
                        key = '{0}_{1}_final{2}'.format(self.split[0],
                                                        k,
                                                        self.split[1])

                    self.outputManager(hook,
                                       v,
                                       key,
                                       self.cs_bucket)

        logging.info('Total Output File Count: ' + str(self.total_output_files))

    def hookMapper(self):
        if self.cs_type == 's3':
            return S3Hook(self.s3_conn_id)

    def outputManager(self, hook, output, key, bucket):
        """
        This method handles the output of the data.
        """
        if self.total_output_files == 0:
            logging.info("No records pulled.")
            if self.skip_if_null:
                downstream_tasks = self.context['task'].get_flat_relatives(upstream=False)

                logging.info('Skipping downstream tasks...')
                logging.debug("Downstream task_ids %s", downstream_tasks)

                if downstream_tasks:
                    self.skip(self.context['dag_run'],
                              self.context['ti'].execution_date,
                              downstream_tasks)
        else:
            logging.info('Logging {0} to ...'.format(key))

            output = [flatten(e) for e in output]
            output = '\n'.join([json.dumps({boa.constrict(k): v
                               for k, v in i.items()}) for i in output])

            if self.cs_type == 's3':
                hook.load_string(
                    string_data=str(output),
                    key=key,
                    bucket_name=bucket,
                    replace=True
                )
                hook.connection.close()

                self.total_output_files += 1

    def paginate_data(self,
                      hook,
                      endpoint,
                      page_variable,
                      page_size=1000,
                      offset=None,
                      company_id=None,
                      campaign_id=None):
        """
        This method takes care of request building and pagination.
        It retrieves 100 at a time and continues to make
        subsequent requests until it retrieves less than 100 records.
        """
        output = []

        final_payload = dict

        final_payload[page_variable] = page_size

        for param in self.api_args:
            final_payload[param] = self.api_args[param]

        logging.info('FINAL PAYLOAD: ' + str(final_payload))

        response = hook.run(endpoint, final_payload).json()
        if not response:
            logging.info('Resource Unavailable.')
            return
        else:
            output.extend([e for e in response[self.api_object]])

        output = self.subTableMapper(output)

        return output

    def methodMapper(self, api_object, company_id=None, campaign_id=None):
        """
        This method maps the desired object to the relevant endpoint.

        Example:
        "workflows": "automation/v3/workflows"
        """
        mapping = {}

        return mapping[api_object]

    def subTableMapper(self, output):
        """
        This mapper expects a list of either dictionaries
        or string values as specified in the 'split' value
        of the mapping and then outputs them to a new object.

        Example:
            {'name': 'contacts',
             'split': 'addresses',
             'retained': [{"contact_id": "contactId"}]
             }

        """
        mapping = []

        def process_record(record, mapping):
            final_returnable_dict = {}

            def getByDotNotation(obj, ref):
                val = obj
                try:
                    for key in ref.split('.'):
                        val = val[key]
                except:
                    val = False
                return val

            for entry in mapping:
                returnable_list = []
                subtable_data = getByDotNotation(record, entry['split'])
                if ((entry['name'] == self.api_object) and subtable_data):
                    final_key_split = entry['split'].lower().replace('.', '_')
                    for item in subtable_data:
                        returnable_dict = {}
                        if isinstance(item, dict):
                            returnable_dict = item
                        elif isinstance(item, str) or isinstance(item, int):
                            returnable_dict[final_key_split] = item
                        for item in entry['retained']:
                            for k, v in item.items():
                                try:
                                    returnable_dict[v] = record[k]
                                except KeyError:
                                    logging.info(record)
                                    logging.info(returnable_dict[v])
                        returnable_list.append(returnable_dict)

                if returnable_list:
                    final_returnable_dict[entry['split']] = returnable_list

                final_returnable_dict['core'] = record

            return final_returnable_dict

        def process_data(output):
            output = [process_record(record, mapping) for record in output]
            output_list = []
            output_dict = {}
            output_dict['core'] = [e.pop('core') for e in output]
            output_list.append(output_dict)
            for entry in mapping:
                output_dict = {}
                if (entry['name'] == self.api_object):
                    output_dict[entry['split']] = [e.pop(entry['split']) for e in output
                                                   if (entry['split'] in list(e.keys()))]
                    output_dict[entry['split']] = [item for sublist in output_dict[entry['split']]
                                                   for item in sublist]
                    if not output_dict[entry['split']]:
                        del output_dict[entry['split']]
                    output_list.append(output_dict)
            output_list = [e for e in output_list if e]

            return output_list

        return process_data(output)

    def filterMapper(self, record):
        """
        This method strips out unnecessary objects (i.e. ones
        that are duplicated in other core objects).

        Example:
            {'name': 'commits',
             'filtered': 'author',
             'retained': ['id']
             }
        """
        mapping = []

        def process(record, mapping):
            """
            This method processes the data according to the above mapping.
            There are a number of checks throughout as the specified filtered
            object and desired retained fields will not always exist in each
            record.
            """

            for entry in mapping:
                # Check to see if the filtered value exists in the record
                if (entry['name'] == self.api_object)\
                 and (entry['filtered'] in list(record.keys())):
                    # Check to see if any retained fields are desired.
                    # If not, delete the object.
                    if entry['retained']:
                        for retained_item in entry['retained']:
                            # Check to see the filterable object exists in the
                            # specific record. This is not always the case.
                            # Check to see the retained field exists in the
                            # filterable object.
                            if record[entry['filtered']] is not None\
                             and retained_item in list(record[entry['filtered']].keys()):
                                # Bring retained field to top level of
                                # object with snakecasing.
                                record["{0}_{1}".format(entry['filtered'],
                                                        retained_item)] = \
                                    record[entry['filtered']][retained_item]
                    if record[entry['filtered']] is not None:
                        del record[entry['filtered']]

            return record

        return process(record, mapping)
