import json
from boto3 import Session
from botocore.session import get_session
from botocore.credentials import InstanceMetadataProvider
from botocore.utils import InstanceMetadataFetcher


class AutoRefreshableSession:
    METHOD = "iam-role"
    DEFAULT_RETRIES = 5
    DEFAULT_METADATA_SERVICE_TIMEOUT = 10  # secs

    def __init__(self, retries=DEFAULT_RETRIES, metadata_service_timeout=DEFAULT_METADATA_SERVICE_TIMEOUT):
        self.instance_metadata_fetcher = InstanceMetadataFetcher()
        self.instance_metadata_fetcher._num_attempts = retries
        self.instance_metadata_fetcher._timeout = metadata_service_timeout
        self.instance_metadata_fetcher._needs_retry_for_credentials = self.needs_retry_for_credentials
        self.instance_metadata_provider = InstanceMetadataProvider(self.instance_metadata_fetcher)

    def check_for_missing_keys(self, required_cred_fields, response):
        print(response.content)
        credentials = json.loads(response.content)
        for field in required_cred_fields:
            if field not in credentials:
                print('Retrieved credentials is missing required field: %s', field)
                return True
        return False

    def needs_retry_for_credentials(self, response):
        return (
                self.instance_metadata_fetcher._is_non_ok_response(response) or
                self.instance_metadata_fetcher._is_empty(response) or
                self.instance_metadata_fetcher._is_invalid_json(response) or
                self.check_for_missing_keys(self.instance_metadata_fetcher._REQUIRED_CREDENTIAL_FIELDS, response)
        )

    def _get(self, region):
        self.session = get_session()
        self.session._credentials = self.instance_metadata_provider.load()
        self.session.set_config_variable("region", region)
        self.autorefresh_session = Session(botocore_session=self.session)
        return self

    def client(self, service_name):
        return self.autorefresh_session.client(service_name=service_name)


def get_autorefresh_session(region):
    session = AutoRefreshableSession()
    return session._get(region)
