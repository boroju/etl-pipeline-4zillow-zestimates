import json
from urllib.request import urlopen
import requests
import models as mod
import logging

_SORT_BY = 'BridgeModificationTimestamp'
_ORDER = 'desc'
_MAX_LIMIT_RESULT_SET = 200
_MAX_OFFSET = 10000
_NO_DATA_MESSAGE = 'We are not able to find data using the provided parameters'
_ZESTIMATES_END = '/api/v2/zestimates_v2/zestimates?'
_ACCESS_TOKEN = 'access_token='


class DataDeliveryClientError(Exception):
    pass


class DataDeliveryClient:

    def __init__(self, api_host, api_key):
        self._api_host = api_host
        self._api_key = api_key
        self._api_url = self.api_host + _ZESTIMATES_END + _ACCESS_TOKEN + self._api_key

    # function to get value of _api_host
    def get_api_host(self):
        return self._api_host

    # function to delete _api_host attribute
    def del_api_host(self):
        del self._api_host

    api_host = property(get_api_host)

    # function to get value of _api_host
    def get_api_key(self):
        return self._api_key

    # function to delete _api_host attribute
    def del_api_key(self):
        del self._api_key

    api_key = property(get_api_key)

    # function to get value of _api_url
    def get_api_url(self):
        return self._api_url

    # function to delete _api_url attribute
    def del_api_url(self):
        del self._api_url

    api_url = property(get_api_url)

    def get_zestimates_by_address_parts(self, city=None, state=None, street=None,
                                        postal_code=None, street_number=None) -> mod.ZestimatesBundles:

        params = dict()

        logger = logging.getLogger(__name__)

        if city:
            params['city'] = city
        if state:
            params['state'] = state
        if street:
            params['streetName'] = street
        if street_number:
            params['houseNumber'] = street_number
        if postal_code:
            params['postalCode'] = postal_code

        params['limit'] = _MAX_LIMIT_RESULT_SET
        params['sortBy'] = _SORT_BY
        params['order'] = _ORDER
        params['offset'] = 0

        zestimates_bundles = []
        index = _MAX_LIMIT_RESULT_SET

        response = requests.get(url=self.api_url, params=params)
        data_json = json.loads(response.content)

        if response.ok and data_json['success']:
            index = int(data_json['total'])
            if index > _MAX_LIMIT_RESULT_SET + _MAX_OFFSET:
                index = _MAX_OFFSET

            zestimate_count = 0
            while zestimate_count <= index:
                response = requests.get(
                    url=self.api_url,
                    params=params
                )
                data_json = json.loads(response.content)
                zestimate_partial = mod.ZestimatesBundles.from_json(data_json)
                zestimates_bundles = zestimates_bundles + zestimate_partial.zestimates
                zestimate_count = zestimate_count + _MAX_LIMIT_RESULT_SET
                params['offset'] = zestimate_count

            return mod.ZestimatesBundles(zestimates_bundles)
        else:
            logger.error(_NO_DATA_MESSAGE)
            raise RuntimeError(data_json)

    def get_zestimates_from_random_sample(self, city=None, state=None, address=None,
                                          postal_code=None, street_number=None) -> mod.ZestimatesBundles:

        params = dict()

        logger = logging.getLogger(__name__)

        if city:
            params['city'] = city
        if state:
            params['state'] = state
        if address:
            params['address'] = address
        if street_number:
            params['houseNumber'] = street_number
        if postal_code:
            params['postalCode'] = postal_code

        params['limit'] = _MAX_LIMIT_RESULT_SET
        params['sortBy'] = _SORT_BY
        params['order'] = _ORDER
        params['offset'] = 0

        zestimates_bundles = []
        index = _MAX_LIMIT_RESULT_SET

        response = requests.get(url=self.api_url, params=params)
        data_json = json.loads(response.content)

        if response.ok and data_json['success']:
            index = int(data_json['total'])
            if index > _MAX_LIMIT_RESULT_SET + _MAX_OFFSET:
                index = _MAX_OFFSET

            zestimate_count = 0
            while zestimate_count <= index:
                response = requests.get(
                    url=self.api_url,
                    params=params
                )
                data_json = json.loads(response.content)
                zestimate_partial = mod.ZestimatesBundles.from_json(data_json)
                zestimates_bundles = zestimates_bundles + zestimate_partial.zestimates
                zestimate_count = zestimate_count + _MAX_LIMIT_RESULT_SET
                params['offset'] = zestimate_count

            return mod.ZestimatesBundles(zestimates_bundles)
        else:
            logger.error(_NO_DATA_MESSAGE)
            raise RuntimeError(data_json)
