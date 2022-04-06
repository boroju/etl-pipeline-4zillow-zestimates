import json
from models import Zestimate, ZestimatesBundles
from utilities import CaseConverters


class ZestimatesEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Zestimate):
            json_dict = {
                'id': obj.id,
                'city': obj.city,
                'zpid': obj.zpid,
                'state': obj.state,
                'address': obj.address,
                'minus30': obj.minus30,
                'Latitude': obj.latitude,
                'Longitude': obj.longitude,
                'timestamp': obj.timestamp,
                'zestimate': obj.zestimate,
                'zillowUrl': obj.zillowurl,
                'lowPercent': obj.lowpercent,
                'postalCode': obj.postalcode,
                'streetName': obj.streetname,
                'unitNumber': obj.unitnumber,
                'unitPrefix': obj.unitprefix,
                'highPercent': obj.highpercent,
                'houseNumber': obj.housenumber,
                'zipPlusFour': obj.zipplusfour,
                'streetSuffix': obj.streetsuffix,
                'directionPrefix': obj.directionprefix,
                'directionSuffix': obj.directionsuffix,
                'rentalTimestamp': obj.rentaltimestamp,
                'rentalZestimate': obj.rentalzestimate,
                'rentalLowPercent': obj.rentallowpercent,
                'rentalHighPercent': obj.rentalhighpercent,
                'houseNumberFraction': obj.housenumberfraction,
                'BridgeModificationTimestamp': obj.bridgemodificationtimestamp,
                'forecastStandardDeviation': obj.forecaststandarddeviation,
                'url': obj.url
            }
            return json_dict
        return json.JSONEncoder.default(self, obj)


class ZestimatesBundlesEncoder(json.JSONEncoder):
    def default(self, obj: ZestimatesBundles):
        if isinstance(obj, ZestimatesBundles):
            json_dict = {}
            for k, v in obj.__dict__.items():
                if v not in [None, False]:
                    if v:
                        v = 1
                    json_dict[CaseConverters.snake_to_pascal(k)] = v
            return json_dict

        return json.JSONEncoder.default(self, obj)

