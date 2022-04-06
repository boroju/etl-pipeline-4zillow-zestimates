from typing import List


class Zestimate:

    def __init__(self,
                 id=None,
                 city=None,
                 zpid=None,
                 state=None,
                 address=None,
                 minus30=None,
                 latitude=None,
                 longitude=None,
                 timestamp=None,
                 zestimate=None,
                 zillowurl=None,
                 lowpercent=None,
                 postalcode=None,
                 streetname=None,
                 unitnumber=None,
                 unitprefix=None,
                 highpercent=None,
                 housenumber=None,
                 zipplusfour=None,
                 streetsuffix=None,
                 directionprefix=None,
                 directionsuffix=None,
                 rentaltimestamp=None,
                 rentalzestimate=None,
                 rentallowpercent=None,
                 rentalhighpercent=None,
                 housenumberfraction=None,
                 bridgemodificationtimestamp=None,
                 # coordinates,
                 forecaststandarddeviation=None,
                 url=None):
        self.id = id
        self.city = city
        self.zpid = zpid
        self.state = state
        self.address = address
        self.minus30 = minus30
        self.latitude = latitude
        self.longitude = longitude
        self.timestamp = timestamp
        self.zestimate = zestimate
        self.zillowurl = zillowurl
        self.lowpercent = lowpercent
        self.postalcode = postalcode
        self.streetname = streetname
        self.unitnumber = unitnumber
        self.unitprefix = unitprefix
        self.highpercent = highpercent
        self.housenumber = housenumber
        self.zipplusfour = zipplusfour
        self.streetsuffix = streetsuffix
        self.directionprefix = directionprefix
        self.directionsuffix = directionsuffix
        self.rentaltimestamp = rentaltimestamp
        self.rentalzestimate = rentalzestimate
        self.rentallowpercent = rentallowpercent
        self.rentalhighpercent = rentalhighpercent
        self.housenumberfraction = housenumberfraction
        self.bridgemodificationtimestamp = bridgemodificationtimestamp
        # self.coordinates = coordinates
        self.forecaststandarddeviation = forecaststandarddeviation
        self.url = url

    @classmethod
    def from_json(cls, json_dict: dict):
        return cls(
            json_dict['id'],
            json_dict['city'],
            json_dict['zpid'],
            json_dict['state'],
            json_dict['address'],
            json_dict['minus30'],
            json_dict['Latitude'],
            json_dict['Longitude'],
            json_dict['timestamp'],
            json_dict['zestimate'],
            json_dict['zillowUrl'],
            json_dict['lowPercent'],
            json_dict['postalCode'],
            json_dict['streetName'],
            json_dict['unitNumber'],
            json_dict['unitPrefix'],
            json_dict['highPercent'],
            json_dict['houseNumber'],
            json_dict['zipPlusFour'],
            json_dict['streetSuffix'],
            json_dict['directionPrefix'],
            json_dict['directionSuffix'],
            json_dict['rentalTimestamp'],
            json_dict['rentalZestimate'],
            json_dict['rentalLowPercent'],
            json_dict['rentalHighPercent'],
            json_dict['houseNumberFraction'],
            json_dict['BridgeModificationTimestamp'],
            # json_dict['Coordinates'],
            json_dict['forecastStandardDeviation'],
            json_dict['url'])


class ZestimatesBundles:

    def __init__(self, zestimates: List[Zestimate] = None):
        self.zestimates = zestimates

    @classmethod
    def from_json(cls, json_dict: dict):
        zestimates = [Zestimate.from_json(l) for l in json_dict['bundle']]
        return cls(zestimates)

    def __iter__(self):
        for zestimate in self.zestimates:
            yield zestimate
