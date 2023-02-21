import frost_sta_client as fsc
from .query_functions import get_entity_list
from geojson import Point
from frost_sta_client.model.ext.entity_list import EntityList
from frost_sta_client.model.ext.unitofmeasurement import UnitOfMeasurement
from frost_sta_client.utils import transform_entity_to_json_dict


class FrostClient():

    OBSERVATION_TYPES = {
        'OM_CategoryObservation': 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_CategoryObservation', # URI
        'OM_CountObservation': 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_CountObservation', # integer
        'OM_Measurement': 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement', # double
        'OM_Observation': 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Observation', # Any
        'OM_TruthObservation': 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_TruthObservation' # boolean
    }

    def __init__(self, url=None, username='', password=''):
        auth_handler = fsc.AuthHandler(username, password)
        self.service = fsc.SensorThingsService(url, auth_handler)
        self.list_callback=None
        self.step_size=None

    @property
    def service(self):
        return self._service

    @service.setter
    def service(self, value):
        if value is None:
            self._service = value
            return
        if not isinstance(value, fsc.SensorThingsService):
            raise ValueError('service should be of type SensorThingsService!')
        self._service = value

    @property
    def list_callback(self):
        return self._list_callback

    @list_callback.setter
    def list_callback(self, value):
        if value is None:
            self._list_callback = value
            return
        if not callable(value):
            raise ValueError('Callback should be callable!')
        self._list_callback = value

    @property
    def step_size(self):
        return self._step_size

    @step_size.setter
    def step_size(self, value):
        self._step_size = value

    def get_locations(self, id='', name='', description='', relations=None, **kwargs):
        return get_entity_list(
            self.service.locations(),
            callback=self.list_callback,
            step_size=self.step_size,
            id=id,
            name=name,
            description=description,
            relations=relations,
            **kwargs
        )

    def get_datastreams(self, id='', name='', description='', relations=None, **kwargs):
        return get_entity_list(
            self.service.datastreams(),
            callback=self.list_callback,
            step_size=self.step_size,
            id=id,
            name=name,
            description=description,
            relations=relations,
            **kwargs
        )

    def get_observed_properties(self, id='', name='', description='', relations=None, **kwargs):
        return get_entity_list(
            self.service.observed_properties(),
            callback=self.list_callback,
            step_size=self.step_size,
            id=id,
            name=name,
            description=description,
            relations=relations,
            **kwargs
        )

    def get_things(self, id='', name='', description='', relations=None, **kwargs):
        return get_entity_list(
            self.service.things(),
            callback=self.list_callback,
            step_size=self.step_size,
            id=id,
            name=name,
            description=description,
            relations=relations,
            **kwargs
        )

    def get_sensors(self, id='', name='', description='', relations=None, **kwargs):
        return get_entity_list(
            self.service.sensors(),
            callback=self.list_callback,
            step_size=self.step_size,
            id=id,
            name=name,
            description=description,
            relations=relations,
            **kwargs
        )

    def get_observations(self, relations=None, start='', end='', lower_limit=None, upper_limit=None, **kwargs):
        return get_entity_list(
            self.service.observations(),
            callback=self.list_callback,
            step_size=self.step_size,
            relations=relations,
            start=start,
            end=end,
            lower_limit=lower_limit,
            upper_limit=upper_limit,
            **kwargs
        )

    def create_location(self,
            name='',
            description='',
            encoding_type='',
            properties=None,
            location=None,
            things=None,
            historical_locations=None,
            **kwargs):

        if not isinstance(location, Point):
            if isinstance(location, tuple) or isinstance(location, list):
                location = Point(tuple(location))
            elif isinstance(location, dict):
                location = Point((location.get('x'), location.get('y')))
            else:
                raise TypeError("location must be a geojson Point, tuple, list, or dict with keys 'x' and 'y'")
    
        if encoding_type == '':
            encoding_type = 'application/vnd.geo+json'
        if things is None and 'thing' in kwargs.keys():
            things = kwargs.get("thing")
            del kwargs['thing']

        return self.create(
            fsc.Location(
                name=name,
                description=description,
                encoding_type=encoding_type,
                properties=properties,
                location = location,
                things=things,
                historical_locations=historical_locations,
                **kwargs
            )
        )

    def create_thing(self,
            name='',
            description='',
            properties=None,
            locations=None,
            historical_locations=None,
            datastreams=None,
            multi_datastreams=None,
            tasking_capabilities=None,
            **kwargs):

        if locations is None and 'location' in kwargs.keys():
            locations = kwargs.get("location")
            del kwargs["location"]
        if not isinstance(locations, list):
            locations = [locations]

        return self.create(
            fsc.Thing(
                name=name,
                description=description,
                properties=properties,
                locations=locations,
                historical_locations=historical_locations,
                datastreams=datastreams,
                multi_datastreams=multi_datastreams,
                tasking_capabilities=tasking_capabilities,
                **kwargs
            )
        )

    def create_unit_of_measurement(self, name='', symbol='', definition=''):
        return UnitOfMeasurement(
            name=name,
            symbol=symbol,
            definition=definition
        )
    
    def create_datastream(self,
            name='',
            description='',
            observation_type='',
            unit_of_measurement=None,
            properties=None,
            thing=None,
            sensor=None,
            observed_property=None,
            **kwargs):

        if thing is None:
            raise ValueError('Cannot create Datastream without Thing!')
        if sensor is None:
            raise ValueError('Cannot create Datastream without Sensor!')
        if observed_property is None:
            raise ValueError('Cannot create Datastream without ObservedProperty!')
        if not isinstance(unit_of_measurement, UnitOfMeasurement):
            raise TypeError('unit_of_measurement must be of type UnitOfMeasurement')

        return self.create(
            fsc.Datastream(
                name=name,
                description=description,
                observation_type=observation_type,
                unit_of_measurement=unit_of_measurement,
                properties=properties,
                thing=thing,
                sensor=sensor,
                observed_property=observed_property,
                **kwargs
            )
        )

    def create_sensor(self,
            name='',
            description='',
            encoding_type='',
            properties=None,
            metadata=None,
            datastreams=None,
            multi_datastreams=None,
            **kwargs):

        return self.create(
            fsc.Sensor(
                name=name,
                description=description,
                encoding_type=encoding_type,
                properties=properties,
                metadata=metadata,
                datastreams=datastreams,
                multi_datastreams=multi_datastreams,
                **kwargs
            )
        )

    def create_observed_property(self,
            name='',
            definition='',
            description='',
            datastreams=None,
            properties=None,
            multi_datastreams=None,
            **kwargs):

        return self.create(
            fsc.ObservedProperty(
                name=name,
                definition=definition,
                description=description,
                datastreams=datastreams,
                properties=properties,
                multi_datastreams=multi_datastreams,
                **kwargs
            )
        )

    def create_observation(self,
            phenomenon_time=None,
            result=None,
            result_time=None,
            result_quality=None,
            valid_time=None,
            parameters=None,
            datastream=None,
            multi_datastream=None,
            feature_of_interest=None,
            **kwargs):

        if datastream is None:
            raise ValueError('Cannot create Observation without Datastream')

        self.create(
            fsc.Observation(
                phenomenon_time=phenomenon_time,
                result=result,
                result_time=result_time,
                result_quality=result_quality,
                valid_time=valid_time,
                parameters=parameters,
                datastream=datastream,
                multi_datastream=multi_datastream,
                feature_of_interest=feature_of_interest,
                **kwargs
            )
        )

    def create(self, entity):
        self.service.create(entity)
        return entity

    def update(self, entity):
        self.service.update(entity)

    def delete(self, entity):
        if isinstance(entity, EntityList):
            for e in entity:
                self.delete(e)
        else:
            self.service.delete(entity)

    def dump(self, entity):
        return transform_entity_to_json_dict(entity)

    def change_datastream_id(self, datastream, new_id):
        ## create new datastream as copy with new id
        new_datastream = self.create_datastream(
            id=new_id,
            name=datastream.name,
            description=datastream.description,
            observation_type=datastream.observation_type,
            unit_of_measurement=datastream.unit_of_measurement,
            properties=datastream.properties,
            thing=datastream.thing\
                if datastream.thing is not None else self.get_things(relations=datastream).get(0),
            sensor=datastream.sensor\
                if datastream.sensor is not None else self.get_sensors(relations=datastream).get(0),
            observed_property=datastream.observed_property\
                if datastream.observed_property is not None else self.get_observed_properties(relations=datastream).get(0)
        )
        ## link observations to new datastream
        observations = self.get_observations(relations=datastream)
        for obs in observations.entities:
            obs.datastream = new_datastream
            self.update(obs)
        ## delete old datastream
        self.delete(datastream)

