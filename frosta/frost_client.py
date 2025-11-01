import frost_sta_client as fsc
from .query_functions import get_entity_list
from geojson import Point
from datetime import datetime, timezone
import pytz
from frost_sta_client.model.entity import Entity
from frost_sta_client.model.location import Location
from frost_sta_client.model.thing import Thing
from frost_sta_client.model.datastream import Datastream
from frost_sta_client.model.sensor import Sensor
from frost_sta_client.model.observedproperty import ObservedProperty
from frost_sta_client.model.observation import Observation
from frost_sta_client.model.ext.entity_list import EntityList
from frost_sta_client.model.ext.unitofmeasurement import UnitOfMeasurement
from frost_sta_client.utils import transform_entity_to_json_dict
import pandas as pd
from .utils import as_time_series
from dateutil.parser import isoparse
import logging

class FrostClient():

    OBSERVATION_TYPES = {
        'OM_CategoryObservation': 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_CategoryObservation', # URI
        'OM_CountObservation': 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_CountObservation', # integer
        'OM_Measurement': 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement', # double
        'OM_Observation': 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Observation', # Any
        'OM_TruthObservation': 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_TruthObservation' # boolean
    }

    def __init__(self, url: str='', username:str='', password: str=''):
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

    def single_entity(self, entity_list: EntityList) -> Entity | None:
        if len(entity_list.entities)>0:
            return entity_list.get(0)
        else:
            return None

    def get_locations(self, id: str='', name: str='', description: str='', 
                      relations: Entity | EntityList | list[Entity] | None=None, **kwargs) -> EntityList:
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
    
    def get_location(self, id: str='', name: str='', description: str='', 
                      relations: Entity | EntityList | list[Entity] | None=None, **kwargs) -> EntityList:
        entity_list = get_entity_list(
            self.service.locations(),
            callback=self.list_callback,
            step_size=self.step_size,
            id=id,
            name=name,
            description=description,
            relations=relations,
            **kwargs
        )
        return self.single_entity(entity_list)
    
    def get_datastreams(self, id: str='', name: str='', description: str='', 
                        relations: Entity | EntityList | list[Entity] | None=None, **kwargs) -> EntityList:
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

    def get_datastream(self, id: str='', name: str='', description: str='', 
                        relations: Entity | EntityList | list[Entity] | None=None, **kwargs) -> Datastream | None:
        entity_list = get_entity_list(
            self.service.datastreams(),
            callback=self.list_callback,
            step_size=self.step_size,
            id=id,
            name=name,
            description=description,
            relations=relations,
            **kwargs
        )
        return self.single_entity(entity_list)
    
    def get_observed_properties(self, id: str='', name: str='', description: str='', 
                                relations: Entity | EntityList | list[Entity] | None=None, **kwargs) -> EntityList:
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

    def get_observed_property(self, id: str='', name: str='', description: str='', 
                                relations: Entity | EntityList | list[Entity] | None=None, **kwargs) -> EntityList:
        entity_list = get_entity_list(
            self.service.observed_properties(),
            callback=self.list_callback,
            step_size=self.step_size,
            id=id,
            name=name,
            description=description,
            relations=relations,
            **kwargs
        )
        return self.single_entity(entity_list)
    

    def get_things(self, id: str='', name: str='', description: str='', 
                   relations: Entity | EntityList | list[Entity] | None=None, **kwargs) -> EntityList:
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

    def get_thing(self, id: str='', name: str='', description: str='', 
                   relations: Entity | EntityList | list[Entity] | None=None, **kwargs) -> EntityList:
        entity_list = get_entity_list(
            self.service.things(),
            callback=self.list_callback,
            step_size=self.step_size,
            id=id,
            name=name,
            description=description,
            relations=relations,
            **kwargs
        )
        return self.single_entity(entity_list)

    def get_sensors(self, id: str='', name: str='', description: str='', 
                    relations: Entity | EntityList | list[Entity] | None=None , **kwargs) -> EntityList:
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
    def get_sensor(self, id: str='', name: str='', description: str='', 
                    relations: Entity | EntityList | list[Entity] | None=None , **kwargs) -> EntityList:
        entity_list = get_entity_list(
            self.service.sensors(),
            callback=self.list_callback,
            step_size=self.step_size,
            id=id,
            name=name,
            description=description,
            relations=relations,
            **kwargs
        )
        return self.single_entity(entity_list)

    def get_observations(self, relations: Entity | EntityList | list[Entity] | None=None, 
                         start: str | datetime | None=None, end: str | datetime | None=None, 
                         lower_limit: float | None=None, upper_limit: float | None=None, **kwargs) -> EntityList:
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
    def get_observation(self, relations: Entity | EntityList | list[Entity] | None=None, 
                         start: str | datetime | None=None, end: str | datetime | None=None, 
                         lower_limit: float | None=None, upper_limit: float | None=None, **kwargs) -> EntityList:
        entity_list = get_entity_list(
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
        return self.single_entity(entity_list)
    
    def get_time_series(self, relations: Entity | EntityList | list[Entity] | None=None, 
                        start: str | datetime | None=None, end: str | datetime | None=None, 
                        lower_limit: float | None=None, upper_limit: float | None=None, 
                        tz: str | pytz.tzinfo.BaseTzInfo | timezone ='UTC', **kwargs) -> pd.Series | None:
        observations = get_entity_list(
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
        return as_time_series(observations, tz=tz)

    def get_observations_list(self, relations: Entity | EntityList | list[Entity] | None=None, 
                        start: str | datetime | None=None, end: str | datetime | None=None, 
                        lower_limit: float | None=None, upper_limit: float | None=None, 
                        tz: str | pytz.tzinfo.BaseTzInfo | timezone ='UTC', **kwargs) -> list[dict]:
        observations = get_entity_list(
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
        return [{'phenomenon_time': isoparse(obs.phenomenon_time), 'result': obs.result} for obs in observations.entities]

    def create_location(self, name: str='', description: str='', encoding_type: str='', 
                        properties: dict | None=None, location: Point | list[float] | dict | None=None, 
                        things=None, historical_locations=None, **kwargs) -> Location:

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

    def create_thing(self, name: str='', description: str='', properties: dict | None=None,
                     locations=None, historical_locations=None, datastreams=None, multi_datastreams=None,
                     tasking_capabilities=None,**kwargs) -> Thing:

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

    def create_unit_of_measurement(self, name: str='', symbol: str='', definition: str='') -> UnitOfMeasurement:
        return UnitOfMeasurement(
            name=name,
            symbol=symbol,
            definition=definition
        )
    
    def create_datastream(self, name: str, description: str, observation_type: str,
                          unit_of_measurement: UnitOfMeasurement, properties=None, thing=None,
                          sensor=None, observed_property=None, **kwargs) -> Datastream:

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

    def create_sensor(self, name: str='', description: str='', encoding_type: str='',
                      properties: dict | None=None, metadata: str | None=None, datastreams=None, 
                      multi_datastreams=None, **kwargs) -> Sensor:

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

    def create_observed_property(self, name: str='', definition: str='', description: str='',
                                 datastreams=None, properties: dict | None=None,
                                 multi_datastreams=None, **kwargs) -> ObservedProperty:

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

    def create_observation(self, phenomenon_time: str | datetime | None=None, result=None,
                           result_time=None, result_quality=None, valid_time=None, parameters=None,
                           datastream: Datastream | None=None, multi_datastream=None, 
                           feature_of_interest=None,**kwargs) -> Observation:

        if datastream is None:
            raise ValueError('Cannot create Observation without Datastream')

        return self.create(
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
