import frost_sta_client as fsc
from datetime import datetime
from dateutil.parser import parse
from dateutil.tz import tzutc
from frost_sta_client.model.ext.entity_type import EntityTypes
from frost_sta_client.model.entity import Entity
from frost_sta_client.model.ext.entity_list import EntityList
import logging

RELATIONS = {
    'Location': {
        'Thing': 'Thing',
        'Datastream': 'Things/Datastreams',
        'Sensor': 'Things/Datastreams/Sensor',
        'ObservedProperty': 'Things/Datastreams/ObservedProperty',
        'Observation': 'Things/Datastreams/Observation'
    },
    'Thing': {
        'Location': 'Locations',
        'Datastream': 'Datastream',
        'Sensor': 'Sensor',
        'ObservedProperty': 'ObservedProperty',
        'Observation': 'Observation'
    },
    'Datastream': {
        'Location': 'Thing/Locations',
        'Thing': 'Thing',
        'Sensor': 'Sensor',
        'ObservedProperty': 'ObservedProperty',
        'Observation': 'Observation'
    },
    'Sensor': {
        'Location': 'Datastreams/Thing/Locations',
        'Thing': 'Datastream/Thing',
        'Datastream': 'Datastream',
        'ObservedProperty': 'Datastream/ObservedProperty',
        'Observation': 'Datastream/Observation'
    },
    'ObservedProperty': {
        'Location': 'Datastreams/Thing/Locations',
        'Thing': 'Datastreams/Thing',
        'Datastream': 'Datastreams',
        'Sensor': 'Datastreams/Sensor',
        'Observation': 'Datastreams/Observation'
    },
    'Observation': {
        'Location': 'Datastream/Thing/Locations',
        'Thing': 'Datastream/Thing',
        'Datastream': 'Datastream',
        'Sensor': 'Datastream/Sensor',
        'ObservedProperty': 'Datastream/ObservedProperty',
    }
}

def get_relation(origin, target):
    return RELATIONS.get(origin).get(target)

def get_entity_list(entities, callback=None, step_size=None, **kwargs):
    query = entities.query()
    query = add_filters(query, **kwargs)
    query = add_expansion(query, **kwargs)
    query = add_order(query, **kwargs)
    query = add_chunks(query, **kwargs)
    entity_list = query.count().list(callback, step_size)
    match len(entity_list.entities):
        case 0:
            return None
        case 1:
            return entity_list.get(0)
        case _:
            return entity_list


def add_filters(query, **kwargs):
    filters = []
    for key, value in kwargs.items():
        if key == "relations" and value is not None:
            if isinstance(value, Entity) or isinstance(value, EntityList):
                relations = [value]
            else:
                relations = [entity for entity in value if isinstance(entity, Entity)]\
                    + [entity_list for entity_list in value if isinstance(entity_list, EntityList)]
            for relative in relations:
                filter = get_relation_filter(query.entity, relative)
                filters.append(filter)
        elif key in ['id', 'name', 'description'] and value != '':
            filters.append(get_string_filter(key, value))
        elif key in ['start', 'end'] and value is not None:
            filters.append(get_time_filter(key, value))
        elif key in ['lower_limit', 'upper_limit'] and value is not None:
            filters.append(get_limit_filter(key, value))
    filters = [f for f in filters if f is not None]
    if len(filters) > 0:
        return query.filter(' and '.join(filters))
    return query

def add_expansion(query, **kwargs):
    if query.entity == 'Datastream':
        return query.expand(
            "Thing($select=@iot.id),Thing/Locations($select=@iot.id,name,location),"\
            "ObservedProperty($select=@iot.id,name)"
        )
    if query.entity == 'Observation':
        return query.expand("Datastream($select=@iot.id)")
    return query

def add_order(query, **kwargs):
    if 'orderby' in kwargs.keys():
        return query.orderby(kwargs.get('orderby'), order = '')
    if query.entity == 'Observation':
        return query.orderby('phenomenonTime', order = 'asc')
    return query.orderby('name', order = 'asc')

def add_chunks(query, **kwargs):
    if 'skip' in kwargs.keys():
        query = query.skip(kwargs.get('skip'))
    if 'top' in kwargs.keys():
        query = query.top(kwargs.get('top'))
    return query

def get_relation_filter(origin, target):
    if isinstance(target, fsc.model.ext.entity_list.EntityList):
        _, target_entity = target.entity_class.rsplit('.', 1)
        relation = get_relation(origin, target_entity)
        ids = [entity.id for entity in target]
        alternatives = [f"'{i}' eq " + relation + '/id' for i in ids]
        return '(' + ' or '.join(alternatives) + ')'
    if isinstance(target, fsc.model.entity.Entity):
        relation = get_relation(origin, type(target).__name__)
        return f"'{target.id}' eq " + relation + '/id'

def get_string_filter(key, value):
    value = value.lower()
    if len(value) > 1:
        if value.startswith('*') and value.endswith('*'):
            return f"substringof('{value[1:-1]}', tolower({key}))"
        if value.startswith('*'):
            return f"endswith(tolower({key}), '{value[1:]}')"
        if value.endswith('*'):
            return f"startswith(tolower({key}), '{value[:-1]}')"
    return f"'{value}' eq tolower({key})"

def get_time_filter(key, value):
    if isinstance(value, str):
        value = parse(value)
    if isinstance(value, datetime):
        value = value.astimezone(tzutc()).isoformat()
        if key == 'start':
            return f'phenomenonTime ge {value}'
        elif key == 'end':
            return f'phenomenonTime lt {value}'
    else:
        return None

def get_limit_filter(key, value):
    if key == 'upper_limit':
        return f'result lt {value}'
    if key == 'lower_limit':
        return f'result ge {value}'
