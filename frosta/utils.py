import pandas as pd
import datetime
import pytz
import numpy as np

from frost_sta_client.model.ext.entity_list import EntityList

def as_dataframe(entity_list):
    if not isinstance(entity_list, EntityList):
        raise ValueError("Only EntityLists can be converted to a DataFrame!")
    if entity_list.entity_class == 'frost_sta_client.model.observation.Observation':
        # Single-pass extraction using tuple unpacking for speed
        data = [
            (obs.phenomenon_time, obs.result, obs.id, obs.datastream.id) 
            for obs in entity_list
        ]
        return pd.DataFrame(
            data,
            columns=["phenomenon_time", "result", "id", "datastream_id"]
        )
    else:
        raise NotImplementedError(
            f'Conversion of EntityList of type {entity_list.entity_class} to DataFrame not yet implemented.'
        )
    
def as_time_series(entity_list, tz: str | pytz.tzinfo.BaseTzInfo | datetime.timezone = 'UTC'):
    # Fixed logic bug: should be OR not AND
    if not isinstance(entity_list, EntityList) \
        or entity_list.entity_class != 'frost_sta_client.model.observation.Observation':
        raise ValueError("Only EntityLists of Observations can be converted to Time Series!")
    
    if len(entity_list.entities) == 0:
        return None
        
    # Single-pass extraction: iterate once instead of twice
    times = []
    results = []
    name = None
    
    for obs in entity_list:
        if name is None:
            name = obs.datastream.id
        times.append(obs.phenomenon_time)
        results.append(obs.result)
    
    # Optimize datetime parsing with utc=True for ISO8601 strings
    # This is faster than format='ISO8601' and then tz_convert
    index = pd.to_datetime(times, utc=True)
    
    # Only convert timezone if it's not UTC
    if tz != 'UTC' and tz != datetime.timezone.utc:
        index = index.tz_convert(tz)
    
    return pd.Series(
        data=results,
        index=index,
        name=name
    )
