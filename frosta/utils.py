import pandas as pd
from frost_sta_client.model.ext.entity_list import EntityList

def as_dataframe(entity_list):
    if not isinstance(entity_list, EntityList):
        raise ValueError("Only EntityLists can be converted to a DataFrame!")
    if entity_list.entity_class == 'frost_sta_client.model.observation.Observation':
        return pd.DataFrame(
            [(obs.phenomenon_time, obs.result, obs.id, obs.datastream.id) for obs in entity_list],
            columns=["phenomenon_time", "result", "id", "datastream_id"]
        )
    else:
        raise NotImplementedError(
            f'Conversion of EntityList of type {entity_list.entity_class} to DataFrame not yet implemented.'
            )
    
def as_time_series(entity_list, tz = 'Europe/Berlin'):
    if not isinstance(entity_list, EntityList) \
        and entity_list.entity_class == 'frost_sta_client.model.observation.Observation':
    
        raise ValueError("Only EntityLists of Observations can be converted to Time Series!")
    
    if len(entity_list.entities) > 0:
        name = entity_list.get(0).datastream.id
    else:
        name = 'n/a'

    return pd.Series(
        data=[obs.result for obs in entity_list],
        index=pd.to_datetime([obs.phenomenon_time for obs in entity_list], format='ISO8601').tz_convert(tz=tz),
        name=name
    )    

    
    
