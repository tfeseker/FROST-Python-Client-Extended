from pandas import DataFrame
from frost_sta_client.model.ext.entity_list import EntityList

def as_dataframe(entity_list):
    if not isinstance(entity_list, EntityList):
        raise ValueError("Only EntityLists can be converted to a DataFrame!")
    if entity_list.entity_class == 'frost_sta_client.model.observation.Observation':
        return DataFrame(
            [(obs.phenomenon_time, obs.result, obs.id, obs.datastream.id) for obs in entity_list],
            columns=["phenomenon_time", "result", "id", "datastream_id"]
        )
    else:
        raise NotImplementedError(
            f'Conversion of EntityList of type {entity_list.entity_class} to DataFrame not yet implemented.'
            )
