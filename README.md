# Extended Sensorthings API Python Client

This Python package is based on the [FROST-Python-Client](https://github.com/FraunhoferIOSB/FROST-Python-Client) and aims to provide a user-friendly client as an extension to the existing package.

This client requires a slightly modified version of the original FROST-Python-Client package as some pull requests have not yet been merged.

## Connecting to a SensorThings service

The only class of this package is `FrostClient`. It holds the SensorThingsService as a property and offers convenience methods to interact with this service.
```
from frosta import FrostClient

client = FrostClient(
    url='http://example.com:8080/FROST-Server/v1.1',
    username="admin",
    password="password"
)
```

## Queries

The client enables the user to conduct queries using String matching with wildcards for the name, decription, and id properties. To find the ObservedProperty for soil moisture content, one could apply the following filter:
```
observed_properties = client.get_observed_properties(name="*moist*")
```
In addition, the queries can be filtered using given entities, e.g. to find all sensors that measure a given ObservedProperty:
```
sensors = client.get_sensors(relations=observed_properties)
```
It is also possible to filter with multiple relations, e.g. to find all Locations where a given sensor is used to observe a given list of ObservedProperties:
```
locations = client.get_locations(relations=[observed_properties, sensors.get(0)])
```
Finding all observations greater of equal to 5 of a given ObservedProperty, from a given Location, and for a given time interval is straightforward:
```
observations = client.get_observations(
    relations=[observed_property, location],
    start="2023-01-01",
    end="2023-01-31",
    lower_limit=5
    )
```

## Further development

This package will be developed further to facilitate the interaction with SensorThings services using dashboards. Contributions are welcome!
