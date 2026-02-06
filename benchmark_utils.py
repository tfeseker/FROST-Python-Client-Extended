"""Benchmark script to measure performance improvements in utils.py"""
import time
import pandas as pd
from frosta.utils import as_time_series
from frost_sta_client.model.ext.entity_list import EntityList
from frost_sta_client.model.observation import Observation
from frost_sta_client.model.datastream import Datastream

# Create mock observations for testing
def create_mock_observations(count=1000):
    """Create mock observation data for benchmarking"""
    datastream = Datastream()
    datastream.id = "test-datastream-001"
    
    observations = []
    base_time = pd.Timestamp('2024-01-01', tz='UTC')
    
    for i in range(count):
        obs = Observation()
        obs.phenomenon_time = (base_time + pd.Timedelta(minutes=i)).isoformat()
        obs.result = 20.0 + (i % 10) * 0.1  # Simulated sensor values
        obs.datastream = datastream
        observations.append(obs)
    
    # Create EntityList with required entity_class parameter
    entity_list = EntityList('frost_sta_client.model.observation.Observation')
    entity_list.entities = observations
    
    return entity_list

# Benchmark
if __name__ == "__main__":
    sizes = [100, 500, 1000, 5000, 10000]
    
    print("Benchmarking as_time_series() performance")
    print("=" * 60)
    
    for size in sizes:
        entity_list = create_mock_observations(size)
        
        # Warm-up
        _ = as_time_series(entity_list)
        
        # Measure
        iterations = 10
        start = time.perf_counter()
        for _ in range(iterations):
            result = as_time_series(entity_list)
        elapsed = time.perf_counter() - start
        
        avg_time = elapsed / iterations * 1000  # Convert to ms
        print(f"{size:6d} observations: {avg_time:7.2f} ms/call ({len(result)} items)")
    
    print("\nOptimizations applied:")
    print("  ✓ Single-pass iteration (was: two list comprehensions)")
    print("  ✓ Fast datetime parsing with utc=True")
    print("  ✓ Skip timezone conversion when tz='UTC'")
    print("  ✓ Fixed validation logic bug (or vs and)")
    print("  ✓ Early return for empty lists")
