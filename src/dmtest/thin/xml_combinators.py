from typing import List, Tuple, Callable, Dict, Iterator
import random
import time

from dmtest.thin.xml import Pool, Thin

#------------------------------------------------

# A mapping is now (length, is_mapped)
Mapping = Tuple[int, bool]  # length, is_mapped
Pattern = Callable[[], Iterator[Mapping]]
AllocatedMapping = Tuple[int, int, bool]    # data, length, is_mapped

def allocate(length: int) -> Pattern:
    """
    Create a pattern that allocates a contiguous block of the specified length.

    Args:
        length (int): The length of the block to allocate.

    Returns:
        Pattern: A pattern function that yields a single allocated block.
    """
    def pattern() -> Iterator[Mapping]:
        yield (length, True)
    return pattern

def gap(length: int) -> Pattern:
    """
    Create a pattern that represents an unallocated gap of the specified length.

    Args:
        length (int): The length of the gap.

    Returns:
        Pattern: A pattern function that yields a single unallocated gap.
    """
    def pattern() -> Iterator[Mapping]:
        yield (length, False)
    return pattern

def repeat(sub_pattern: Pattern, count: int) -> Pattern:
    """
    Create a pattern that repeats a given sub-pattern a specified number of times.

    Args:
        sub_pattern (Pattern): The pattern to repeat.
        count (int): The number of times to repeat the pattern.

    Returns:
        Pattern: A pattern function that yields the repeated sub-pattern.
    """
    def pattern() -> Iterator[Mapping]:
        for _ in range(count):
            yield from sub_pattern()
    return pattern

def sequence(*patterns: Pattern) -> Pattern:
    """
    Create a pattern that combines multiple patterns in sequence.

    Args:
        *patterns (Pattern): Variable number of patterns to combine.

    Returns:
        Pattern: A pattern function that yields mappings from all input patterns in sequence.
    """
    def pattern() -> Iterator[Mapping]:
        for p in patterns:
            yield from p()
    return pattern

def random_allocate(length: int, probability: float) -> Pattern:
    """
    Create a pattern that randomly allocates blocks with a given probability.

    Args:
        length (int): The total length of the pattern.
        probability (float): The probability of each block being allocated.

    Returns:
        Pattern: A pattern function that yields randomly allocated blocks and gaps.
    """
    def pattern() -> Iterator[Mapping]:
        current_run = 0
        is_mapped = random.random() < probability
        for _ in range(length):
            if random.random() < probability == is_mapped:
                current_run += 1
            else:
                if current_run > 0:
                    yield (current_run, is_mapped)
                current_run = 1
                is_mapped = not is_mapped
        if current_run > 0:
            yield (current_run, is_mapped)
    return pattern

def limit(sub_pattern: Pattern, max_length: int) -> Pattern:
    """
    Create a pattern that limits the total length of a given sub-pattern.

    Args:
        sub_pattern (Pattern): The pattern to limit.
        max_length (int): The maximum total length allowed.

    Returns:
        Pattern: A pattern function that yields mappings from the sub-pattern up to the specified limit.
    """
    def pattern() -> Iterator[Mapping]:
        total_length = 0
        for length, is_mapped in sub_pattern():
            if total_length + length > max_length:
                remaining = max_length - total_length
                if remaining > 0:
                    yield (remaining, is_mapped)
                break
            yield (length, is_mapped)
            total_length += length
    return pattern

#--------------------------------

def allocate_linear(pattern: Pattern, data_start: int = 0) -> Iterator[AllocatedMapping]:
    """
    Allocate data blocks linearly for a given pattern.

    Args:
        pattern (Pattern): The base pattern to allocate data for.
        data_start (int): The starting data block number.

    Returns:
        Iterator[AllocatedMapping]: An iterator of mappings with linearly allocated data blocks.
    """
    next_data = data_start
    for length, is_mapped in pattern():
        if is_mapped:
            yield (next_data, length, True)
            next_data += length
        else:
            yield (0, length, False)

def allocate_random(pattern: Pattern, data_pool_size: int) -> Iterator[AllocatedMapping]:
    """
    Allocate data blocks randomly for a given pattern.

    Args:
        pattern (Pattern): The base pattern to allocate data for.
        data_pool_size (int): The size of the pool to allocate data blocks from.

    Returns:
        Iterator[AllocatedMapping]: An iterator of mappings with randomly allocated data blocks.

    Raises:
        ValueError: If there are not enough unique data blocks available in the pool.
    """
    # Read the pattern completely
    mappings = list(pattern())
    
    # Build a list of (data length, mapping index) for allocated blocks
    data_blocks = [(length, i) for i, (length, is_mapped) in enumerate(mappings) if is_mapped]
    
    # Shuffle the data blocks
    random.shuffle(data_blocks)
    
    # Allocate linearly
    next_data = 0
    allocated = {}
    for length, index in data_blocks:
        if next_data + length > data_pool_size:
            raise ValueError("Not enough unique data blocks available")
        allocated[index] = next_data
        next_data += length
    
    # Yield the allocated mappings
    for i, (length, is_mapped) in enumerate(mappings):
        if is_mapped:
            yield (allocated[i], length, True)
        else:
            yield (0, length, False)

#--------------------------------

def apply_pattern_to_thin(thin, allocated_pattern: Iterator[AllocatedMapping]):
    """
    Apply a fully allocated pattern to a thin device.

    Args:
        thin: The thin device object to apply the pattern to.
        allocated_pattern (Iterator[AllocatedMapping]): The fully allocated pattern to apply.
    """
    thin_offset = 0
    for data_begin, length, is_mapped in allocated_pattern:
        if is_mapped:
            thin.add_mapping(thin_offset, data_begin, length, int(time.time()))
        thin_offset += length

def create_fragmented_pattern(total_length: int, fragment_size: int, allocation_ratio: float) -> Pattern:
    """
    Create a pattern with fragmented allocations.

    Args:
        total_length (int): Total length of the pattern.
        fragment_size (int): Size of each fragment (allocated or unallocated).
        allocation_ratio (float): Ratio of allocated fragments to total fragments.

    Returns:
        Pattern: A fragmented allocation pattern.
    """
    def pattern() -> Iterator[Mapping]:
        remaining = total_length
        while remaining > 0:
            length = min(fragment_size, remaining)
            is_allocated = random.random() < allocation_ratio
            yield (length, is_allocated)
            remaining -= length
    return pattern

def create_hotspot_pattern(total_length: int, hotspot_count: int, hotspot_size: int, hotspot_allocation_ratio: float, base_allocation_ratio: float) -> Pattern:
    """
    Create a pattern with hotspots of high allocation density.

    Args:
        total_length (int): Total length of the pattern.
        hotspot_count (int): Number of hotspots to create.
        hotspot_size (int): Size of each hotspot.
        hotspot_allocation_ratio (float): Allocation ratio within hotspots.
        base_allocation_ratio (float): Allocation ratio outside hotspots.

    Returns:
        Pattern: A pattern with allocation hotspots.
    """
    hotspot_pattern = random_allocate(hotspot_size, hotspot_allocation_ratio)
    base_pattern = random_allocate(total_length - hotspot_count * hotspot_size, base_allocation_ratio)
    
    def pattern() -> Iterator[Mapping]:
        base_iter = base_pattern()
        for _ in range(hotspot_count):
            # Yield some base pattern
            for _ in range(random.randint(0, total_length // (hotspot_count * 2))):
                yield next(base_iter, (0, False))
            # Yield a hotspot
            yield from hotspot_pattern()
        # Yield remaining base pattern
        yield from base_iter
    
    return limit(pattern, total_length)

def create_time_based_pattern(total_length: int, time_periods: int, allocation_increase: float) -> Pattern:
    """
    Create a pattern where allocation density increases over time.

    Args:
        total_length (int): Total length of the pattern.
        time_periods (int): Number of time periods to simulate.
        allocation_increase (float): Increase in allocation ratio per time period.

    Returns:
        Pattern: A pattern with increasing allocation density over time.
    """
    period_length = total_length // time_periods
    
    def pattern() -> Iterator[Mapping]:
        for i in range(time_periods):
            allocation_ratio = min(0.1 + i * allocation_increase, 1.0)
            yield from random_allocate(period_length, allocation_ratio)()
    
    return limit(pattern, total_length)

#--------------------------------

def allocate_data_blocks(patterns: Dict[int, List[Mapping]], pool_size: int) -> Dict[int, List[AllocatedMapping]]:
    """
    Allocate data blocks for multiple thins simultaneously.
    
    Args:
        patterns (Dict[int, List[Mapping]]): A dictionary of thin device IDs to their respective patterns.
        pool_size (int): The total number of data blocks in the pool.
    
    Returns:
        Dict[int, List[AllocatedMapping]]: A dictionary of thin device IDs to their allocated mappings.
    """
    allocation_requests = []
    for thin_id, mappings in patterns.items():
        for i, (length, is_mapped) in enumerate(mappings):
            if is_mapped:
                allocation_requests.append((thin_id, i, length))
    
    random.shuffle(allocation_requests)
    
    allocated_patterns = {thin_id: [] for thin_id in patterns}
    next_data_block = 0
    
    for thin_id, mapping_index, length in allocation_requests:
        if next_data_block + length > pool_size:
            raise ValueError("Not enough data blocks in the pool to satisfy all allocation requests")
        
        allocated_patterns[thin_id].append((next_data_block, length, True))
        next_data_block += length
    
    # Fill in unmapped regions
    for thin_id, mappings in patterns.items():
        full_pattern = []
        allocated_index = 0
        for length, is_mapped in mappings:
            if is_mapped:
                full_pattern.append(allocated_patterns[thin_id][allocated_index])
                allocated_index += 1
            else:
                full_pattern.append((0, length, False))
        allocated_patterns[thin_id] = full_pattern
    
    return allocated_patterns


def generate_pool_with_thins(data_block_size: int, nr_data_blocks: int, thin_patterns: Dict[int, Pattern]) -> Pool:
    """
    Generate a complete Pool object with multiple thins using the provided patterns.
    
    Args:
        data_block_size (int): The size of each data block.
        nr_data_blocks (int): The total number of data blocks in the pool.
        thin_patterns (Dict[int, Pattern]): A dictionary of thin device IDs to their respective patterns.
    
    Returns:
        Pool: A fully populated Pool object with multiple thins.
    """
    pool = Pool(data_block_size, nr_data_blocks)
    
    # Generate full patterns for each thin
    full_patterns = {dev_id: list(pattern()) for dev_id, pattern in thin_patterns.items()}
    
    # Allocate data blocks across all thins
    allocated_patterns = allocate_data_blocks(full_patterns, nr_data_blocks)
    
    # Create and populate thin devices
    for dev_id, allocated_pattern in allocated_patterns.items():
        thin = pool.add_thin(dev_id)
        apply_pattern_to_thin(thin, iter(allocated_pattern))
    
    return pool


#------------------------------------------------
