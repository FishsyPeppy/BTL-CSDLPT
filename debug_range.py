#!/usr/bin/env python3
"""
Debug range partition logic
"""
from Interface import *

def debug_range_logic():
    """Debug range partition logic"""
    
    # Test với 5 partitions như trong testcase
    numberofpartitions = 5
    delta = 5.0 / numberofpartitions  # 1.0
    
    print(f"Number of partitions: {numberofpartitions}")
    print(f"Delta: {delta}")
    print()
    
    # In ra range cho mỗi partition
    for i in range(numberofpartitions):
        minRange = i * delta
        maxRange = minRange + delta
        print(f"Partition {i}: [{minRange}, {maxRange}]")
        if i == 0:
            print(f"  Contains ratings >= {minRange} AND <= {maxRange}")
        else:
            print(f"  Contains ratings > {minRange} AND <= {maxRange}")
    
    print()
    
    # Test với rating = 3
    rating = 3.0
    print(f"Testing rating = {rating}")
    
    # Logic hiện tại
    if rating == 5.0:
        index = numberofpartitions - 1
    else:
        index = int(rating / delta)
        if index >= numberofpartitions:
            index = numberofpartitions - 1
    
    print(f"Current logic: rating/{delta} = {rating/delta}")
    print(f"int({rating/delta}) = {int(rating/delta)}")
    print(f"Calculated index: {index}")
    print(f"Expected table: range_part{index}")
    
    # Theo yêu cầu README, rating 3 nên ở partition nào?
    # Partition 0: [0, 1] (>= 0 and <= 1)
    # Partition 1: (1, 2] (> 1 and <= 2) 
    # Partition 2: (2, 3] (> 2 and <= 3) <- rating 3 nên ở đây
    # Partition 3: (3, 4] (> 3 and <= 4)
    # Partition 4: (4, 5] (> 4 and <= 5)
    
    print(f"\nBased on ranges, rating {rating} should be in partition 2")
    print("But testcase expects it in range_part2, so our logic is correct")
      # Hãy kiểm tra logic insert một cách chi tiết với logic mới
    print(f"\nDetailed check with new logic:")
    for test_rating in [0, 0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5, 5]:
        if test_rating == 5.0:
            idx = numberofpartitions - 1
        elif test_rating == 0.0:
            idx = 0
        else:
            idx = int(test_rating / delta)
            if test_rating == idx * delta and idx > 0:
                idx = idx - 1
            if idx >= numberofpartitions:
                idx = numberofpartitions - 1
        print(f"Rating {test_rating} -> Partition {idx}")

if __name__ == "__main__":
    debug_range_logic()
