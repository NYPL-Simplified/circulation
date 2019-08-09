def median(numbers):
    numbers = sorted(numbers)
    length = len(numbers)
    if length % 2 == 0:
        high_middle = numbers[length//2]
        low_middle = numbers[length//2 - 1]
        return (high_middle + low_middle) / 2.0
    else:
        return numbers[length//2]
