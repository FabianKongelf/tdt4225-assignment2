array = [
    [0, 1, 2, 3],
    [0, 1, 2, 3],
    [0, 1, 2, 3],
    [0, 1, 2, 3],
    [0, 1, 2, 3],
    [0, 1, 2, 3],
    [0, 1, 2, 3],
    [0, 1, 2, 3],
    [0, 1, 2, 3],
    [0, 1, 2, 3]
]

column_index = 3  # Index of the column you want to extract

# Initialize an empty 1D array
result = []

# Iterate through the rows and append values from the specified column
for row in array:
    if len(row) > column_index:
        value = row[column_index]
        result.append(value)

print(result)
