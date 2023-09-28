# Input string
input_string = "2008/08/20 12:09:17 2008/08/20 12:45:05 walk"

# Split the string on spaces
split_parts = input_string.split()

# Combine date and time parts and append "walk"
result_list = [split_parts[0] + " " + split_parts[1], split_parts[2] + " " + split_parts[3], split_parts[4]]

# Print the resulting list
print(result_list)
