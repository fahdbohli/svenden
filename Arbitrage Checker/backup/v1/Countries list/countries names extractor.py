import os

# Get the current directory
current_dir = os.getcwd()

# File to write the names into
output_file = "json_filenames.txt"

# List to hold the base filenames without .json extension
json_filenames = []

# Iterate through all files in the directory
for filename in os.listdir(current_dir):
    if filename.endswith(".json"):
        base_name = os.path.splitext(filename)[0]  # Remove the .json extension
        json_filenames.append(base_name)

# Write the results to the output text file
with open(output_file, "w") as f:
    for name in json_filenames:
        f.write(name + "\n")

print(f"Exported {len(json_filenames)} JSON filenames to '{output_file}'")
