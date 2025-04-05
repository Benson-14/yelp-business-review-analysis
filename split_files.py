import json 

input_file = "yelp_academic_dataset_review.json"
output_prefix = "split_file_"
num_files = 10

with open(input_file, "r", encoding="utf8") as f:
    total_lines = sum(1 for _ in f)

lines_per_file = total_lines // num_files

print(f"Total lines: {total_lines}, Lines per file: {lines_per_file}")

with open(input_file, "r", encoding="utf8") as f:
    for i in range(num_files):
        output_filename = f"{output_prefix}{i+1}.json"

        with open(output_filename, "w", encoding="utf8") as out_file:
            for j in range(lines_per_file):
                line = f.readline()
                if not line:
                    break
                out_file.write(line)


print("JSON file successfully split into smaller parts!")

