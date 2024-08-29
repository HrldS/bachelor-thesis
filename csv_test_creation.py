import csv
import random


# Function to generate random number between 1 and 30
def random_number():
    return random.randint(1, 30)


# Number of rows your csv file will have
num_rows = 3500000

# Write to CSV file with the name "test_data"
# w = create a new file or overwrite an existing file
# a = edit an existing file by adding new rows to the file
with open('test_data34.csv', 'w', newline='') as file:
    writer = csv.writer(file)

    for i in range(1, num_rows + 1):
        row = [f'Object {i}', random_number(), random_number(), random_number()]
        writer.writerow(row)


print(f"CSV file 'test_data.csv' has been created successfully")