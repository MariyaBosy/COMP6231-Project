import csv

input_file = "./files/airbnb-reviews.csv"
output_file = 'airbnb-reviews-delimited.csv'

with open(input_file, 'r', encoding='utf-8') as f_in, open(output_file, 'w', newline='', encoding='utf-8') as f_out:
    reader = csv.reader(f_in, delimiter=';')
    writer = csv.writer(f_out, delimiter=',')
    
    for row in reader:
        writer.writerow(row)
