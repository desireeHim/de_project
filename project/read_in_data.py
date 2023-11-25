import json

data = []
i = 0
for line in open('data/arxiv-metadata-oai-snapshot.json', 'r', encoding='utf-8'):
    data.append(json.loads(line))
    i = i + 1
    if i == 10:    # you can change the limit of reading in data objects by changing the number in if- condition
        break
    
## if everything went well this line should print out the first element of the list
#print(json.dumps(data[0], indent=2))   
# data cleaning example: "droping" data with one word titles
data = [obj for obj in data if len(obj['title'].split(" ")) > 1]
modified_data = []

## line by line data modification
for line in data:
    modified_data.append(line)

with open(f'data/staging/cleaned_data.json', 'w') as json_file:
    json.dump(modified_data, json_file, indent=2)