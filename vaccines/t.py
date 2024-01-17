import json

with open('vaccine_products.txt', 'r') as f:
    d = f.read()
    di = json.loads(d)
    print(di)