import re
import gzip
from tqdm import tqdm

def parse(filename, total):
  IGNORE_FIELDS = ['Total items', 'reviews']
  
  entry = {}
  categories = []
  reviews = []
  similar_items = []
  
  with gzip.open(filename, 'r') as f:
    for line in tqdm(f, total=total):
      line = line.decode()
      line = line.strip()
      colonPos = line.find(':')

      if line.startswith("Id"):
        if reviews:
          entry["reviews"] = reviews
        if categories:
          entry["categories"] = categories
        yield entry
        entry = {}
        categories = []
        reviews = []
        rest = line[colonPos+2:]
        entry["id"] = rest.strip()

      elif line.startswith("categories"):
        entry["categories_amount"] = line.split()[1]
        
      elif line.startswith("similar"):
        similar_items = line.split()[2:]
        entry["similar_amount"] = line.split()[1]
        entry["similar_items"] = similar_items

      elif line.startswith("reviews"):
        review_info = line.split()
        entry["reviews_total"] = int(review_info[2])
        entry["reviews_downloaded"] = int(review_info[4])
        entry["reviews_avg_rating"] = float(review_info[7])

      # "cutomer" is typo of "customer" in original data
      elif line.find("cutomer:") != -1:
        review_info = line.split()
        reviews.append({'time': review_info[0],
                        'customer_id': review_info[2], 
                        'rating': int(review_info[4]), 
                        'votes': int(review_info[6]), 
                        'helpful': int(review_info[8])})

      elif line.startswith("|"):
        categories.append({'id': int(line[line.rfind('[') + 1 : -1]),
                          'hierarchy': line})

      elif colonPos != -1:
        eName = line[:colonPos]
        rest = line[colonPos+2:]

        if not eName in IGNORE_FIELDS:
          entry[eName] = rest.strip()

    if reviews:
      entry["reviews"] = reviews
    if categories:
      entry["categories"] = categories
      
    yield entry


if __name__ == '__main__':
  import simplejson
  import subprocess
  file_path = "data/amazon-meta.txt.gz"
  result = subprocess.Popen('wc -l data/amazon-meta.txt.gz', shell=True, stdout=subprocess.PIPE)
  n_lines = int([l for l in result.stdout][0].decode().split()[0])
  print(n_lines)

  for e in parse(file_path, total=n_lines):
    if e:
      s = simplejson.dumps(e)
      break

  print(s)