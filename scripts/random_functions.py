'''
This script will quickly create 10 subscriber functions to help test
'''

import requests
import random

word_site = "http://svnweb.freebsd.org/csrg/share/dict/words?view=co&content-type=text/plain"

response = requests.get(word_site)
words = response.content.splitlines()

word_list = []
for word in range(1, 3):
    random_word = random.randint(1, len(words))
    word_list.append(words[random_word].decode().lower())

for word in word_list:
    print(word)
    url = f'https://xpcnqg9f37.execute-api.us-east-1.amazonaws.com/dev/create/namespace/infosec/event/twitter/subscriber/{word}'
    requests.post(url)