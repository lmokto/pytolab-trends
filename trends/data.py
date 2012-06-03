import json

import exceptions

def parse_post(data): 
    try:
      post = {}
      s = data.split(':<$>:')
      post['msg'] = data
      post['author_name'] = s[0]
      post['author_id'] = int(s[1])
      post['id'] = int(s[2])
      post['text'] = s[3]
      post['retweeted'] = int(s[4])
      post['coordinates'] = json.loads(s[5])
      post['time'] = int(s[6])
    except IndexError, ValueError:
      raise exceptions.DataError()
