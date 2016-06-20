import json

class Store:
  def __init__(self, that=None):
    self.store_dict = dict() if that is None else dict(that)

  def insert(self, key, value):
    if key in self.store_dict:
      return False, None
 
    self.store_dict[key] = value
    return True, None

  def update(self, key, value):
    if key not in self.store_dict:
      return False, None

    self.store_dict[key] = value
    return True, None

  def delete(self, key):
    value = self.store_dict.pop(key, None)
    return value is not None, value

  def get(self, key):
    if key in self.store_dict:
      return True, self.store_dict[key]

    return False, ""
    
  def countkey(self):
    return len(self.store_dict)
    
  def dump(self):
    arr = [[key, self.store_dict[key]] for key in self.store_dict] 
    return json.dumps(arr)
