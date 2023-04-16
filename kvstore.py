class KeyValueStore:
    def __init__(self):
        self.store = {}

    def get(self, key):
        if key in self.store:
            return self.store[key]
        else:
            return "Key does not exist"

    def set(self, key, value):
        self.store[key] = value
        return "Key-value pair added"

    def delete(self, key):
        if key in self.store:
            del self.store[key]
            return "Key deleted"
        else:
            return "Key does not exist"
