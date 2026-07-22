
class ClientTable:
    def __init__(self):
        self.data = dict()
    # 重复，旧的，新的
    # "duplicate" | "stale" | "new"
    def check(self, client_id, seq) -> str:
        d = self.data.get(client_id)
        if d:
            last_seq = d.get("last_seq")
            if seq < last_seq:
                return "stale"
            if seq == last_seq:
                return "duplicate"
            if seq > last_seq:
                return "new"
        return "new"
    def record(self, client_id, seq, ok, result, error):
        self.data[client_id] = {
            "last_seq" : seq,
            "last_ok" : ok,
            "last_result" : result,
            "last_error" : error
        }
    def return_old(self,client_id):
        return self.data.get(client_id,{})

    def to_dict(self):
        return dict(self.data)
    
    def from_dict(self,d):
        self.data = dict(d)
