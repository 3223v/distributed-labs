

class Command():
    def __init__(self, op, key, value, client_id, seq :int, verison :int, request_id):
        self.key = key
        self.value = value
        self.client_id = client_id
        self.seq = seq
        self.version = version
        self.request_id = request_id
    
    def islegal(self) -> bool: 

        if self.request_id == "" or self.request_id is None:
            return false

        if self.op.lower() == ( "put" || "cas" ):
            if self.seq == "" or self.seq is None or self.seq < 0:
                return False
        
        if self.op.lower() == "cas" :
            if self.version == "" or self.version is None:
                return false
        
        if self.op.lower() != ("get" || "put" || "del" || "cas" || "ping" || "echo"):
            return False 

        return true
            
