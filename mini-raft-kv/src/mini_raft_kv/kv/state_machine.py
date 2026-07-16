from mini_raft_kv.kv import Command


class StateMachine():

    def __init__(self):
        self.ct = ClientTable()
        self.dt = dict()

    # 上层自校验cmd的正确性
    def apply(self,cmd :Command):

        if cmd.op.lower() == "get":
            if self.dt.get(cmd.key) is None:
                return {
                    "cmd": cmd,
                    "result" : {
                        ""
                    } 
                }
            return 


        elif cmd.op.lwer() == "put":

        elif cmd.op.lower() == "del":
        
        elif cmd.op.lower() == "cas":

        # ping echo 让网络层处理即可
        else:




