from mini_raft_kv.common.command import Command
from mini_raft_kv.common.query import Query
from mini_raft_kv.kv.client_table import ClientTable
from functools import singledispatch

class StateMachine():

    def __init__(self):
        self.ct = ClientTable()
        self.dt = dict()

                # 这里错误出参应该是
                # {
                #     "ok": True/False,
                #     "result": None,
                #     "error" : None
                # }
                # result = {
                #     "key":"",
                #     "value":"",
                #     "version":""
                # }
                # error = {
                #     "code":"",
                #     "data":"",
                #     "message":""
                # }

    def apply(self, cmd) ->dict:
        if self.ct.check(cmd.client_id,cmd.seq) == "new":
            if cmd.op.lower() == "put":
                # put in dt save in dt
                self.dt[cmd.key] = cmd.value
                re = {
                    "ok":True,
                    "result":{
                        "key" :"",
                        "value" : self.dt.get(cmd.key),
                        "version":""
                    },
                    "error":None
                }
                self.ct.record(cmd.client_id,cmd.seq,re.get("ok"),re.get("result"),re.get("error"))
                return re
            elif cmd.op.lower() == "del":
                tmp = self.dt.pop(cmd.key)
                re = {
                    "ok":True,
                    "result":{
                        "key" :"",
                        "value" : tmp,
                        "version":""
                    },
                    "error":None
                }
                self.ct.record(cmd.client_id,cmd.seq,re.get("ok"),re.get("result"),re.get("error"))
                return re
            elif cmd.op.lower() == "cas":
                pass
            else:
                return {
                    "ok" : False,
                    "result" : None,
                    "error" : {
                        "code" : "",
                        "data" : "",
                        "message" : "未知错误"
                    }
                }
        elif self.ct.check(cmd.client_id,cmd.seq) == "duplicate":

        else:
            return 

    def read(self, qry) ->dict:
        if qry.key in dt:
            return {
                "ok" : True,
                "result" : {
                    "key" : "",
                    "value" : dt.get(qry.key),
                    "version" :""
                },
                "error" : None
            }
        return {
            "ok" : False,
            "result" : None,
            "error" : {
                "code" : "",
                "data" : "",
                "message" : "不存在的key"
            }
        }


