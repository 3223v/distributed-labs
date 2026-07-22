from mini_raft_kv.common.command import Command
from mini_raft_kv.common.query import Query
from mini_raft_kv.kv.client_table import ClientTable

class StateMachine():

    def __init__(self):
        self.ct = ClientTable()
        self.dt = dict()
        self.last_applied = 0
        # {
        #     key:{
        #         data :
        #         version
        #     }
        # }

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
            self.last_applied += 1
            if cmd.op.lower() == "put":
                # put in dt save in dt
                existing = self.dt.get(cmd.key)
                new_version = (existing["version"] + 1) if existing else 1

                self.dt[cmd.key] = {
                    "data": cmd.value,
                    "version": new_version
                }
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
                self.last_applied += 1
                return re
            elif cmd.op.lower() == "del":
                tmp = ""
                if cmd.key in self.dt:
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
                self.last_applied += 1
                return re
            elif cmd.op.lower() == "cas":
                if cmd.key not in self.dt:
                    e_v = 0
                else:
                    e_v = self.dt[cmd.key].get("version")
                re = {}
                if e_v == cmd.version:
                    # 正确
                    n_v = e_v +1
                    self.dt[cmd.key] = {
                        "data" : cmd.value,
                        "version" : n_v
                    }
                    re = {
                        "ok" :True,
                        "result" : {
                            "key" :"",
                            "value" : self.dt[cmd.key]["data"],
                            "version" : self.dt[cmd.key]["version"]
                        },
                        "error" : None
                    }
                else:
                    re = {
                        "ok" :False,
                        "result" : None,
                        "error" : {
                            "data" : "",
                            "message" : "version error",
                            "code" : ""
                        }
                    }
                self.ct.record(cmd.client_id,cmd.seq,re.get("ok"),re.get("result"),re.get("error"))
                self.last_applied += 1
                return re
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
            # 返回旧的即可
            re  = self.ct.return_old(cmd.client_id)
            return {
                "ok" : re["last_ok"],
                "result" : re["last_result"],
                "error" : re["last_error"]
            }
        else:
            return {
                "ok" : False,
                "result" : None,
                "error" : {
                    "code" : "",
                    "data" : "",
                    "message" : "重复请求等错误"
                }
             }

    def read(self, qry) ->dict:
        if qry.key in self.dt:
            return {
                "ok" : True,
                "result" : {
                    "key" : "",
                    "value" : self.dt.get(qry.key).get("data"),
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


