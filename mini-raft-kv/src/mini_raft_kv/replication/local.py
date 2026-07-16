

class LocalEngine(Engine):

    def __init__(self, wl:WAL, sm:StateMachine):
        self.wl = wl
        self.sm = sm

    def submit(self,cmd:Command):
        wl.append(cmd)
        sm.apply(cmd)

    def query(self,cmd:Command):
        sm.apply(cmd)