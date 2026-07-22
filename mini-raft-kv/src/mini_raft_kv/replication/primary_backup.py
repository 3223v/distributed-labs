

class PrimaryBackupEngine(Engine):

    def __init__(self, wal, sm, sst, backup_host, backup_port):
        self.wl = wal
        self.sm = sm
        self.sst = sst
        self.backup = Client(backup_host, backup_port, v=1, client_id="primary", timeout=3)    