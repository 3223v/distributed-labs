from abc import ABC, abstractmethod

class Engine(ABC):
    def __init__(self):
        pass

    @abstractmethod
    async def query(self)->dict:
        pass

    @abstractmethod
    async def submit(self)->dict:
        pass
