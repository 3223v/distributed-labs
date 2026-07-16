from abc import ABC, abstractmethod

class Engine(ABC):
    @abstractmethod
    def query():
        pass
        
    @abstractmethod
    def submit():
        pass
