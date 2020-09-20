
class Dbo:
    connection = None

    def __init__(self, connection) -> None:
        self.connection = connection

    def create(self) -> int:
        pass

    def update(self) -> None:
        pass

    def delete(self) -> None:
        pass
