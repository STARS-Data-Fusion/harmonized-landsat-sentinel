class HLSGranuleID:
    def __init__(self, ID: str):
        parts = ID.split(".")
        self.sensor = parts[1]
        self.tile = parts[2][1:]
        self.timestamp = parts[3]
        self.version = ".".join(parts[4:])

    def __repr__(self) -> str:
        return f"{self.sensor}.T{self.tile}.{self.timestamp}.{self.version}"
