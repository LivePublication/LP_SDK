class FormalParameter:
    """Represents a formal parameter that may be passed between functions in a computational workflow."""
    def __init__(self, name: str, type_: type):
        self.name = name
        self.type = type_

    def __repr__(self):
        return f'FormalParameter({self.name}, {self.type})'

    @property
    def input(self):
        return self, 'input'

    @property
    def output(self):
        return self, 'output'


class FileFormalParameter(FormalParameter):
    """Represents a file as a formal parameter, that may be ingested or created by a function in a computational workflow."""
    def __init__(self, name: str, file_format: str = None):
        super().__init__(name, str)
        self.format = file_format

    def __repr__(self):
        return f'FileFormalParameter({self.name}, {self.format})'
