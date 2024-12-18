class FormalParameter:
    """Represents a formal parameter that may be passed between functions in a computational workflow."""
    def __init__(self, name: str, type_: type):
        self.name = name
        self.type = type_

    def __repr__(self):
        return f'FormalParameter({self.name}, {self.type})'

    def input(self, default: object = None):
        return self, 'input', default

    def output(self, default: object = None):
        return self, 'output', default


class FileFormalParameter(FormalParameter):
    """Represents a file as a formal parameter, that may be ingested or created by a function in a computational workflow."""
    def __init__(self, name: str, file_format: str = None):
        super().__init__(name, str)
        self.format = file_format

    def __repr__(self):
        return f'FileFormalParameter({self.name}, {self.format})'

    def input(self, path: str):
        return self, 'input', path

    def output(self, path: str):
        return self, 'output', path
