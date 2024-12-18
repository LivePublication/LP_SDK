import inspect


def _function(a: int, b: str, c: float) -> None:
    pass


def test_signature():
    sig = inspect.signature(_function)
    assert list(sig.parameters.keys()) == ['a', 'b', 'c']
    assert sig.return_annotation == None
    assert sig.parameters['a'].annotation == int
    assert sig.parameters['b'].annotation == str
    assert sig.parameters['c'].annotation == float


class FileParam:
    def __init__(self, name: str):
        self.name = name

    def __repr__(self):
        return f'FileParam({self.name})'

    def __call__(self, *args, **kwargs):
        return {'name': self.name, 'type': 'file', 'args': args, 'kwargs': kwargs}

_file = FileParam('file')

def _function1(a: int, b: _file(output=True), c: float) -> None:
    pass

def _function2(a: _file(input=True), c: float, d: _file(output=False)) -> None:
    pass

def test_file_param():
    sig = inspect.signature(_function1)
    assert list(sig.parameters.keys()) == ['a', 'b', 'c']
    assert sig.return_annotation == None
    assert sig.parameters['a'].annotation == int
    assert sig.parameters['b'].annotation == FileParam
    assert sig.parameters['c'].annotation == float

