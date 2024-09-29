from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor
MAPPER: Status
REDUCER: Status

class FormatFile(_message.Message):
    __slots__ = ["outfinal_paths", "response"]
    OUTFINAL_PATHS_FIELD_NUMBER: _ClassVar[int]
    RESPONSE_FIELD_NUMBER: _ClassVar[int]
    outfinal_paths: _containers.RepeatedScalarFieldContainer[str]
    response: Status
    def __init__(self, response: _Optional[_Union[Status, str]] = ..., outfinal_paths: _Optional[_Iterable[str]] = ...) -> None: ...

class NotifyMapper(_message.Message):
    __slots__ = ["input_paths", "k", "my_index", "num_reducer", "xcoord", "ycoord"]
    INPUT_PATHS_FIELD_NUMBER: _ClassVar[int]
    K_FIELD_NUMBER: _ClassVar[int]
    MY_INDEX_FIELD_NUMBER: _ClassVar[int]
    NUM_REDUCER_FIELD_NUMBER: _ClassVar[int]
    XCOORD_FIELD_NUMBER: _ClassVar[int]
    YCOORD_FIELD_NUMBER: _ClassVar[int]
    input_paths: _containers.RepeatedScalarFieldContainer[str]
    k: int
    my_index: int
    num_reducer: int
    xcoord: _containers.RepeatedScalarFieldContainer[float]
    ycoord: _containers.RepeatedScalarFieldContainer[float]
    def __init__(self, my_index: _Optional[int] = ..., num_reducer: _Optional[int] = ..., input_paths: _Optional[_Iterable[str]] = ..., k: _Optional[int] = ..., xcoord: _Optional[_Iterable[float]] = ..., ycoord: _Optional[_Iterable[float]] = ...) -> None: ...

class NotifyReducer(_message.Message):
    __slots__ = ["intermediate_paths", "my_index", "num_mapper"]
    INTERMEDIATE_PATHS_FIELD_NUMBER: _ClassVar[int]
    MY_INDEX_FIELD_NUMBER: _ClassVar[int]
    NUM_MAPPER_FIELD_NUMBER: _ClassVar[int]
    intermediate_paths: _containers.RepeatedScalarFieldContainer[str]
    my_index: int
    num_mapper: int
    def __init__(self, my_index: _Optional[int] = ..., num_mapper: _Optional[int] = ..., intermediate_paths: _Optional[_Iterable[str]] = ...) -> None: ...

class Status(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
