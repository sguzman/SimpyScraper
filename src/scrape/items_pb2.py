# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: src/items.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='src/items.proto',
  package='items',
  syntax='proto3',
  serialized_pb=_b('\n\x0fsrc/items.proto\x12\x05items\"\xd1\x01\n\x04\x42ook\x12\r\n\x05title\x18\x01 \x01(\t\x12\x0b\n\x03img\x18\x02 \x01(\t\x12\x0c\n\x04\x64\x65sc\x18\x03 \x01(\t\x12\x0c\n\x04host\x18\x04 \x01(\t\x12\x12\n\ncategories\x18\x05 \x03(\t\x12\x0f\n\x07isbn_10\x18\x06 \x01(\t\x12\x0f\n\x07isbn_13\x18\x07 \x01(\t\x12\x0f\n\x07\x61uthors\x18\x08 \x01(\t\x12\x0e\n\x06\x66ormat\x18\t \x01(\t\x12\x10\n\x08pub_date\x18\n \x01(\t\x12\x0c\n\x04size\x18\x0b \x01(\t\x12\x0b\n\x03pub\x18\x0c \x01(\t\x12\r\n\x05pages\x18\r \x01(\tb\x06proto3')
)




_BOOK = _descriptor.Descriptor(
  name='Book',
  full_name='items.Book',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='title', full_name='items.Book.title', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='img', full_name='items.Book.img', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='desc', full_name='items.Book.desc', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='host', full_name='items.Book.host', index=3,
      number=4, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='categories', full_name='items.Book.categories', index=4,
      number=5, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='isbn_10', full_name='items.Book.isbn_10', index=5,
      number=6, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='isbn_13', full_name='items.Book.isbn_13', index=6,
      number=7, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='authors', full_name='items.Book.authors', index=7,
      number=8, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='format', full_name='items.Book.format', index=8,
      number=9, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='pub_date', full_name='items.Book.pub_date', index=9,
      number=10, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='size', full_name='items.Book.size', index=10,
      number=11, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='pub', full_name='items.Book.pub', index=11,
      number=12, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='pages', full_name='items.Book.pages', index=12,
      number=13, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=27,
  serialized_end=236,
)

DESCRIPTOR.message_types_by_name['Book'] = _BOOK
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

Book = _reflection.GeneratedProtocolMessageType('Book', (_message.Message,), dict(
  DESCRIPTOR = _BOOK,
  __module__ = 'src.items_pb2'
  # @@protoc_insertion_point(class_scope:items.Book)
  ))
_sym_db.RegisterMessage(Book)


# @@protoc_insertion_point(module_scope)
