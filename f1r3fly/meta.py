from dataclasses import MISSING, Field, fields as dataclass_fields
from typing import List

_PB_PARAM = "PB"
_FROM_PB = "__from_pb"


def _make_from_pb_fn(flds: List[Field]):
    """Build a from_pb function dynamically.
    
    Creates a function that constructs a dataclass instance from a protobuf object.
    """
    globals_dict = {}
    
    # Add field types/factories to globals for use in generated code
    for f in flds:
        if f.default_factory is not MISSING:
            globals_dict[f'_pb_type_{f.name}'] = f.default_factory
        elif getattr(f.type, _FROM_PB, None):
            # Use the type directly if it has from_pb but no factory
            globals_dict[f'_pb_type_{f.name}'] = f.type
    
    body_lines = []
    for f in flds:
        field_type_name = getattr(f.type, "_name", None)
        if field_type_name == "List":
            # Check if this is a List of from_pb types
            list_inner_type = getattr(f.type, "__args__", (None,))[0]
            if list_inner_type and getattr(list_inner_type, _FROM_PB, None):
                globals_dict[f'_pb_type_{f.name}'] = list_inner_type
                body_lines.append(
                    f"        {f.name}=[_pb_type_{f.name}.from_pb(_pb) for _pb in {_PB_PARAM}.{f.name}],")
            elif f.default_factory is not MISSING:
                body_lines.append(
                    f"        {f.name}=[_pb_type_{f.name}.from_pb(_pb) for _pb in {_PB_PARAM}.{f.name}],")
            else:
                body_lines.append(f"        {f.name}=[i for i in {_PB_PARAM}.{f.name}],")
        elif getattr(f.type, _FROM_PB, None):
            body_lines.append(
                f"        {f.name}=_pb_type_{f.name}.from_pb({_PB_PARAM}.{f.name}),")
        else:
            body_lines.append(f"        {f.name}={_PB_PARAM}.{f.name},")
    
    body = '\n'.join(body_lines)
    func_def = f"""def from_pb(cls, {_PB_PARAM}):
    return cls(
{body}
    )
"""
    
    local_ns: dict = {}
    exec(func_def, globals_dict, local_ns)
    return local_ns['from_pb']


def _process_cls(cls):
    """Process a dataclass to add the from_pb classmethod."""
    flds = list(dataclass_fields(cls))
    fn = _make_from_pb_fn(flds)
    if not hasattr(cls, "from_pb"):
        setattr(cls, "from_pb", classmethod(fn))
    if not hasattr(cls, _FROM_PB):
        setattr(cls, _FROM_PB, True)
    return cls


def from_pb(_cls):
    """
    Make sure _cls is a wrap by dataclass.
    Generate a from_pb class method for the dataclass based
    on the dataclass definition.
    """

    def wrap(cls):
        return _process_cls(cls)

    if _cls is None:
        return wrap

    return wrap(_cls)
