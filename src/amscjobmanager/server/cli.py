"""Generate transport-independent command arguments and help from JSON schemas."""

import argparse
import json

import jsonschema
from ..manager_functions import FunctionDescription


def _format_schema(schema: dict, *, root: dict | None = None, label='', indent=0, seen=()) -> str:
    """Render JSON-schema types, field descriptions, and nesting as readable text.

    Args:
        schema (dict): Schema node to describe, including optional local $ref.
        root (dict | None): Schema document used to resolve local JSON pointers;
            None uses schema itself. References are expanded once per path.
        label (str): Optional field name, including required-key notation.
        indent (int): Number of leading spaces, initially zero.
        seen (tuple[str, ...]): References already expanded on this path, used
            to stop recursion in self-referential schemas.

    Returns:
        str: Multiline type/description tree. Object fields are named; array
            entries use [0..] to indicate zero-based positions, not database IDs.
    """
    root = schema if root is None else root
    if '$ref' in schema:
        reference = schema['$ref']
        if not reference.startswith('#/') or reference in seen:
            return ' ' * indent + (label + ': ' if label else '') + reference.rsplit('/', 1)[-1]
        target = root
        for part in reference[2:].split('/'):
            target = target[part.replace('~1', '/').replace('~0', '~')]
        schema = {**target, **{key: value for key, value in schema.items() if key != '$ref'}}
        seen = (*seen, reference)

    branches = schema.get('anyOf', schema.get('oneOf', []))
    kind = schema.get('type', 'any JSON value')
    if branches:
        kind = ' | '.join(_format_schema(branch, root=root, seen=seen).splitlines()[0].split(' — ', 1)[0]
                          for branch in branches)
    elif kind == 'array':
        item_type = _format_schema(schema.get('items', {}), root=root, seen=seen).splitlines()[0].split(' — ', 1)[0]
        kind = f'array[{item_type}]'
    elif isinstance(kind, list):
        kind = ' | '.join(kind)
    constraints = [f'{key}={schema[key]}' for key in
                   ('minimum', 'maximum', 'exclusiveMinimum', 'exclusiveMaximum', 'minItems', 'maxItems') if key in schema]
    if 'const' in schema:
        constraints.append('value: ' + json.dumps(schema['const']))
    if 'enum' in schema:
        constraints.append('choices: ' + ', '.join(json.dumps(value) for value in schema['enum']))
    if constraints:
        kind += ' (' + '; '.join(constraints) + ')'
    description = schema.get('description', '')
    if not description and branches:
        descriptions = [branch['description'] for branch in branches if branch.get('description')]
        if len(descriptions) == 1:
            description = descriptions[0]
    description = ' '.join(description.split('\n\n', 1)[0].split())
    line = ' ' * indent + (label + ': ' if label else '') + kind
    if description:
        line += ' — ' + description
    lines = [line]
    for name, field in schema.get('properties', {}).items():
        field_label = name + (' (required key)' if name in schema.get('required', []) else '')
        lines.append(_format_schema(field, root=root, label=field_label, indent=indent + 2, seen=seen))
    if schema.get('type') == 'array':
        lines.append(_format_schema(schema.get('items', {}), root=root, label='[0..]', indent=indent + 2, seen=seen))
    # Expand structured union variants so stage/operation type tags and their
    # individual fields appear in generated help, including nested API stages.
    if branches and any('$ref' in branch or 'properties' in branch for branch in branches):
        for index, branch in enumerate(branches, start=1):
            lines.append(_format_schema(branch, root=root, label=f'option {index}',
                                        indent=indent + 2, seen=seen))
    return '\n'.join(lines)


def parse_function_arguments(function: FunctionDescription, arguments: list[str], *,
                             required_positionals=False, program='manager_client.py',
                             connection_arguments='') -> dict | str:
    """Parse CLI tokens using the server's schema, or return tool help text.

    Args:
        function (FunctionDescription): Discovered schemas/description; input
            underscores become option hyphens. parameter_order orders positionals.
        arguments (list[str]): Tokens after the tool name, excluding global
            connection options. required_positionals (bool, default False)
            makes required fields positional; program (str) and
            connection_arguments (str) customize usage/help. Integers/numbers/strings take one value; bools
            use --flag/--no-flag; arrays take one or more values. Objects and
            other complex schemas take JSON values. Omitted fields are not
            sent, leaving server defaults intact. -h/--help requests help.

    Returns:
        dict | str: Validated argument values, or formatted help including
            description, input types/options and output fields/types without
            executing the tool. No network calls are performed here.

    Raises:
        ValueError: Unknown/malformed options, missing required fields, or
            arguments incompatible with the advertised JSON schema.
    """
    prefix = ' '.join(part for part in (program, connection_arguments) if part)
    parser = argparse.ArgumentParser(
        prog=f'{prefix} {function.name}', description=(function.description or '').split('\n\n', 1)[0],
        exit_on_error=False, allow_abbrev=False, add_help=False,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(f"Show this command's help: {prefix} -h {function.name} "
                f'or {prefix} --help {function.name}\n'
                f'Show the main help page: {prefix} -h or {prefix} --help'),
    )
    schema = function.input_schema
    try:
        properties = schema.get('properties', {})
        order = getattr(function, 'parameter_order', None) or list(properties)
        for name in order:
            field = properties[name]
            # A nullable scalar/array uses its non-null option type. Omission
            # leaves its default on the server; JSON mode supports explicit null.
            branches = field.get('anyOf', [])
            non_null = [branch for branch in branches if branch.get('type') != 'null']
            shape = {**field, **non_null[0]} if len(non_null) == 1 else field
            kind = shape.get('type')
            if isinstance(kind, list):
                kinds = [value for value in kind if value != 'null']
                kind = kinds[0] if len(kinds) == 1 else None
            description = _format_schema(field, root=schema).splitlines()[0]
            if 'default' in field:
                description += f" (server default: {json.dumps(field['default'])})"
            if name in schema.get('required', []):
                description += ' (required)'
            options = {'dest': name, 'default': argparse.SUPPRESS, 'help': description}
            positional = required_positionals and name in schema.get('required', [])
            if positional:
                options.pop('dest')
            if kind == 'boolean' and not positional:
                options['action'] = argparse.BooleanOptionalAction
            else:
                value_schema = shape.get('items', {}) if kind == 'array' else shape
                value_kind = value_schema.get('type')
                if value_kind is None and all(isinstance(value, str) for value in value_schema.get('enum', [None])):
                    value_kind = 'string'
                options['type'] = {'integer': int, 'number': float, 'string': str}.get(value_kind, json.loads)
                if kind == 'array':
                    options['nargs'] = '+'
                if 'enum' in value_schema:
                    options['choices'] = value_schema['enum']
            parser.add_argument(name if positional else '--' + name.replace('_', '-'), **options)

        if '-h' in arguments or '--help' in arguments:
            outputs = _format_schema(function.output_schema) if function.output_schema else 'No structured output schema advertised by the server.'
            return (parser.format_help() + '\nInputs (server schema):\n' + _format_schema(schema)
                    + '\n\nOutputs (server schema):\n' + outputs + '\n')
        parsed, unknown = parser.parse_known_args(arguments)
        if unknown:
            raise ValueError('Unrecognized tool arguments: ' + ' '.join(unknown))
        values = vars(parsed)
        # Validate bounds, required fields and complex JSON after conversion.
        # Explicit False and zero are retained; omitted defaults remain omitted.
        json.dumps(values, allow_nan=False)
        jsonschema.validate(values, schema)
    except (argparse.ArgumentError, jsonschema.ValidationError, jsonschema.SchemaError) as exc:
        raise ValueError(getattr(exc, 'message', str(exc))) from exc
    return values


def format_function_list(functions: list[FunctionDescription]) -> str:
    """Format discovered functions (list[FunctionDescription]) as a command overview.

    Return str with command names aligned beside each description's first line.
    An empty list reports that no commands are advertised. No transport calls occur.
    """
    lines = ['Commands (from server):']
    width = max((len(function.name) for function in functions), default=0)
    for function in functions:
        brief = (function.description or 'No description provided.').splitlines()[0]
        lines.append(f'  {function.name:<{width}}  {brief}')
    if not functions:
        lines.append('  No commands are advertised by this server.')
    return '\n'.join(lines) + '\n'
