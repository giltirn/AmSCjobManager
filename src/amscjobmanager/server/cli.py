"""Translate an MCP tool's input schema into traditional command-line options."""

import argparse
import json

import jsonschema
from mcp.types import Tool

from .client import ToolRequest


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
    return '\n'.join(lines)


def parse_tool_arguments(tool: Tool, arguments: list[str]) -> ToolRequest | str:
    """Parse CLI tokens using the server's schema, or return tool help text.

    Args:
        tool (mcp.types.Tool): Discovered tool, including name, description,
            input_schema and output_schema. Underscores become option hyphens.
        arguments (list[str]): Tokens after the tool name, excluding global
            connection options. Integers/numbers/strings take one value; bools
            use --flag/--no-flag; arrays take one or more values. Objects and
            other complex schemas take JSON values. Omitted fields are not
            sent, leaving server defaults intact. -h/--help requests help.

    Returns:
        ToolRequest | str: Validated request model, or formatted help including
            description, input types/options and output fields/types without
            executing the tool. No network calls are performed here.

    Raises:
        ValueError: Unknown/malformed options, missing required fields, or
            arguments incompatible with the advertised JSON schema.
    """
    parser = argparse.ArgumentParser(
        prog=f'manager_client.py {tool.name}', description=(tool.description or '').split('\n\n', 1)[0],
        exit_on_error=False, allow_abbrev=False, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    schema = tool.input_schema
    try:
        for name, field in schema.get('properties', {}).items():
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
            if kind == 'boolean':
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
            parser.add_argument('--' + name.replace('_', '-'), **options)

        if '-h' in arguments or '--help' in arguments:
            outputs = _format_schema(tool.output_schema) if tool.output_schema else 'No structured output schema advertised by the server.'
            return parser.format_help() + '\nOutputs (server schema):\n' + outputs + '\n'
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
    return ToolRequest(tool=tool.name, arguments=values)
