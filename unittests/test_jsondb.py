import pytest
from nanolog_parser.src.formatters.jsondb import JSONFlattener

# Sample data for the test
sample_json_lines = """{"name": "Alice", "age": 30}
{"type": "employee", "id": 123, "data": {"department": "sales", "manager": "Bob"}}"""


def test_flatten_lines():
    expected_result = [{'sql_id': 1, 'name': 'Alice', 'age': 30}, {'sql_id': 2, 'type': 'employee', 'id': 123}]
    expected_children =  {'data': [{'sql_id': 1, 'department': 'sales', 'manager': 'Bob'}]} 
    expected_mappings =  [{'main_type': 'root', 'main_sql_id': 2, 'link_type': 'data', 'link_sql_id': 1}]
    actual_result, children, mappings = JSONFlattener().flatten_lines(sample_json_lines)
    assert actual_result == expected_result
    assert children == expected_children
    assert mappings == expected_mappings
    
    
# Sample data for the test
sample_json_lines2 = """{"type": "employee", "id": 1, "data": {"department": "sales", "manager": "Kim"}}
{"type": "employee", "id": 2, "data": {"department": "sales", "manager": "Bob"}}"""

def test_flatten_lines_2():
    expected_result = [{'sql_id': 1, 'type': 'employee', 'id': 1},{'sql_id': 2, 'type': 'employee', 'id': 2}]
    expected_children =  {'data': [{'sql_id': 1, 'department': 'sales', 'manager': 'Kim'}, {'sql_id': 2, 'department': 'sales', 'manager': 'Bob'}]} 
    expected_mappings =  [{'main_type': 'root', 'main_sql_id': 1, 'link_type': 'data', 'link_sql_id': 1}, {'main_type': 'root', 'main_sql_id': 2, 'link_type': 'data', 'link_sql_id': 2}]
    actual_result, children, mappings = JSONFlattener().flatten_lines(sample_json_lines2)
    assert actual_result == expected_result
    assert children == expected_children
    assert mappings == expected_mappings
    


# Sample data for the test
sample_json_lines3 = """{"type": "employee", "id": 1, "data": {"department": "sales", "manager": "Kim"}}
{"type": "employee", "id": 2, "data": {"department": "sales", "manager": "Kim"}}"""

#Same nested element
def test_flatten_lines_3():
    expected_result = [{'sql_id': 1, 'type': 'employee', 'id': 1},{'sql_id': 2, 'type': 'employee', 'id': 2}]
    expected_children =  {'data': [{'sql_id': 1, 'department': 'sales', 'manager': 'Kim'}]} 
    expected_mappings =  [{'main_type': 'root', 'main_sql_id': 1, 'link_type': 'data', 'link_sql_id': 1}, {'main_type': 'root', 'main_sql_id': 2, 'link_type': 'data', 'link_sql_id': 1}]
    actual_result, children, mappings = JSONFlattener().flatten_lines(sample_json_lines3)
    assert actual_result == expected_result
    assert children == expected_children
    assert mappings == expected_mappings




sample_json_lines4 = """{"type": "employee", "id": 1, "data": {"department": "sales", "manager": "Kim"}}
{"type": "employee", "id": 3, "data": {"department": "sales", "manager": "Bob"}}
{"type": "employee", "id": 2, "data": {"department": "sales", "manager": "Kim"}}
{"type": "employee", "id": 7, "data": {"department": "sales", "manager": "Jane"}}"""

#Same nested element
def test_flatten_lines_4():
    expected_result = [{'sql_id': 1, 'type': 'employee', 'id': 1},
                    {'sql_id': 2, 'type': 'employee', 'id': 3},
                    {'sql_id': 3, 'type': 'employee', 'id': 2},
                    {'sql_id': 4, 'type': 'employee', 'id': 7}]
    expected_children =  {'data': [{'sql_id': 1, 'department': 'sales', 'manager': 'Kim'},
                                {'sql_id': 2, 'department': 'sales', 'manager': 'Bob'},
                                {'sql_id': 3, 'department': 'sales', 'manager': 'Jane'}]} 
    expected_mappings =  [{'main_type': 'root', 'main_sql_id': 1, 'link_type': 'data', 'link_sql_id': 1},
                        {'main_type': 'root', 'main_sql_id': 2, 'link_type': 'data', 'link_sql_id': 2}, 
                        {'main_type': 'root', 'main_sql_id': 3, 'link_type': 'data', 'link_sql_id': 1},
                        {'main_type': 'root', 'main_sql_id': 4, 'link_type': 'data', 'link_sql_id': 3}]
    actual_result, children, mappings = JSONFlattener().flatten_lines(sample_json_lines4)
    assert actual_result == expected_result
    assert children == expected_children
    assert mappings == expected_mappings