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
    
    
    
    
sample_json_lines5 = """{"type": "employee", "id": 1, "entries": [{"hash": "X"},{"hash": "Y"},{"hash": "Z"} ]}
{"type": "else", "entries": [{"hash": "X"},{"hash": "A"}]}"""


#Same nested element
def test_flatten_lines_5():
    expected_result = [{"sql_id": 1,"type": "employee","id": 1},
                       {"sql_id": 2,"type": "else"}]
    
    expected_children =  { "entries": [{"sql_id": 1,"hash": "X"},
                                       {"sql_id": 1,"hash": "Y"},
                                       {"sql_id": 1,"hash": "Z"},
                                       {"sql_id": 4,"hash": "A"}]}
    
    expected_mappings =[{"main_type": "root","main_sql_id": 1,"link_type": "entries","link_sql_id": 1},
                        {"main_type": "root","main_sql_id": 1,"link_type": "entries","link_sql_id": 2},
                        {"main_type": "root","main_sql_id": 1,"link_type": "entries","link_sql_id": 3},
                        {"main_type": "root","main_sql_id": 2,"link_type": "entries","link_sql_id": 1},
                        {"main_type": "root","main_sql_id": 2,"link_type": "entries","link_sql_id": 4}]
    
    actual_result, children, mappings = JSONFlattener().flatten_lines(sample_json_lines5)
    
    assert actual_result == expected_result
    assert children == expected_children
    assert mappings == expected_mappings


sample_json_lines6 = """{"type": "employee", "id": 1, "entries": [{"hash": "X"},{"hash": "Y"},{"hash": "Z"} ]}
{"type": "else", "entry": {"hash": "X"}}
{"type": "else", "entry": {"hash": "A"}}"""
#Same nested element
def test_flatten_lines_6():
    expected_result = [{"sql_id": 1,"type": "employee","id": 1},
                       {"sql_id": 2,"type": "else"},
                       {"sql_id": 3,"type": "else"}]
    
    expected_children =  { "entries": [{"sql_id": 1,"hash": "X"},
                                       {"sql_id": 1,"hash": "Y"},
                                       {"sql_id": 1,"hash": "Z"},
                                       {"sql_id": 4,"hash": "A"}]}
    
    expected_mappings =[{"main_type": "root","main_sql_id": 1,"link_type": "entries","link_sql_id": 1},
                        {"main_type": "root","main_sql_id": 1,"link_type": "entries","link_sql_id": 2},
                        {"main_type": "root","main_sql_id": 1,"link_type": "entries","link_sql_id": 3},
                        {"main_type": "root","main_sql_id": 2,"link_type": "entries","link_sql_id": 1},
                        {"main_type": "root","main_sql_id": 3,"link_type": "entries","link_sql_id": 4}]
    
    flattener = JSONFlattener()
    flattener.add_key_mappings({"entry" : "entries"})
    
   
    
    actual_result, children, mappings = flattener.flatten_lines(sample_json_lines6)
    
    import json
    print(json.dumps(flattener.key_mappings, indent=4))
    print(json.dumps(actual_result, indent=4))
    print(json.dumps(children, indent=4))
    print(json.dumps(mappings, indent=4))
 
    assert actual_result == expected_result
    assert children == expected_children
    assert mappings == expected_mappings