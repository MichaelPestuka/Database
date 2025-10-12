### B Plus Node
    type | key_count | key_offsets    | value_offsets  | keys | pointers/values |
    1B   | 2B        | key_count * 2B | key_count * 2B | nB   | nB              |

### Table
    | prefix | name | n_records | n * record_type | n * record_name |

    name/record_name = | len(name) | name |