import inspect, logging, ast

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
# Get the global logger
logger = logging.getLogger('global_logger')

def deb(*args):
    # Get the frame of the caller
    frame = inspect.currentframe().f_back
    # Get the file name and line number of the caller
    file_name = frame.f_code.co_filename
    line_number = frame.f_lineno

    # Get the source code of the caller
    source_code = inspect.getsourcelines(frame)[0]
    # Find the line calling the function
    call_line = source_code[line_number - frame.f_code.co_firstlineno].strip()

    # Extract everything between parentheses
    start = call_line.find('(') + 1
    end = call_line.rfind(')')
    expressions = call_line[start:end]

    # Parse the expression into an AST (Abstract Syntax Tree) to handle complex expressions
    parsed_expr = ast.parse(expressions)

    # ANSI escape code for red color
    red_color = "\033[91m"
    reset_color = "\033[0m"

    # Print the file name and line number
    print(f"{red_color}{file_name}, line {line_number}:{reset_color}")

    # Depending on the parsed expression type, handle both individual calls and collections
    expr_nodes = parsed_expr.body[0].value
    if isinstance(expr_nodes, ast.Tuple):
        # If the expression is a tuple (multiple args), process each
        for i, (expr_node, value) in enumerate(zip(expr_nodes.elts, args)):
            expr_str = ast.get_source_segment(expressions, expr_node)
            print(f"{expr_str.strip()} = {value}")
    else:
        # Handle single expressions (e.g., function calls like len(tid_set))
        expr_str = ast.get_source_segment(expressions, expr_nodes)
        print(f"{expr_str.strip()} = {args[0]}")