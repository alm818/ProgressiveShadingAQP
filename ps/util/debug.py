import inspect

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

    # Extract variable names from the call line
    start = call_line.find('(') + 1
    end = call_line.find(')')
    var_names = call_line[start:end].split(',')

    # ANSI escape code for red color
    red_color = "\033[91m"
    reset_color = "\033[0m"

    # Print the file name and line number
    print(f"{red_color}{file_name}, line {line_number}:{reset_color}")

    # Print each variable name and its value
    for name, value in zip(var_names, args):
        print(f"{name.strip()} = {value}")
