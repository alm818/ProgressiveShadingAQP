from ps.util.debug import deb
from sqlparse import sql, tokens
from typing import List

AND = 'AND'
OR = 'OR'
LTE = '<='
LT = '<'
GTE = '>='
GT = '>'

def is_op_less(op):
    return str(op) in [LTE, LT]

def is_op_greater(op):
    return str(op) in [GTE, GT]

class ASTNode:
    pass

class BinaryOp(ASTNode):

    def __init__(self, left, op, right):
        self.left = left
        self.op = op
        self.right = right

    def is_leaf(self):
        return not isinstance(self.left, BinaryOp) and not isinstance(self.right, BinaryOp)

    def is_condition(self):
        return str(self.op) not in [AND, OR]

# class UnaryOp(ASTNode):
#     def __init__(self, op, operand):
#         self.op = op
#         self.operand = operand

class Variable(ASTNode):

    def __init__(self, name):
        self.name = name

class Constant(ASTNode):

    def __init__(self, value):
        self.value = value

precedence = {AND:1, OR:1, LT:2, LTE:2, GT:2, GTE:2, '=':2, '!=':2}

def get_precedence(op):
    return precedence[op.value.upper()] if op.value.upper() in precedence else 0

def is_operator(token):
    return token.value.upper() in precedence and (token.ttype == tokens.Keyword or token.ttype == tokens.Operator.Comparison)

def is_variable(token):
    return token.ttype == tokens.Name

class Predicate:

    ast: BinaryOp
    variable_names: List[str]

    def __init__(self, where):
        postfix = []
        stack = []
        for token in list(where.flatten())[1:]:
            if not token.is_whitespace:
                if is_operator(token):
                    while stack and get_precedence(token) <= get_precedence(stack[-1]):
                        postfix.append(stack.pop())
                    stack.append(token)
                elif token.value == '(':
                    stack.append(token)
                elif token.value == ')':
                    while stack and stack[-1].value != '(':
                        postfix.append(stack.pop())
                    stack.pop()
                else:
                    postfix.append(token)
        while stack:
            postfix.append(stack.pop())

        self.variable_names = set()
        for token in postfix:
            if is_operator(token):
                right = stack.pop()
                left = stack.pop()
                stack.append(BinaryOp(left, token, right))
            elif is_variable(token):
                stack.append(Variable(token.value))
                self.variable_names.add(token.value)
            else:
                stack.append(Constant(token.value))
        self.ast = stack.pop()
        self.variable_names = list(self.variable_names)        