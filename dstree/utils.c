#include <stdio.h>
#include <stdlib.h>

void my_assert(int expression, const char* expressionStr, const char* file, int line, const char* function) {
    if (!expression) {
        fprintf(stderr, "Assertion failed: (%s), function %s, file %s, line %d.\n",
                expressionStr, function, file, line);
        exit(EXIT_FAILURE);
    }
}