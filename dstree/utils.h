#ifndef utils_h
#define utils_h

#ifdef __cplusplus
extern "C" {
#endif

void my_assert(bool expression, const char* expressionStr, const char* file, int line, const char* function);

#define ASSERT(expression) my_assert((expression), #expression, __FILE__, __LINE__, __FUNCTION__)

#ifdef __cplusplus
}
#endif

#endif