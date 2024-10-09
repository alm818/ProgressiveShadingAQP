#ifndef UDECLARE_HPP
#define UDECLARE_HPP

#include <map>
#include <sstream>
#include <ostream>
#include <utility>
#include <fmt/core.h>
#include <libpq-fe.h>
#include <memory>
#include <string>
#include <vector>
#include <type_traits>
#include <stdexcept>
#include <cstdlib>
#include <memory>
#include <random>
#include <filesystem>
#include <boost/algorithm/string.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/variant.hpp>
#include <boost/preprocessor.hpp>

#include "udebug.hpp"

namespace fs = std::filesystem;

using std::string;
using std::ostringstream;
using std::pair;
using std::vector;
using std::ostream;
using std::map;
using std::to_string;
using std::shared_ptr;
using std::unique_ptr;

/**
 * @brief ENUM MACRO
 * 
 */ 

#define ENUM(name, ...)  \
    enum name { __VA_ARGS__ };\
    extern string str(name value); \
    extern name to_##name(size_t index); \
    extern int to_index(name value); \
    extern ostream& operator<<(ostream& os, const name& value);\
    extern map<string, name> to##name

ENUM(Column, numeric_type, string_type, array_type, unsupported);

class Config{
private:
    long long seedMode;
    std::random_device rd;
protected:
    static shared_ptr<Config> config;
public:
    boost::property_tree::ptree pt;
public:
    Config();
    Config(Config &other) = delete;
    void operator=(const Config&) = delete;
    unsigned int seed();
    static shared_ptr<Config> getInstance();
};

struct PGconnDeleter {
    void operator()(PGconn* conn) {
        if (conn) PQfinish(conn);
    }
};

using PGconnPtr = std::unique_ptr<PGconn, PGconnDeleter>;

void check(PGconnPtr& conn, PGresult* res, const char* file, const int& line);
void check(PGconnPtr& conn, bool success, const char* file, const int& line);

#define ck(conn, arg) check(conn, arg, __FILE__, __LINE__)

class PgManager{
private:
    static string conninfo;
    static vector<vector<string>> typeGroups;
private:
    static string getConnInfo();
    static Column getColumn(string dataType);
public:
    static string schema;
    PGconnPtr conn;
public:
    PgManager();
    long long getTableSize(const string& tableName);
    vector<string> getTables();
    map<string, Column> getColumns(const string& tableName);
    int getColumnLength(const string& tableName, const string& columnName);
    long long getPageCount(const string& tableName);
    void addColumn(const string& tableName, const string& columnName, const string& pgType);
    bool existTable(const string& tableName);
    void dropTable(const string& tableName);
    
    string getUniqueSerialKey(const string &tableName);
    vector<string> getAllKeys(const string& tableName);
};

class SingleRow{
private:
    unique_ptr<PgManager> pg;
    PGresult* res;
public:
    ~SingleRow();
    SingleRow(const string& query);
    bool fetchRow();
    long long getBigInt(const int& columnIndex);
    double getNumeric(const int& columnIndex);
};

#endif