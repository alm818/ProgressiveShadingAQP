#include <cassert>
#include <boost/property_tree/ini_parser.hpp>
#include <boost/tokenizer.hpp>

#include "udeclare.hpp"

using std::make_shared;
using std::make_unique;
using boost::algorithm::to_lower;

#define PROCESS_ONE_ELEMENT(r, unused, idx, elem) \
	BOOST_PP_COMMA_IF(idx) BOOST_PP_STRINGIZE(elem)

#define ENUM_INIT(name, ...)\
	vector<string> from##name = { BOOST_PP_SEQ_FOR_EACH_I(PROCESS_ONE_ELEMENT, %%, BOOST_PP_VARIADIC_TO_SEQ(__VA_ARGS__)) };\
	string str(name value) { return from##name[static_cast<int>(value)];} \
	name to_##name(size_t index) { assert(index>=0 && index<from##name.size()); return static_cast<name>(index);}\
	int to_index(name value) { return static_cast<int>(value); }\
	ostream& operator<<(ostream& os, const name& value){ os << str(value); return os;}\
	map<string, name> init##name(vector<string> vec) { map<string, name> res; int index = 0; for (const auto& str : vec) res[str] = static_cast<name>(index++); return res;} \
	map<string, name> to##name = init##name(from##name)

ENUM_INIT(Column, numeric_type, string_type, array_type, unsupported);

Config::Config(){
	fs::path configPath = fs::current_path() / "config.txt";
	boost::property_tree::ini_parser::read_ini(configPath.string(), pt);

    seedMode = pt.get<long long>("parameters.seed");
    if (seedMode == -1) seedMode = rd();
}

shared_ptr<Config> Config::config = nullptr;

shared_ptr<Config> Config::getInstance(){
    if (config == nullptr){
        config = std::make_shared<Config>();
    }
    return config;
}

unsigned int Config::seed(){
    if (seedMode >= 0) return static_cast<unsigned int>(seedMode);
    return rd();
}

string PgManager::schema;
vector<vector<string>> PgManager::typeGroups (toColumn.size()-1);

string PgManager::getConnInfo(){
	auto pt = Config::getInstance()->pt;
	schema = pt.get<string>("postgres.schema");
	for (size_t i = 0; i < typeGroups.size(); ++i){
		string columnType = str(static_cast<Column>(i));
		boost::split(typeGroups[i], pt.get<string>(fmt::format("pgmanager.{}", columnType)), boost::is_any_of(","));
	}

	return fmt::format("postgresql://{}@{}?port={}&dbname={}&password={}", 
		pt.get<string>("postgres.username"), 
		pt.get<string>("postgres.hostname"),
		pt.get<string>("postgres.port"), 
		pt.get<string>("postgres.database"), 
		pt.get<string>("postgres.password"));
}

string PgManager::conninfo = PgManager::getConnInfo();

Column PgManager::getColumn(string dataType){
	to_lower(dataType);
	for (int i = typeGroups.size()-1; i >= 0; --i){
		for (auto token : typeGroups[i]){
			Column column = to_Column(i);
			if (dataType.find(token) != string::npos){
				if (column == Column::array_type){
					bool isNumericColumn = false;
					for (auto numericToken : typeGroups[to_index(column)]){
						if (dataType.find(numericToken) != string::npos){
							isNumericColumn = true;
							break;
						}
					}
					if (!isNumericColumn) return Column::unsupported;
				}
				return column;
			}
		}
	}
	return Column::unsupported;
}

const vector<ExecStatusType> okTypes = {
	PGRES_SINGLE_TUPLE,
	PGRES_TUPLES_OK,
	PGRES_COMMAND_OK,
	PGRES_PIPELINE_SYNC,
	PGRES_COPY_IN,
    PGRES_COPY_OUT
};

void check(PGconnPtr& conn, PGresult* res, const char* file, const int& line){
	auto status = PQresultStatus(res);
	bool isBad = true;
	for (auto okType : okTypes){
		if (status == okType){
			isBad = false;
			break;
		}
	}
	if (isBad) {
		cout << RED << "File " << file  \
			<< ", Line " << line << RESET << "\n";
		cout << PQerrorMessage(conn.get());
		PQclear(res);
		conn.reset();
		exit(1);
	}
}

void check(PGconnPtr& conn, bool success, const char* file, const int& line){
	if (!success){
		cout << RED << "File " << file  \
			<< ", Line " << line << RESET << "\n";
		cout << PQerrorMessage(conn.get());
		conn.reset();
		exit(1);
	}
}

PgManager::PgManager(){
	conn = PGconnPtr(PQconnectdb(conninfo.c_str()), PGconnDeleter());
	ck(conn, PQstatus(conn.get()) == CONNECTION_OK);
}

/**
 * @brief Get the size of the table
 * 
 * @param tableName table's name
 * @return long long the size of the table, 0 if the table neither not exists nor has no rows
 */
long long PgManager::getTableSize(const string& tableName){
	string sql = fmt::format("SELECT COUNT(*) FROM \"{}\"", tableName);
	auto res = PQexec(conn.get(), sql.c_str());
	ck(conn, res);
	long long size = 0;
	if (PQntuples(res) > 0) size = atoll(PQgetvalue(res, 0, 0));
	PQclear(res);
	return size;
}

vector<string> PgManager::getTables(){
	string sql = "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'";
	auto res = PQexec(conn.get(), sql.c_str());
	ck(conn, res);
	vector<string> tables;
	for (int i = 0; i < PQntuples(res); ++i) tables.push_back(string(PQgetvalue(res, i, 0)));
	PQclear(res);
	return tables;
}

map<string, Column> PgManager::getColumns(const string& tableName){
	string sql = fmt::format(" \
		SELECT column_name, udt_name::regtype FROM information_schema.columns \
		WHERE table_schema = '{}' AND table_name = '{}'", schema, tableName);
	auto res = PQexec(conn.get(), sql.c_str());
	ck(conn, res);
	map<string, Column> columns;
	for (int i = 0; i < PQntuples(res); i ++) columns[string(PQgetvalue(res, i, 0))] = PgManager::getColumn(string(PQgetvalue(res, i, 1)));
	PQclear(res);
	return columns;
}

int PgManager::getColumnLength(const string& tableName, const string& columnName){
	int len = 0;
	string sql = fmt::format(" \
		SELECT udt_name::regtype FROM information_schema.columns \
		WHERE table_schema = '{}' AND table_name = '{}' AND column_name='{}'", schema, tableName, columnName);
	auto res = PQexec(conn.get(), sql.c_str());
	ck(conn, res);
	auto columnType = PgManager::getColumn(string(PQgetvalue(res, 0, 0)));
	PQclear(res);
	if (columnType == Column::numeric_type || columnType == Column::string_type) len=1;
	else if (columnType == Column::array_type){
		sql = fmt::format("SELECT array_length({}, 1) FROM \"{}\" LIMIT 1", columnName, tableName);
		res = PQexec(conn.get(), sql.c_str());
		ck(conn, res);
		len = atoi(PQgetvalue(res, 0, 0));
		PQclear(res);
	}
	return len;
}

long long PgManager::getPageCount(const string& tableName){
	string sql = fmt::format("SELECT pg_relation_size('{}') / current_setting('block_size')::int AS total_pages;", tableName);
	auto res = PQexec(conn.get(), sql.c_str());
	ck(conn, res);
    long long count = atoll(PQgetvalue(res, 0, 0));
	PQclear(res);
    return count;
}

void PgManager::addColumn(const string& tableName, const string& columnName, const string& pgType){
    string sql = fmt::format("SELECT 1 FROM information_schema.columns WHERE table_name = '{}' AND column_name = '{}'", tableName, columnName);
	auto res = PQexec(conn.get(), sql.c_str());
	ck(conn, res);
    if (PQntuples(res)){
        PQclear(res);
        string index_name = tableName + "_" + columnName;
        sql = fmt::format("SELECT indexname FROM pg_indexes WHERE tablename = '{}' AND indexname = '{}'", tableName, index_name);
        res = PQexec(conn.get(), sql.c_str());
        ck(conn, res);
        if (PQntuples(res)){
            PQclear(res);
            sql = fmt::format("DROP INDEX {}", index_name);
            res = PQexec(conn.get(), sql.c_str());
            ck(conn, res);
        }
        PQclear(res);
        return;
    }
    PQclear(res);
	sql = fmt::format("ALTER TABLE \"{}\" ADD COLUMN \"{}\" {}", tableName, columnName, pgType);
	res = PQexec(conn.get(), sql.c_str());
	ck(conn, res);
	PQclear(res);
}

bool PgManager::existTable(const string& tableName){
	string sql = fmt::format("SELECT * FROM pg_tables WHERE tablename='{}' AND schemaname='{}'", tableName, schema);
	auto res = PQexec(conn.get(), sql.c_str());
	ck(conn, res);
	bool exists = PQntuples(res) > 0;
	PQclear(res);
	return exists;
}

void PgManager::dropTable(const string& tableName){
	string sql = fmt::format("DROP TABLE \"{}\"", tableName);
	auto res = PQexec(conn.get(), sql.c_str());
	ck(conn, res);
	PQclear(res);
}

string PgManager::getUniqueSerialKey(const string &tableName){
    string sql = fmt::format("                              \
        SELECT kcu.column_name                              \
        FROM information_schema.table_constraints tc        \
        JOIN information_schema.key_column_usage kcu        \
          ON tc.constraint_name = kcu.constraint_name       \
         AND tc.table_schema = kcu.table_schema             \
        WHERE tc.constraint_type = 'UNIQUE' AND tc.table_name = '{}'", tableName);
	auto res = PQexec(conn.get(), sql.c_str());
	ck(conn, res);
    auto ukey_count = PQntuples(res);
    if (ukey_count){
        vector<string> ukeys (ukey_count);
        for (int i = 0; i < ukey_count; ++i) ukeys[i] = string(PQgetvalue(res, i, 0));
        PQclear(res);
        for (auto ukey : ukeys){
            sql = fmt::format("SELECT pg_get_serial_sequence('{}', '{}') AS sequence_name;", tableName, ukey);
            auto res = PQexec(conn.get(), sql.c_str());
            ck(conn, res);
            if (PQntuples(res)){
                string seq = string(PQgetvalue(res, 0, 0));
                PQclear(res);
                vector<string> result;
                boost::split(result, seq, boost::is_any_of("."));
                sql = fmt::format("SELECT start_value           \
                    FROM   pg_sequences                         \
                    WHERE  schemaname = '{}' AND sequencename = '{}';", result[0], result[1]);
                res = PQexec(conn.get(), sql.c_str());
                ck(conn, res);
                if (PQntuples(res) && atoi(PQgetvalue(res, 0, 0)) == 1){
                    PQclear(res);
                    return ukey;
                } else PQclear(res);
            } else PQclear(res);
        }
    } else PQclear(res);
    string key = tableName + "_id";
    sql = fmt::format("ALTER TABLE {} ADD COLUMN {} SERIAL UNIQUE;", tableName, key);
    res = PQexec(conn.get(), sql.c_str());
    ck(conn, res);
    PQclear(res);
    return key;
}

vector<string> PgManager::getAllKeys(const string& tableName){
    string sql = fmt::format("                              \
        SELECT DISTINCT(kcu.column_name)                    \
        FROM                                                \
            information_schema.table_constraints AS tc      \
            JOIN information_schema.key_column_usage AS kcu \
            ON tc.constraint_name = kcu.constraint_name     \
            AND tc.table_schema = kcu.table_schema          \
        WHERE                                               \
            tc.table_name = '{}'                            \
            AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE');\
        ", tableName);
	auto res = PQexec(conn.get(), sql.c_str());
	ck(conn, res);
    vector<string> keys (PQntuples(res));
    for (int i = 0; i < PQntuples(res); ++i) keys[i] = string(PQgetvalue(res, i, 0));
    PQclear(res);
    return keys;
}

SingleRow::~SingleRow(){
	while (res){
		PQclear(res);
		res = PQgetResult(pg->conn.get());
	}
}

SingleRow::SingleRow(const string& query){
	pg = make_unique<PgManager>();
	ck(pg->conn, PQsendQuery(pg->conn.get(), query.c_str()));
	ck(pg->conn, PQsetSingleRowMode(pg->conn.get()));
	res = nullptr;
}

bool SingleRow::fetchRow(){
	ck(pg->conn, PQconsumeInput(pg->conn.get()));
	if (res) PQclear(res);
	res = PQgetResult(pg->conn.get());
	if (res){
		ck(pg->conn, res);
		if (PQntuples(res)) return true;
	}
	return false;
}

long long SingleRow::getBigInt(const int& columnIndex){
	return atoll(PQgetvalue(res, 0, columnIndex));
}

double SingleRow::getNumeric(const int& columnIndex){
	return atof(PQgetvalue(res, 0, columnIndex));
}