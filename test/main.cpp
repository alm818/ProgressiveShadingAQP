#include "config.h"
#include "globals.h"
#include "systemutils.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <math.h>
#include <getopt.h>
#include <time.h>
#include <iostream>
#include <vector>
#include <filesystem>
#include <libpq-fe.h>
#include <algorithm>
#include <random>
#include <numeric>
#include "ts.h"
#include "dstree_file_loaders.h"
#include "dstree_index.h"
#include "dstree_node.h"
#include "dstree_file_buffer.h"
#include "dstree_file_buffer_manager.h"
#include "utils.h"
#include "udebug.hpp"
#include "udeclare.hpp"

#include <boost/property_tree/ini_parser.hpp>
#include <boost/algorithm/string/join.hpp>

#ifdef VALUES
#include <values.h>
#endif

namespace fs = std::filesystem;

using std::cout;
using std::endl;
using std::flush;
using std::vector;
using std::string;
using std::min;

unsigned int smallest_power_log(int b, unsigned int a) {
    // b > 1 is guaranteed
    if (a == 0) return 1;  // edge case: smallest power for zero can be handled based on context

    // Calculate n using logarithms
    double n = std::ceil(std::log(a) / std::log(b));
    
    return static_cast<unsigned int>(n);
}

void print_node(dstree_node * node, int depth){
    for (int i = 0; i < depth; i ++) printf("\t");
    printf("%s %d", node->filename, node->node_size);
    if (is_leaf(node)){
        printf(" [");
        for (int i = 0; i < node->node_size; ++i) printf("%d ", node->ts_index[i]);
        printf("]\n");
    } else{
        printf("\n");
        print_node(node->left_child, depth + 1);
        print_node(node->right_child, depth + 1);
    }
}

void explore_node(int df, dstree_node * node, int& count, bool& merge_allow, vector<int>& partition_ids){
    if (is_leaf(node)){
        int upper = count + (df - count % df) % df;
        if (upper < count + node->node_size || !merge_allow){
            count = upper;
            merge_allow = true;
        }
        count += node->node_size;
        for (int j = 0; j < node->node_size; j ++){
            partition_ids[node->ts_index[j]-1] = (count + df - 1) / df - 1;
        }
    } else{
        explore_node(df, node->left_child, count, merge_allow, partition_ids);
        explore_node(df, node->right_child, count, merge_allow, partition_ids);
        merge_allow = false;
    }
}

void hierarchicalize_table(string table_name, vector<string>& columns, int df){
	auto config = Config::getInstance();
    double memory = config->pt.get<double>("parameters.buffer_memory");
    string partition_column = config->pt.get<string>("pgmanager.partition_column");
    string sql;
    PGresult * res;
    
    // Phase 1 : partitioning
    // Find unique key
    PgManager pg = PgManager();
    string skey = pg.getUniqueSerialKey(table_name);

    // Get partitioning columns
    if (columns.size() == 0){
        auto columns_map = pg.getColumns(table_name);
        vector<string> keys = pg.getAllKeys(table_name);
        for (auto key : keys) columns_map[key] = Column::unsupported;
        if (columns_map.count(partition_column)) columns_map[partition_column] = Column::unsupported;
        vector<string> columns;
        for (auto p : columns_map) if (p.second == Column::numeric_type) columns.push_back(p.first);
        if (columns.size() == 0) return;
    }
    string partition_table = config->pt.get<string>("pgmanager.partition_table");
    sql = fmt::format("                                 \
        CREATE TABLE IF NOT EXISTS {} (                 \
        table_name TEXT NOT NULL,                       \
        columns TEXT[] NOT NULL,                        \
        df INTEGER NOT NULL                             \
    )", partition_table);
	res = PQexec(pg.conn.get(), sql.c_str());
	ck(pg.conn, res);
    PQclear(res);
    deb(columns);

    // Get normalization of each column, and COUNT(*)
    sql = fmt::format("SET work_mem = '{}MB'", (int) (memory / 2));
	res = PQexec(pg.conn.get(), sql.c_str());
	ck(pg.conn, res);
    PQclear(res);
    vector<string> sumabs (columns.size());
    for (int i = 0; i < columns.size(); ++i) sumabs[i] = fmt::format("SUM(ABS({}))/SQRT(COUNT(*))", columns[i]);
    sql = fmt::format("SELECT COUNT(*), {} FROM \"{}\"", boost::algorithm::join(sumabs, ", "), table_name);
	res = PQexec(pg.conn.get(), sql.c_str());
	ck(pg.conn, res);
    int table_size = atoi(PQgetvalue(res, 0, 0));
    vector<double> norms (columns.size());
    for (int i = 0; i < columns.size(); ++i) norms[i] = atof(PQgetvalue(res, 0, i+1));
    PQclear(res);

    // COPY implementation is slower!
	// auto config = Config::getInstance();
    // int max_tuple_count = static_cast<int>(min(config->pt.get<double>("parameters.buffer_memory") * 1e6 / (8 * (1 + columns.size())), 1e9));
    // sql = fmt::format("COPY (SELECT {} FROM {} ORDER BY {}_id) TO STDOUT WITH (FORMAT csv)", boost::algorithm::join(columns, ", "), table_name, table_name);
	// res = PQexec(pg.conn.get(), sql.c_str());
	// ck(pg.conn, res);
    // PQclear(res);
    // char *copyData = NULL;
    // int result;
    // int cnt = 0;
    // while ((result = PQgetCopyData(pg.conn.get(), &copyData, 0)) > 0) {
    //     char *line = copyData;
    //     char *token = strtok(line, ",");
    //     // while (token != NULL) {
    //     //     double value = strtod(token, NULL);
    //     //     ASSERT(!(value == 0 && (token == NULL || *token != '0')));
    //     //     token = strtok(NULL, ",");
    //     // }
    //     cnt ++;
    //     PQfreemem(copyData);
    //     cout << cnt << endl;
    // }
    // ck(pg.conn, result != -2);
    // deb(cnt, result);

    struct dstree_index * index = NULL;
    static char * index_path = "out/";
    static unsigned int time_series_size = columns.size();
    static unsigned int init_segments = columns.size();
    static unsigned int leaf_size = df;
    static double buffered_memory_size = memory / 4;
    boolean is_index_new = 1;
    struct dstree_index_settings * index_settings = dstree_index_settings_init(
        index_path,
        time_series_size,   
        init_segments,       
        leaf_size,          
        buffered_memory_size,
        is_index_new);
    ASSERT(index_settings != NULL);
    index = dstree_index_init(index_settings);
    index->first_node = dstree_root_node_init(index->settings);
    ASSERT(index != NULL);

    // Cursor implementation
    int max_tuple_count = static_cast<int>(min(memory / 4 * 1e6 / (8 * (1 + columns.size())), 1e9));
    res = PQexec(pg.conn.get(), "BEGIN");
    ck(pg.conn, res);
    PQclear(res);
    // sql = fmt::format("DECLARE mycursor CURSOR FOR SELECT {}, {} FROM {} ORDER BY lineitem_id LIMIT 1000", skey, boost::algorithm::join(columns, ", "), table_name);
    sql = fmt::format("DECLARE mycursor CURSOR FOR SELECT {}, {} FROM {}", skey, boost::algorithm::join(columns, ", "), table_name);
    res = PQexec(pg.conn.get(), sql.c_str());
    ck(pg.conn, res);
    PQclear(res);
    vector<int> ids; ids.reserve(table_size);
    std::mt19937 g(config->seed());
    while (1){
        res = PQexec(pg.conn.get(), fmt::format("FETCH {} FROM mycursor", max_tuple_count).c_str());
        ck(pg.conn, res);
        int row_count = PQntuples(res);
        if (row_count){
            vector<int> indices (row_count);
            std::iota(indices.begin(), indices.end(), 0);
            std::shuffle(indices.begin(), indices.end(), g);
            ts_type ts[time_series_size];
            for (int i = 0; i < row_count; i ++){
                ids.push_back(atoi(PQgetvalue(res, indices[i], 0)));
                for (int j = 0; j < columns.size(); j ++) ts[j] = atof(PQgetvalue(res, indices[i], j+1)) / norms[j];
                // cout << ids.back() << " ";
                // for (int j = 0; j < columns.size(); j ++) cout << ts[j] << " ";
                // cout << endl;
                dstree_index_insert(index, ts);
                if (i % 10000 == 0) cout << fmt::format("Row {}/{}", i, row_count) << endl;
            }
        } else break;
        PQclear(res);
    }
    res = PQexec(pg.conn.get(), "CLOSE mycursor");
    ck(pg.conn, res);
    PQclear(res);
    res = PQexec(pg.conn.get(), "COMMIT");
    ck(pg.conn, res);
    PQclear(res);

    // Phase 2 : hierarchicalize
    int total_count = index->first_node->node_size;
    vector<int> partition_ids (total_count);
    int count = 0;
    bool merge_allow = true;
    explore_node(df, index->first_node, count, merge_allow, partition_ids);

    pg.addColumn(table_name, partition_column, "INTEGER");
    int batch_size = 1000000;
    int batch_count = (total_count-1) / batch_size + 1;
    vector<string> sorted_partition_ids (total_count);
    for (int i = 0; i < total_count; ++i) sorted_partition_ids[ids[i]-1] = std::to_string(partition_ids[i]);
    for (int b = 0; b < batch_count; ++b){
        cout << fmt::format("Batch {}/{}", b+1, batch_count) << endl;
        int start = b*batch_size;
        int end = min(start+batch_size, total_count);
        vector<string> batch_ids (sorted_partition_ids.begin() + start, sorted_partition_ids.begin() + end);
        sql = fmt::format("                                     \
            WITH cte AS (                                       \        
                SELECT id+{} AS id, pid                         \
                FROM unnest(ARRAY[{}]) WITH ORDINALITY AS t(pid, id))\
            UPDATE {} SET {} = cte.pid FROM cte                 \
            WHERE {} = cte.id AND {} BETWEEN {} AND {}", start, boost::algorithm::join(batch_ids, ","), table_name, partition_column, skey, skey, start+1, end);
        res = PQexec(pg.conn.get(), sql.c_str());
        ck(pg.conn, res);
        PQclear(res);
    }

    string index_name = table_name + "_" + partition_column;
    sql = fmt::format("CREATE INDEX {} ON {} ({})", index_name, table_name, partition_column);
    res = PQexec(pg.conn.get(), sql.c_str());
    ck(pg.conn, res);
    PQclear(res);
    sql = fmt::format("CLUSTER {} USING {}", table_name, index_name);
    res = PQexec(pg.conn.get(), sql.c_str());
    ck(pg.conn, res);
    PQclear(res);
}

int main(int argc, char* argv[]) {
    ASSERT(argc == 3 || argc == 4);
    string table_name = string(argv[1]);
    int df = std::stoi(argv[2]);
    vector<string> columns;
    if (argc == 4){
        string columns_str = string(argv[3]);
        std::stringstream ss(columns_str);
        string word;
        while (ss >> word) columns.push_back(word);
    }
    hierarchicalize_table(table_name, columns, df);
    return 0;
}