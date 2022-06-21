#include "DBFile.h"

#define N 3

char *catalog_path = "catalog";
// char *tpch_dir ="../tpch-dbgen/1G/";
char *tpch_dir = "/Users/siddhiw/Dropbox/Mac/Documents/Sem2/DBI/COP6726_DBI_Project/DBI_Project_COP6726/Project1/tpch-dbgen/";


char* values[N] = {"partsupp", "part", "supplier"};

int main(){
    char* name;
    for(int i=0; i<N; i++){
        name = values[i];
        char name_tbl[100];
        sprintf(name_tbl, "%s%s.tbl", tpch_dir, name);
        char name_bin[100];
        sprintf(name_bin, "%s.bin", name);
        Schema schema (catalog_path, name);

        DBFile dbfile;
        dbfile.Create(name_bin, heap, NULL);
        dbfile.Load(schema, name_tbl);
        dbfile.Close();
    }
}