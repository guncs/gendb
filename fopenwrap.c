#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <dlfcn.h>
#include <libpq-fe.h>

char *conninfo;
PGconn *conn;
PGresult *res;
int nFields;
int j = 0;
int k = 0;

static void
exit_nicely(PGconn *conn)
{
    PQfinish(conn);
    exit(1);
}

FILE* fopen(const char* path, const char* mode) {
  
    conninfo = "user=postgres password=postgres dbname=postgres";
    printf("Opening2 %s\n", path);
    

    /* Make db connection */
    conn = PQconnectdb(conninfo);
    printf("Opening3 %s\n", path);
    // Check if backend connection successful
    if (PQstatus(conn) != CONNECTION_OK)
    {
        fprintf(stderr, "Connection to database failed: %s", PQerrorMessage(conn));
        exit_nicely(conn);
    }
    printf("Opening4 %s\n", path);
    

    // Start a transaction block
    res = PQexec(conn, "BEGIN");
    if (PQresultStatus(res) != PGRES_COMMAND_OK){
        fprintf(stderr, "BEGIN command failed: %s", PQerrorMessage(conn));
        exit_nicely(conn);
    }  
    // Clear memory
    PQclear(res);
    printf("Opening5 %s\n", path);


    //Declare cursor
    res = PQexec(conn, "DECLARE mydata CURSOR FOR select * from pg_am");
    if (PQresultStatus(res) != PGRES_COMMAND_OK){
        fprintf(stderr, "DECLARE CURSOR failed: %s", PQerrorMessage(conn));
        exit_nicely(conn);
    }
    PQclear(res);
    printf("Opening6 %s\n", path);


    //Fetch all the rows
    res = PQexec(conn, "FETCH ALL in mydata");
    if (PQresultStatus(res) != PGRES_TUPLES_OK){
        fprintf(stderr, "FETCH ALL failed: %s", PQerrorMessage(conn));
        exit_nicely(conn);
    }
    nFields = PQnfields(res);
    printf("Opening7 %s\n", path);


    FILE* (*real_fopen)(const char*, const char*) = dlsym(RTLD_NEXT, "fopen");
    return real_fopen(path, mode);
}

char* fgets(char *str, int n, FILE *f){

    /*res = PQexec(conn, "FETCH NEXT FROM mydata");
    if (PQresultStatus(res) != PGRES_TUPLES_OK){
        fprintf(stderr, "FETCH NEXT failed: %s", PQerrorMessage(conn));
        exit_nicely(conn);
    }*/

    // print attribute names
    if(j == 0){
        for (int i = 0; i < nFields; i++){
            printf("%-15s", PQfname(res, i));
        }
        printf("\n\n");
    }
    j++;

    // print rows
    //for (i = firstrow; i < lastrow+1; i++){
    for (int i = 0; i < nFields; i++){
        printf("%-15s", PQgetvalue(res, k, i));
    }
    printf("\n");
    k++;

    char* (*real_fgets)(char*, int, FILE*) = dlsym(RTLD_NEXT, "fgets");
    return real_fgets(str, n, f);
}

int fclose(FILE *f){

    PQclear(res);

    // close cursor
    res = PQexec(conn, "CLOSE mydata");
    PQclear(res);

    // end transaction
    res = PQexec(conn, "END");
    PQclear(res);

    // close db connection 
    PQfinish(conn);

    int (*real_fclose)(FILE*) = dlsym(RTLD_NEXT, "fclose");
    return real_fclose(f);
}