#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <dlfcn.h>
#include <libpq-fe.h>
#include <string.h>

char *conninfo;
PGconn *conn;
PGresult *res;
PGresult *wres;
int nFields;
int cnt_fget = 0;
int k = 0;
int cnt_fput = 0;

static void
exit_nicely(PGconn *conn)
{
    PQfinish(conn);
    exit(1);
}

FILE* fopen(const char* path, const char* mode) {
  
    conninfo = "user=gunce password=gunce dbname=gunce";
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

    res = PQexec(conn, 
        "CREATE TABLE table3 (id serial primary key); CREATE INDEX ind3 ON table3 (id);");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        fprintf(stderr, "CREATE TABLE failed: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    PQclear(res);
    printf("Opening5 table created!! \n");


	FILE* (*real_fopen)(const char*, const char*) = dlsym(RTLD_NEXT, "fopen");
    return real_fopen(path, mode);
}

char* fgets(char *str, int n, FILE *f){

    // Start a transaction block
    res = PQexec(conn, "BEGIN");
    if (PQresultStatus(res) != PGRES_COMMAND_OK){
        fprintf(stderr, "BEGIN command failed: %s", PQerrorMessage(conn));
        exit_nicely(conn);
    }  
    // Clear memory
    PQclear(res);
    //printf("Opening5 %s\n", f);

    //Declare cursor
    res = PQexec(conn, "DECLARE mydata CURSOR FOR select * from tgunc");
    if (PQresultStatus(res) != PGRES_COMMAND_OK){
        fprintf(stderr, "DECLARE CURSOR failed: %s", PQerrorMessage(conn));
        exit_nicely(conn);
    }
    PQclear(res);
    //printf("Opening6 %s\n", f);
    
    //Fetch all the rows
    res = PQexec(conn, "FETCH ALL in mydata");
    if (PQresultStatus(res) != PGRES_TUPLES_OK){
        fprintf(stderr, "FETCH ALL failed: %s", PQerrorMessage(conn));
        exit_nicely(conn);
    }
    nFields = PQnfields(res);

    // print attribute names
    if(cnt_fget == 0){
        for (int i = 0; i < nFields; i++){
            printf("%-15s", PQfname(res, i));
        }
        printf("\n\n");
    }
    cnt_fget++;

    // print rows
    //for (i = firstrow; i < lastrow+1; i++){
    if(k != PQntuples(res)){
        for (int i = 0; i < nFields; i++){
            printf("%-15s", PQgetvalue(res, k, i));
        }
        printf("\n");
        k++;
    }

    char* (*real_fgets)(char*, int, FILE*) = dlsym(RTLD_NEXT, "fgets");
    return real_fgets(str, n, f);
}

int fputs(const char *str, FILE *f){
    
    if(cnt_fput == 0){
        char dest[200];
        strcpy(dest, "INSERT INTO tgunc VALUES (");
        strcat(dest, str);
        strcat(dest, ")");
        const char *string = dest;

        printf("Opening 8 : fputs testing:  %s\n\n\n\n", string);

        wres = PQexec(conn, string);
        if (PQresultStatus(wres) != PGRES_COMMAND_OK)
        {
            fprintf(stderr, "INSERT command failed: %s", PQerrorMessage(conn));
            PQclear(wres);
            exit_nicely(conn);
        }

        printf("Opening 9 : fputs testing2\n\n\n\n");
    }
    cnt_fput++;
    int (*real_fputs)(const char*, FILE*) = dlsym(RTLD_NEXT, "fputs");
    return real_fputs(str, f);
}


int fclose(FILE *f){

    PQclear(res);

    // close cursor
    res = PQexec(conn, "CLOSE mydata");
    PQclear(res);
    printf("Opening10 \n");
    
    // end transaction
    res = PQexec(conn, "END");
    PQclear(res);
    //printf("Opening7 %s\n", path);
    
    // close db connection 
    PQfinish(conn);
    printf("Opening11 \n");
    int (*real_fclose)(FILE*) = dlsym(RTLD_NEXT, "fclose");
    return real_fclose(f);
}