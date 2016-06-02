//#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <dlfcn.h>
#include <libpq-fe.h>
#include <string.h>
#include <boost/any.hpp>


const char *conninfo;
PGconn *conn;
PGresult *res;
PGresult *wres;
int nFields;
int cnt_fget = 0;
int k = 0;
int cnt_fput = 0;
boost::any anytype = 1;

static void
exit_nicely(PGconn *conn)
{
    PQfinish(conn);
    exit(1);
}

FILE* fopen(const char* path, const char* mode) {
    const char* tablestr = "table15";
    const char* s = ".txt";
    if(strstr(path, s) != NULL){
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


    char dest[200];
    strcpy(dest, "CREATE TABLE ");
    strcat(dest, tablestr);
    strcat(dest, " (id serial primary key); CREATE INDEX ind15 ON ");
    strcat(dest, tablestr);
    strcat(dest, " (id);");
    const char *string = dest;


    res = PQexec(conn, dest);
        //"CREATE TABLE table4 (id serial primary key); CREATE INDEX ind4 ON table4 (id);");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        fprintf(stderr, "CREATE TABLE failed: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    PQclear(res);
    printf("Opening5 table created!! \n");
    }

    typedef FILE* (*ropen_ptr)(const char*, const char*);
    printf("Opening55 \n");
    ropen_ptr real_fopen;
    printf("Opening56 \n");
    real_fopen = (ropen_ptr)dlsym(RTLD_NEXT, "fopen");
    printf("Opening57  \n");
    //FILE* (*real_fopen)(const char*, const char*) = dlsym(RTLD_NEXT, "fopen");
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

    typedef char* (*rgets_ptr)(char*, int, FILE*);
    rgets_ptr real_fgets;
    real_fgets = (rgets_ptr)dlsym(RTLD_NEXT, "fgets");
    //char* (*real_fgets)(char*, int, FILE*) = dlsym(RTLD_NEXT, "fgets");
    return real_fgets(str, n, f);
}

int fputs(const char *str, FILE *f){
    
    if(cnt_fput == 0){

        //char longstr[100] = str;
        char *longstr = (char*)malloc(100);
        char *token;

        token = strtok(longstr, " ");
        anytype = token;
        //?!?!?!?!?!?!??!?!?!?!?!
        //?!?!?!?!?!?!??!?!?!?
        while(token != NULL){
          //printf( " %s\n", token );
        
          token = strtok(NULL, longstr);
        }


        char dest[200];
        strcpy(dest, "INSERT INTO table15 VALUES (");
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
        free(longstr);
        printf("Opening 9 : fputs testing2\n\n\n\n");
    }
    cnt_fput++;

    typedef int (*rputs_ptr)(const char*, FILE*);
    rputs_ptr real_fputs;
    real_fputs = (rputs_ptr)dlsym(RTLD_NEXT, "fputs");
    //int (*real_fputs)(const char*, FILE*) = dlsym(RTLD_NEXT, "fputs");
    return real_fputs(str, f);
}


int fclose(FILE *f){
    //printf("Opening60 \n");
    //PQclear(res);
    //printf("Opening61 \n");
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

    typedef int (*rclose_ptr)(FILE*);
    rclose_ptr real_fclose;
    real_fclose = (rclose_ptr)dlsym(RTLD_NEXT, "fclose");
    //int (*real_fclose)(FILE*) = dlsym(RTLD_NEXT, "fclose");
    return real_fclose(f);
}