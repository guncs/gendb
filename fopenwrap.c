//#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <dlfcn.h>
#include <libpq-fe.h>
#include <string.h>
#include <boost/any.hpp>
#include <stdarg.h>
//#include <boost/format.hpp>
#include <iostream>
//using namespace std;

const char *conninfo;
PGconn *conn;
PGresult *res;
PGresult *wres;
int nFields;
int cnt_fget = 0;
int k = 0;
int cnt_fput = 0;
//boost::any anytype = 1;

static void
exit_nicely(PGconn *conn)
{
    PQfinish(conn);
    exit(1);
}

FILE* fopen(const char* path, const char* mode) {
    const char* tablestr = "table81";
    const char* s = ".txt";
    if(strstr(path, s) != NULL){
        conninfo = "user=gunce password=gunce dbname=gunce";
        printf("2: entered fopenwrapper %s\n", path);
        

        /* Make db connection */
        conn = PQconnectdb(conninfo);
        //printf("Opening3 %s\n", path);
        // Check if backend connection successful
        if (PQstatus(conn) != CONNECTION_OK)
        {
            fprintf(stderr, "Connection to database failed: %s", PQerrorMessage(conn));
            exit_nicely(conn);
        }
        printf("3: connection established %s\n", path);


        char dest[200];
        strcpy(dest, "CREATE TABLE ");
        strcat(dest, tablestr);
        strcat(dest, " (id serial primary key); CREATE INDEX ind81 ON ");
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
        printf("4: table created!! \n");
    }

    typedef FILE* (*ropen_ptr)(const char*, const char*);
    ropen_ptr real_fopen;
    real_fopen = (ropen_ptr)dlsym(RTLD_NEXT, "fopen");
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

int fprintf(FILE *f, const char *format, ... ){
    //printf("ENTERING FPRINTF WRAPPER \n");
    va_list arg;
    va_start(arg, format);
    char formatstr[10];
    const char* f1 = "%d";
    const char* f2 = "%f";
    const char* f3 = "%s";
    int type = 0;
    if(cnt_fput == 0){ //decimal float or integer
        char dest[200];
        strcpy(dest, "ALTER TABLE table81 ADD COLUMN ");

        if(strstr(format, f1) != NULL){
            type = 1;
            strcpy(formatstr, f1);
            char typestr[10];
            char fstr[10];
            strcpy(typestr, " integer");
            int bint = va_arg(arg, int);
            snprintf(fstr, 10, "%d", bint);
            strcat(dest, fstr);
            strcat(dest, typestr);
            strcat(dest, ")");
            const char *string = dest;
            wres = PQexec(conn, string);
            if (PQresultStatus(wres) != PGRES_COMMAND_OK)
            {
                fprintf(stderr, "ALTER command failed: %s", PQerrorMessage(conn));
                PQclear(wres);
                exit_nicely(conn);
            }


            char insertstr[200];
            strcpy(insertstr, "INSERT INTO table81 VALUES (");
            strcat(insertstr, fstr);
            strcat(insertstr, ");");
            const char *instr = insertstr;
            wres = PQexec(conn, instr);
            if (PQresultStatus(wres) != PGRES_COMMAND_OK)
            {
                fprintf(stderr, "INSERT command failed: %s", PQerrorMessage(conn));
                PQclear(wres);
                exit_nicely(conn);
            }

        }else if(strstr(format, f2) != NULL){
            type = 2;
            strcpy(formatstr, f2);
            char typestr[10];
            char fstr[10];
            strcpy(typestr, " real");
            double bint = va_arg(arg, double);
            snprintf(fstr, 10, "%f", bint);
            strcat(dest, fstr);
            strcat(dest, typestr);
            strcat(dest, ")");
            const char *string = dest;
            wres = PQexec(conn, string);
            if (PQresultStatus(wres) != PGRES_COMMAND_OK)
            {
                fprintf(stderr, "ALTER command failed: %s", PQerrorMessage(conn));
                PQclear(wres);
                exit_nicely(conn);
            }

            char insertstr[200];
            strcpy(insertstr, "INSERT INTO table81 VALUES (");
            strcat(insertstr, fstr);
            strcat(insertstr, ");");
            const char *instr = insertstr;
            wres = PQexec(conn, instr);
            if (PQresultStatus(wres) != PGRES_COMMAND_OK)
            {
                fprintf(stderr, "INSERT command failed: %s", PQerrorMessage(conn));
                PQclear(wres);
                exit_nicely(conn);
            }


        }else{//comparing f3 (string format)
            type = 3;
        }
    } else {
        char istr[100];

        if(type == 1){  
            int bint = va_arg(arg, int);
            snprintf(istr, 10, "%d", bint);
        }else if(type == 2){ 
            double bint = va_arg(arg, double);
            snprintf(istr, 10, "%f", bint);
        }else {}   

        char insertstr[200];
        strcpy(insertstr, "INSERT INTO table81 VALUES (");
        strcat(insertstr, istr);
        strcat(insertstr, ");");
        const char *instr = insertstr;
        wres = PQexec(conn, instr);
        if (PQresultStatus(wres) != PGRES_COMMAND_OK)
        {
            fprintf(stderr, "INSERT command failed: %s", PQerrorMessage(conn));
            PQclear(wres);
            exit_nicely(conn);
        }
        
    }

    va_end(arg);
    int i = 1;
    cnt_fput++;
    typedef int (*fprintf_ptr)(FILE*, const char*, ...);
    fprintf_ptr real_fprintf;
    real_fprintf = (fprintf_ptr)dlsym(RTLD_NEXT, "fprintf");
    //int (*real_fputs)(const char*, FILE*) = dlsym(RTLD_NEXT, "fputs");
    return real_fprintf(f, format, i);
}

int fputs(const char *string, FILE *f){
    if(cnt_fput == 0){
        /*char *longstr = (char*)malloc(100);
        char dest[200];
        strcpy(dest, "INSERT INTO table15 VALUES (");
        strcat(dest, str);
        strcat(dest, ")");
        const char *string = dest;
        printf("6 : fputs string:  %s\n\n\n\n", string);
        wres = PQexec(conn, string);
        if (PQresultStatus(wres) != PGRES_COMMAND_OK)
        {
            fprintf(stderr, "INSERT command failed: %s", PQerrorMessage(conn));
            PQclear(wres);
            exit_nicely(conn);
        }
        free(longstr);*/
    
    }
    cnt_fput++;
    typedef int (*fputs_ptr)(const char*, FILE*);
    fputs_ptr real_fputs;
    real_fputs = (fputs_ptr)dlsym(RTLD_NEXT, "fputs");
    //int (*real_fputs)(const char*, FILE*) = dlsym(RTLD_NEXT, "fputs");
    return real_fputs(string, f);
}


int fclose(FILE *f){
    //printf("Opening60 \n");
    //PQclear(res);
    //printf("Opening61 \n");
    // close cursor
    res = PQexec(conn, "CLOSE mydata");
    PQclear(res);
    //printf("Opening10 \n");
    
    // end transaction
    res = PQexec(conn, "END");
    PQclear(res);
    //printf("Opening7 %s\n", path);
    
    // close db connection 
    PQfinish(conn);
    printf("8: connection ended \n");

    typedef int (*fclose_ptr)(FILE*);
    fclose_ptr real_fclose;
    real_fclose = (fclose_ptr)dlsym(RTLD_NEXT, "fclose");
    //int (*real_fclose)(FILE*) = dlsym(RTLD_NEXT, "fclose");
    return real_fclose(f);
}