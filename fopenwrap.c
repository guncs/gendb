#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <dlfcn.h>
#include <libpq-fe.h>
#include <string.h>
//#include <boost/any.hpp>
#include <stdarg.h>
#include <unistd.h>
//#include <boost/format.hpp>
//#include <iostream>

const char *conninfo;
PGconn *conn;
PGresult *res;
PGresult *wres;

int nFields;
int cnt_fget = 0;
int k = 0;
int cnt_fput = 0;
int cnt_fprintf = 0;
const char* tableno = "47";
int column_counter = 0; //in case two columns start with the same value, we need make make their names different


//names of the files to which data will be written
const char* src_path1 = "h_data.txt";
//const char* src_path2 = "x_data.txt";
const char* src_path2 = "laplace_solution.txt";
//const char* src_path2 = "test.txt";

char* src_path;
int is_src_code = 0;
int is_fopen_called = 0;


static void
exit_nicely(PGconn *conn)
{
    PQfinish(conn);
    exit(1);
}

FILE* fopen(const char* path, const char* mode) {

    
    if((strcmp(path, src_path1) == 0) || (strcmp(path, src_path2) == 0)){
    //const char* s = ".txt";
    //if(strstr(path, s) != NULL){
        is_fopen_called = 1;
        
        char *src_path;
        const char *s = ".";
        char *pathdup= strdup(path); 
        src_path = strtok(pathdup, s);

        printf("%s\n", src_path);
        //free(pathdup);
        //src_path = path;

        conninfo = "user=gunce password=gunce dbname=gunce";
        //conninfo = "user=postgres password=postgres dbname=postgres";
        printf("2: entered fopenwrapper %s\n", src_path);

        /* Make db connection */
        conn = PQconnectdb(conninfo);
        //printf("Opening3 %s\n", path);
        // Check if backend connection successful
        if (PQstatus(conn) != CONNECTION_OK){
            printf("Connection to database failed: %s", PQerrorMessage(conn));
            exit_nicely(conn);
        }
        printf("3: connection established %s\n", src_path);

        char dest[200];

        const char *s_path = src_path;
        printf("%s\n", s_path);

        strcpy(dest, "CREATE TABLE ");
        strcat(dest, s_path);
        strcat(dest, tableno);
        strcat(dest, " (id serial primary key); CREATE INDEX ind");
        strcat(dest, s_path);
        strcat(dest, tableno);
        strcat(dest, " ON ");
        strcat(dest, s_path);
        strcat(dest, tableno);
        strcat(dest, " (id);");
        const char *string = dest;

        //printf("1 mi:%s\n", dest);
        res = PQexec(conn, dest);
         //printf("2 mi:\n");
            //"CREATE TABLE table4 (id serial primary key); CREATE INDEX ind4 ON table4 (id);");
        if (PQresultStatus(res) != PGRES_COMMAND_OK){
            printf("CREATE TABLE failed: %s", PQerrorMessage(conn));
            PQclear(res);
            exit_nicely(conn);
        }
         //printf("3 mu:\n");
        PQclear(res);
         //printf("4 mu:\n");
        printf("4: table created!! \n");
    }

    //typedef FILE* (*ropen_ptr)(const char*, const char*);
    //ropen_ptr real_fopen;
    //real_fopen = (ropen_ptr)dlsym(RTLD_NEXT, "fopen");
	FILE* (*real_fopen)(const char*, const char*) = dlsym(RTLD_NEXT, "fopen");
    return real_fopen(path, mode);
}

char* fgets(char *str, int n, FILE *f){
    
    if(is_fopen_called == 1){
    
        printf("entered fgets \n");
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
    }
    //typedef char* (*rgets_ptr)(char*, int, FILE*);
    //rgets_ptr real_fgets;
    //real_fgets = (rgets_ptr)dlsym(RTLD_NEXT, "fgets");
    char* (*real_fgets)(char*, int, FILE*) = dlsym(RTLD_NEXT, "fgets");
    return real_fgets(str, n, f);
}

int formatarr[10];
int cnt_formatstrs = 0;
int cnt = 0;
//1 : integer
//2 : float
//3 : string

int fprintf(FILE *f, const char *format, ... ){
    
    if(is_fopen_called == 1){

        printf("entered fprintf\n");

        //find if the file called fprintf is one of the fopen functions in heatmpi.c
        char f_path[1024];
        char f_name[1024];
        int fd = fileno(f);
        sprintf(f_path, "/proc/self/fd/%d", fd);
        memset(f_name, 0, sizeof(f_name));
        readlink(f_path, f_name, sizeof(f_name) - 1);
        printf("%s\n", f_name);
        const char* filename = f_name;
        if(strstr(filename, src_path1) != NULL){
            src_path = "h_data";
            is_src_code = 1;
        } else if(strstr(filename, src_path2) != NULL){
            src_path = "laplace_solution";
            //src_path = "x_data";
            //src_path = "test";
            is_src_code = 1;
        } else {
            //is_src_code = 0;
        }

        if(is_src_code == 1){

            char formatstr[10];
            char insertstr[200];
            const char *instr;
            const char* f1 = "%d";
            const char* f2 = "%f";
            const char* f3 = "%s";
            const char* s_path = src_path;
            va_list arg;
            va_start(arg, format);
            //printf("11a\n");
           
            strcpy(insertstr, "INSERT INTO ");
            //printf("11b\n");
            printf("%s\n", s_path);
            strcat(insertstr, s_path);
            strcat(insertstr, tableno);
            //printf("11c\n");
            
            strcat(insertstr, " VALUES (DEFAULT, ");
            //printf("11d\n");
            if(cnt_fprintf == 0){ //first call, alter table schema and add 1st row
                char dest[200];
                char fstr[20];
                char typestr[10];
                const char *string;
                const char* s1 = " ";
                const char* s2 = "\n";
                char *token;
                char *formatdup = strdup(format); 
                printf("22\n");
                token = strtok(formatdup, s1);
                
                while(token != NULL){
                    
                    if(strcmp(token, f1) == 0){
                        formatarr[cnt] = 1;    
                    } 
                    else if(strcmp(token, f2) == 0){
                        formatarr[cnt] = 2;
                    } 
                    else if(strcmp(token, s2) == 0){
                        formatarr[cnt] = 2;
                        cnt_formatstrs--;
                    } 
                    else {
                        formatarr[cnt] = 3;
                    }
                      
                    cnt_formatstrs++;
                    cnt++;

                    token = strtok(NULL, s1);

                }
                //printf("33\n");
                strcpy(dest, "ALTER TABLE ");
                strcat(dest, s_path);
                strcat(dest, tableno);
                for(int i = 0; i < cnt_formatstrs; ){
                    
                    //printf("44\n");

                    int x = formatarr[i];
                    if(x == 1){//integer
                        strcpy(typestr, " integer");
                        int bint = va_arg(arg, int);
                        snprintf(fstr, 10, "%d", bint);
                    }else if(x == 2){ //float
                        strcpy(typestr, " real");
                        double bint = va_arg(arg, double);
                        snprintf(fstr, 10, "%f", bint);
                    }else{//string

                    }


                    printf("55\n");

                    strcat(dest, " ADD COLUMN _");


                    char *fstrtoken1;
                    char fstrtoken2[15];
                    char *fstrdup = strdup(fstr); 
                    fstrtoken1 = strtok(fstrdup, ".");
                    //fstrtoken2 = strtok(NULL, ".");
                    sprintf(fstrtoken2, "%d", column_counter);
                    strcat(dest, fstrtoken1);
                    strcat(dest, fstrtoken2);
                    strcat(dest, typestr);
                    strcat(insertstr, fstr);
                    i++;
                    column_counter++;
                    if(i != cnt_formatstrs){
                        strcat(dest, ", ");
                        strcat(insertstr, ", ");
                    }

                }

                printf("66\n");

                strcat(dest, ";");
                string = dest;        
                strcat(insertstr, ");");
                instr = insertstr;

                wres = PQexec(conn, string);
                if (PQresultStatus(wres) != PGRES_COMMAND_OK){
                    printf("ALTER command failed: %s", PQerrorMessage(conn));
                    PQclear(wres);
                    exit_nicely(conn);
                }

                wres = PQexec(conn, instr);
                if (PQresultStatus(wres) != PGRES_COMMAND_OK){
                    printf("INSERT command failed: %s", PQerrorMessage(conn));
                    PQclear(wres);
                    exit_nicely(conn);
                }
            }
            if(cnt_fprintf >= 1){ // add next rows
                
                char istr[100];
                for(int i = 0; i < cnt_formatstrs; ){
                    
                    int x = formatarr[i];
                    if(x == 1){//integer
                        int bint = va_arg(arg, int);
                        snprintf(istr, 10, "%d", bint);
                    }else if(x == 2){ //float
                        double bint = va_arg(arg, double);
                        snprintf(istr, 10, "%f", bint);
                    }else{//string

                    }
                    strcat(insertstr, istr);
                    i++;
                    if(i != cnt_formatstrs){
                        strcat(insertstr, ", ");
                    }
                }       
                strcat(insertstr, ");");
                instr = insertstr;
                printf("inserting next rowss:\n");
                wres = PQexec(conn, instr);
                if (PQresultStatus(wres) != PGRES_COMMAND_OK){
                    printf("INSERT command failed: %s", PQerrorMessage(conn));
                    PQclear(wres);
                    exit_nicely(conn);
                }   
            }
            va_end(arg);
            cnt_fprintf++;
        }
    }
    int i = 1;
    //typedef int (*fprintf_ptr)(FILE*, const char*, ...);
    //fprintf_ptr real_fprintf;
    //real_fprintf = (fprintf_ptr)dlsym(RTLD_NEXT, "fprintf");
    int (*real_fprintf)( FILE*, const char*, ...) = dlsym(RTLD_NEXT, "fprintf");
    return real_fprintf(f, format, i);
}

int fputs(const char *string, FILE *f){ //can be implemented in the future for other user programs
    
    if(is_fopen_called == 1){
        printf("entered fputs\n");
        if(cnt_fput == 0){
        }
        cnt_fput++;
    }
    //typedef int (*fputs_ptr)(const char*, FILE*);
    //fputs_ptr real_fputs;
    //real_fputs = (fputs_ptr)dlsym(RTLD_NEXT, "fputs");
    int (*real_fputs)(const char*, FILE*) = dlsym(RTLD_NEXT, "fputs");
    return real_fputs(string, f);
}

int fclose(FILE *f){

    if(is_fopen_called == 1){
        printf("entered fclose\n");    
        //PQclear(res);

        // close cursor
        res = PQexec(conn, "CLOSE mydata");
        PQclear(res);
        
        // end transaction
        res = PQexec(conn, "END");
        PQclear(res);
        
        // close db connection 
        PQfinish(conn);
        //printf("8: connection ended \n");
    }
    is_fopen_called = 0;
    is_src_code = 0;
    cnt_fprintf = 0;
    k = 0;
    column_counter = 0;
    cnt_formatstrs = 0;
    cnt = 0;
    //formatarr
    //typedef int (*fclose_ptr)(FILE*);
    //fclose_ptr real_fclose;
    //real_fclose = (fclose_ptr)dlsym(RTLD_NEXT, "fclose");
    int (*real_fclose)(FILE*) = dlsym(RTLD_NEXT, "fclose");
    return real_fclose(f);
}