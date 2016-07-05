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


//Postgres variables
const char *conninfo;
PGconn *conn;
PGresult *res;
PGresult *wres;


int nFields; //no of fields of a given table

int tup_no = 0; // tuple to be printed in fgets

int cnt_fget = 0; //print attribute names only if fgets is called for the first time

int cnt_fput = 0; // when 0, it means fputs is called for the first time, so alter table schema

int cnt_fprintf = 0; // when 0, it means fprintf is called for the first time, so alter table schema

const char* tableno = "52"; //to change table name quickly for testing purposes

int column_counter = 0; //in case two columns start with the same value, we need make make their names different

char* src_path; //one of the data files which fopen calls

int is_fopen_called = 0; //to not execute wrapper functions before first fopen call

int is_src_code = 0; //to execute wrapper functions only if they are called with the src_path,
                     //so they will not override functions in the mpi library files

int formatarr[10]; //to store format strings given in fprintf in terms of integers
//1 : integer
//2 : float
//3 : string
//-1 : \n

int cnt_formatstrs = 0; //count number of format strings given in fprintf

int i_formatarr = 0; //index of formatarr


//file names to which data will be written
const char* src_path1 = "h_data.txt";
//const char* src_path2 = "x_data.txt";
//const char* src_path2 = "laplace_solution.txt";
const char* src_path2 = "test.txt";


static void exit_nicely(PGconn *conn){ //postgres exit helper function 
    PQfinish(conn);
    exit(1);
}

FILE* fopen(const char* path, const char* mode) {

    if((strcmp(path, src_path1) == 0) || (strcmp(path, src_path2) == 0)){ // execute wrapper if path is one of the desired files

        char dest[200]; //to store create table query
        is_fopen_called = 1; 

        const char *s = ".";
        char *pathdup= strdup(path); 
        src_path = strtok(pathdup, s); // postgres does not accept dot in the table name
        
        printf("%s\n", src_path); //for testing

        conninfo = "user=gunce password=gunce dbname=gunce";
        //conninfo = "user=postgres password=postgres dbname=postgres";

        printf("2: entered fopenwrapper %s\n", src_path); //for testing 


        /* Make db connection */
        conn = PQconnectdb(conninfo);

        // Check if backend connection successful
        if (PQstatus(conn) != CONNECTION_OK){
            printf("Connection to database failed: %s", PQerrorMessage(conn));
            exit_nicely(conn);
        }
        printf("3: connection established %s\n", src_path);//for testing 


        //form create table and index statements
        strcpy(dest, "CREATE TABLE ");
        strcat(dest, src_path);
        strcat(dest, tableno);
        strcat(dest, " (id serial primary key); CREATE INDEX ind");
        strcat(dest, tableno);
        strcat(dest, " ON ");
        strcat(dest, src_path);
        strcat(dest, tableno);
        strcat(dest, " (id);");
        const char *string = dest;

        printf("1 :%s\n", dest); //for testing
        
        //Execute create table and check if command successful
        res = PQexec(conn, dest);
        if (PQresultStatus(res) != PGRES_COMMAND_OK){
            printf("CREATE TABLE failed: %s", PQerrorMessage(conn));
            PQclear(res);
            exit_nicely(conn);
        }
        PQclear(res);
        printf("4: table created!! \n");
    }

    //commented out for c++ version
    //typedef FILE* (*ropen_ptr)(const char*, const char*);
    //ropen_ptr real_fopen;
    //real_fopen = (ropen_ptr)dlsym(RTLD_NEXT, "fopen");


	FILE* (*real_fopen)(const char*, const char*) = dlsym(RTLD_NEXT, "fopen");
    return real_fopen(path, mode);
}

char* fgets(char *str, int n, FILE *f){
    
    if(is_fopen_called == 1){ //no need to check the name of file f if fopen is not called yet
    
        printf("entered fgets \n"); //for testing

        // Start a transaction block
        res = PQexec(conn, "BEGIN");
        if (PQresultStatus(res) != PGRES_COMMAND_OK){
            fprintf(stderr, "BEGIN command failed: %s", PQerrorMessage(conn));
            exit_nicely(conn);
        }  
        
        PQclear(res); //clear memory

        printf("Opening5 \n"); //for testing


        //Declare cursor
        res = PQexec(conn, "DECLARE mydata CURSOR FOR select * from tgunc");
        if (PQresultStatus(res) != PGRES_COMMAND_OK){
            fprintf(stderr, "DECLARE CURSOR failed: %s", PQerrorMessage(conn));
            exit_nicely(conn);
        }
        PQclear(res); // clear memory

        printf("Opening6 \n"); //for testing
        
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

        //print rows
        if(tup_no != PQntuples(res)){
            for (int i = 0; i < nFields; i++){
                printf("%-15s", PQgetvalue(res, tup_no, i));
            }
            printf("\n");
            tup_no++;
        }

    }

    //commented out for c++ version
    //typedef char* (*rgets_ptr)(char*, int, FILE*);
    //rgets_ptr real_fgets;
    //real_fgets = (rgets_ptr)dlsym(RTLD_NEXT, "fgets");
    
    char* (*real_fgets)(char*, int, FILE*) = dlsym(RTLD_NEXT, "fgets");
    return real_fgets(str, n, f);
}

int fprintf(FILE *f, const char *format, ... ){
    
    if(is_fopen_called == 1){//no need to check the name of file f if fopen is not called yet

        printf("entered fprintf\n"); //for testing

        //find the filename of FILE* f, and check if it is one of the given two file names(above)
        //to which data will be written
        char f_path[1024];
        char f_name[1024];
        int fd = fileno(f); //get the file descriptor
        sprintf(f_path, "/proc/self/fd/%d", fd);
        memset(f_name, 0, sizeof(f_name));
        readlink(f_path, f_name, sizeof(f_name) - 1); //find the whole path name of the file descriptor
        
        printf("%s\n", f_name); //for testing

        const char* filename = f_name; //cast as constant char to put into strstr
        
        //test if the whole path name includes one of the two given file names 
        if(strstr(filename, src_path1) != NULL){
            
            //take the name of the file until first dot, since postgres does not accept dot in table name
            const char *s = ".";
            char *pathdup= strdup(src_path1); 
            src_path = strtok(pathdup, s); 

            is_src_code = 1;

        } else if(strstr(filename, src_path2) != NULL){
            
            //take the name of the file until first dot, since postgres does not accept dot in table name
            const char *s = ".";
            char *pathdup= strdup(src_path2); 
            src_path = strtok(pathdup, s);

            is_src_code = 1;
        
        } else {
            //is_src_code = 0;
        }

        if(is_src_code == 1){ //execute this part (alter table and insert statements) 
                              //only if fprintf calls one of the two data files

            char insertstr[200]; //store insert statement

            //possible strings that the given format string may include
            const char* f1 = "%d";
            const char* f2 = "%f";
            const char* f3 = "%s";
            const char* f4 = "\n";

            //store the input arguments of fprintf given as the 3rd input named "..."
            va_list arg;
            va_start(arg, format);

            //start forming the insert statement
            strcpy(insertstr, "INSERT INTO ");
            strcat(insertstr, src_path);
            strcat(insertstr, tableno);
            strcat(insertstr, " VALUES (DEFAULT, ");


            if(cnt_fprintf == 0){ //first call to fprintf, alter table schema and add 1st row
                
                char dest[200]; //store the alter table statement
                char fstr[20]; //store a value of a field of the 1st row, given in the va_list named arg
                char typestr[10]; //store the type of the value fstr to put in alter table statement as column type
                

                //assuming format string is given in the form similar to "%f %d \n",
                //take each string seperated by space and put into the format array named formatarr
                const char* s = " ";
                char *token;
                char *formatdup = strdup(format); 
                token = strtok(formatdup, s);
                
                while(token != NULL){
                    
                    if(strcmp(token, f1) == 0){ //integer type
                        formatarr[i_formatarr] = 1;    
                    } 
                    else if(strcmp(token, f2) == 0){ //float type
                        formatarr[i_formatarr] = 2;
                    } 
                    else if(strcmp(token, f4) == 0){ //"\n"
                        formatarr[i_formatarr] = 2;
                        cnt_formatstrs--;
                    } 
                    else { //string type
                        formatarr[i_formatarr] = 3;
                    }
                      
                    cnt_formatstrs++; //now we know how many format string are there(i.e. excluding \n)
                    i_formatarr++;

                    token = strtok(NULL, s);

                }

                //start forming alter table statement
                strcpy(dest, "ALTER TABLE ");
                strcat(dest, src_path);
                strcat(dest, tableno);
                
                //add column names and types to the alter table statement
                //column name will be the values in the first row 
                for(int i = 0; i < cnt_formatstrs; ){    

                    int x = formatarr[i]; //get column type from the formatarr
                    if(x == 1){//integer
                        strcpy(typestr, " integer");
                        int bint = va_arg(arg, int); //get value
                        snprintf(fstr, 10, "%d", bint); //put into fstr as column name
                    }else if(x == 2){ //float
                        strcpy(typestr, " real");
                        double bint = va_arg(arg, double); //get value
                        snprintf(fstr, 10, "%f", bint); //put into fstr as column name
                    }else{//string

                    }

                    strcat(dest, " ADD COLUMN _");
                    
                    //column names do not accept dots either, take fstr until dot if it stored float
                    char *fstrtoken1;
                    char *fstrdup = strdup(fstr); 
                    fstrtoken1 = strtok(fstrdup, ".");
                    
                    //in case there are several columns that start with the same value, 
                    //we add the column_counter to make column names different
                    char fstrtoken2[15];
                    sprintf(fstrtoken2, "%d", column_counter);
                    
                    //store column name and type to alter table statement
                    strcat(dest, fstrtoken1);
                    strcat(dest, fstrtoken2);
                    strcat(dest, typestr);

                    //store value to insert statement
                    strcat(insertstr, fstr);
                    
                    i++;
                    column_counter++;
                    
                    //add "," for a new add column statement
                    if(i != cnt_formatstrs){
                        strcat(dest, ", ");
                        strcat(insertstr, ", ");
                    }

                }

                //end of statement 
                strcat(dest, ";");  
                const char *alstr = dest; //cast dest as constant char to put into PQexec
                
                //end of statement 
                strcat(insertstr, ");");
                const char *instr = insertstr; //cast insertstr as constant char to put into PQexec

                //execute and check if the alter table command works
                wres = PQexec(conn, alstr);
                if (PQresultStatus(wres) != PGRES_COMMAND_OK){
                    printf("ALTER command failed: %s", PQerrorMessage(conn));
                    PQclear(wres);
                    exit_nicely(conn);
                }

                //execute and check if the insert command works
                wres = PQexec(conn, instr);
                if (PQresultStatus(wres) != PGRES_COMMAND_OK){
                    printf("INSERT command failed: %s", PQerrorMessage(conn));
                    PQclear(wres);
                    exit_nicely(conn);
                }
            }

            if(cnt_fprintf >= 1){ // add next rows
                
                char istr[100]; //store values to put into insert statement

                for(int i = 0; i < cnt_formatstrs; ){
                    
                    int x = formatarr[i];
                    if(x == 1){//integer
                        int bint = va_arg(arg, int); //get value from arg list
                        snprintf(istr, 10, "%d", bint); 
                    }else if(x == 2){ //float
                        double bint = va_arg(arg, double); //get value from arg list
                        snprintf(istr, 10, "%f", bint); 
                    }else{//string

                    }
                    strcat(insertstr, istr);
                    i++;

                    //add "," if there are more than one columns to insert
                    if(i != cnt_formatstrs){
                        strcat(insertstr, ", ");
                    }
                }     

                //end of insert statement
                strcat(insertstr, ");");
                const char *instr = insertstr; //cast insertstr as constant char to put into PQexec
                
                printf("inserting next rowss:\n"); //for testing
                
                //execute and test if the insert statement is successful
                wres = PQexec(conn, instr);
                if (PQresultStatus(wres) != PGRES_COMMAND_OK){
                    printf("INSERT command failed: %s", PQerrorMessage(conn));
                    PQclear(wres);
                    exit_nicely(conn);
                }   
            }

            va_end(arg); //end of argument list

            cnt_fprintf++; //to execute alter table statement only in the first fprintf call 
        }
    }

    int i = 1; //real_fprintf below still asks for a value rather than "..."

    //commented out for c++ version
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

    //commented out for c++ version
    //typedef int (*fputs_ptr)(const char*, FILE*);
    //fputs_ptr real_fputs;
    //real_fputs = (fputs_ptr)dlsym(RTLD_NEXT, "fputs");
    
    int (*real_fputs)(const char*, FILE*) = dlsym(RTLD_NEXT, "fputs");
    return real_fputs(string, f);
}

int fclose(FILE *f){

    if(is_fopen_called == 1){ //no need to check the name of file f if fopen is not called yet

        printf("entered fclose\n"); //for testing 
        //PQclear(res);

        // close cursor
        res = PQexec(conn, "CLOSE mydata");
        PQclear(res);
        
        // end transaction
        res = PQexec(conn, "END");
        PQclear(res);
        
        // close db connection 
        PQfinish(conn);
        printf("8: connection ended \n"); //for testing
    }
    
    //zero all the public variables
    is_fopen_called = 0;
    is_src_code = 0;
    cnt_fprintf = 0;
    column_counter = 0;
    cnt_formatstrs = 0;
    i_formatarr = 0;
    tup_no = 0;

    
    //commented out for c++ version
    //typedef int (*fclose_ptr)(FILE*);
    //fclose_ptr real_fclose;
    //real_fclose = (fclose_ptr)dlsym(RTLD_NEXT, "fclose");
    
    int (*real_fclose)(FILE*) = dlsym(RTLD_NEXT, "fclose");
    return real_fclose(f);
}