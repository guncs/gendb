#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <dlfcn.h>
#include <libpq-fe.h>

static void
exit_nicely(PGconn *conn)
{
    PQfinish(conn);
    exit(1);
}

FILE* fopen(const char* path, const char* mode) {
    char *conninfo;
    PGconn *conn;
    PGresult *res;
    conninfo = "user= password= dbname= hostaddr= port=";
    printf("Opening2 %s\n", path);
    
    /* Make db connection */
    conn = PQconnectdb(conninfo);
    printf("Opening3 %s\n", path);
    // Check if backend connection successful
    if (PQstatus(conn) != CONNECTION_OK)
    {
        fprintf(stderr, "Connection to database failed: %s",
                PQerrorMessage(conn));
        exit_nicely(conn);
    }
    printf("Opening4 %s\n", path);
	FILE* (*real_fopen)(const char*, const char*) = dlsym(RTLD_NEXT, "fopen");
    return real_fopen(path, mode);
}