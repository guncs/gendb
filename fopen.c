#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char** argv) {
	//argv[1] : a txt or csv file
	char buf[80];
	int r;
	double d;
	char str[100];
	char dest[200];
	char str1[25];
	char str2[25];
	printf("1: test fopen %s\n", argv[1]);
	FILE* fileptr = fopen(argv[1], "a+");
	printf("5: fopen success \n");
	
	for(int i =0; i < 10; i++){
		d = (double)rand();
		int dtoi = (int)d;
		const char *string2 = "%d";
		fprintf(fileptr, string2, dtoi);

		r = rand() % 10000;
		sprintf(str1, "%d", r);
		strcpy(dest, str1);
		const char *string = dest;
		//fputs(string, fileptr);
	}
	/*do {
		fgets(buf, 80, fileptr);
		if (feof(fileptr)){
			break; 
		}
	} while(1);*/
	fclose(fileptr);
	printf("9: fclose success\n"); //reaches here when only open and close
	return 0; 
}