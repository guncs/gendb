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

	printf("Opening1 %s\n", argv[1]);
	FILE* fileptr = fopen(argv[1], "a+");
	
	/*for(int i =0; i < 11; i++){
		r = rand() % 10000;
		d = (double)((rand() % 10000) / 7);
		for(int i = 0; i < 20; i++){
   			int x = rand() % 25; 
   			str[i] = (x + 97);
		} 
		sprintf(str1, "%d", r);
		sprintf(str2, "%f", d);
	   	strcpy(dest, str1);
	   	strcat(dest, " ");
	    strcat(dest, str);
	    strcat(dest, " ");
	    strcat(dest, str2);
    	const char *string = dest;
    	printf("what is string?: %s\n", string); 
		fputs(string, fileptr);
	}

	do {
		fgets(buf, 80, fileptr);
		if (feof(fileptr)){
			break; 
		}
	} while(1);*/

	fclose(fileptr);
	return 0; 
}

