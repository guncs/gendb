#include <stdio.h>
#include <stdlib.h>

int main(int argc, char** argv) {
	//argv[1] : a txt or csv file
	int x = 0;
	char buf[80];
	printf("Opening1 %s\n", argv[1]);
	FILE* fileptr = fopen(argv[1], "r");
	do {
		fgets(buf, 80, fileptr);
		if (feof(fileptr)){
			break; 
		}
		//printf("%s", buf);
	} while(1);

	fputs("a, b, c, d, e, f, g", fileptr);
	
	fclose(fileptr);
	return 0; 
}