#include <stdio.h>
#include <dirent.h>

int main(int argc, char **argv) {
	if (argc != 2) {
		const char *bin = argc > 0 ? argv[0] : "check_ino";
		fprintf(stderr, "usage: %s <dir>", bin);
		return 1;
	}

	DIR *d = opendir(argv[1]);
	if (d == NULL) {
		perror("opendir");
		return 1;
	}

	struct dirent *ent;
	while ((ent = readdir(d)) != NULL) {
		printf("%20s -> %llx\n", ent->d_name, ent->d_ino);
	}

	closedir(d);
	return 0;
}
