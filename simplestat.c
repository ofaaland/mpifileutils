#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

char item_buf[128];
typedef char * (*printfn_t) (struct stat *);

struct field_spec {
	char fs_input_spec;
	char fs_desc[30];
	printfn_t printfn;
};

static char *do_file_size(struct stat *sb) {
	snprintf(item_buf, sizeof(item_buf), "%llu bytes", (long long) sb->st_size);
	return item_buf;
}

static char *do_blocks(struct stat *sb) {
	snprintf(item_buf, sizeof(item_buf), "%llu blocks", (long long) sb->st_blocks);
	return item_buf;
}

/* When adding supported specs, verify item_buf is large enough */
struct field_spec format_specs[] = {
{'s', "file size", do_file_size},
{'b', "blocks", do_blocks}
};

void do_print_fn(struct stat *sb, char spec)
{
	int printed=0;
	int i;

	for (i=0; i < (sizeof(format_specs)/sizeof(struct field_spec)); i++) {
		if (spec == format_specs[i].fs_input_spec) {
			printf("%s", format_specs[i].printfn(sb));
			printed = 1;
			break;
		}
	}

	if (! printed) {
		printf("UNSUPPORTED FORMAT SPEC '%c'", spec);
	}
}

void test_formats(struct stat *sb, char *format)
{
	int i;
	char *ptr;

	printf("test of each of the supported format specs:\n");
	for (i=0; i < (sizeof(format_specs)/sizeof(struct field_spec)); i++) {
		printf("   %% %c %s\n", format_specs[i].fs_input_spec,  format_specs[i].printfn(sb));
	}

	printf("\n");
	printf("format string: %s\n", format);

	printf("\n");
}

int process_format(struct stat *sb, char *format)
{
	int i;
	char *ptr;

	ptr = format;
	while (*ptr) {
		if (*ptr == '%') {
			ptr++;
			if (*ptr == '\0')
				break;
			else if (*ptr == '%')
				continue;
			else {
				do_print_fn(sb, *ptr);
				ptr++;
			}
		} else {
			putc(*ptr, stdout);
			ptr++;
		}
	}
	printf("\n");
}

void usage(char *cmd)
{
	int i;

	printf("usage: %s <filename>\n", cmd);
	printf("\n");
	printf("supported format string specifiers:\n");
	for (i=0; i < (sizeof(format_specs)/sizeof(struct field_spec)); i++) {
		printf("    %% %c %s\n", format_specs[i].fs_input_spec,  format_specs[i].fs_desc);
	}
}

int main(int argc, char *argv[])
{
	char *cmd = argv[0];
	char *fname;

	int i;
	char *format_strings[] = {
		"",
		"%%%%%%",
		"blocks is %b",
		"blocks is %b and inode is %i",
		"blocks is %b and size is %s",
		"foo and bar are baz",
		"size is %s",
		"size is %s and blocks is %b"
	};
	int format_string_count = sizeof(format_strings) / sizeof(char *);

	char *fmt_string;
	int rc;
	struct stat statbuf;

	if (argc < 2) {
		fprintf(stderr, "%s: missing argument(s)\n", cmd);
		usage(cmd);
		exit(1);
	}

	fname = argv[1];

	if (lstat(fname, &statbuf) == -1) {
		perror("lstat");
		exit(2);
	}

	for (i=0; i<format_string_count;i++) {
		fmt_string = format_strings[i];
		printf("Testing with format '%s'\n", fmt_string);
		(void)  process_format(&statbuf, fmt_string);
		printf("\n");
	}
}
