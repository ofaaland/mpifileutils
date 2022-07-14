#include <stdio.h>
#include <unistd.h>
#include <getopt.h>
#include <stdlib.h>
#include <mpi.h>
#include <libcircle.h>
#include <linux/limits.h>
#include <libgen.h>
#include <errno.h>
#include <dtcmp.h>
#include <inttypes.h>
#define _XOPEN_SOURCE 600
#include <fcntl.h>
#include <string.h>
/* for bool type, true/false macros */
#include <stdbool.h>
#include <assert.h>

#include "mfu.h"
#include "strmap.h"
#include "list.h"

/* for daos */
#ifdef DAOS_SUPPORT
#include "mfu_daos.h"
#endif

/* Print a usage message */
static void print_usage(void)
{
    printf("\n");
    printf("Usage: dcmp [options] source target\n");
    printf("\n");
#ifdef DAOS_SUPPORT
    printf("DAOS paths can be specified as:\n");
    printf("       daos://<pool>/<cont>[/<path>] | <UNS path>\n");
#endif
    printf("Options:\n");
    printf("  -o, --output <EXPR:FILE>  - write list of entries matching EXPR to FILE\n");
    printf("  -t, --text                - change output option to write in text format\n");
    printf("  -b, --base                - enable base checks and normal output with --output\n");
    printf("      --bufsize <SIZE>      - IO buffer size in bytes (default " MFU_BUFFER_SIZE_STR ")\n");
    printf("      --chunksize <SIZE>    - minimum work size per task in bytes (default " MFU_CHUNK_SIZE_STR ")\n");
#ifdef DAOS_SUPPORT
    printf("      --daos-api            - DAOS API in {DFS, DAOS} (default uses DFS for POSIX containers)\n");
#endif
    printf("  -s, --direct              - open files with O_DIRECT\n");
    printf("      --progress <N>        - print progress every N seconds\n");
    printf("  -v, --verbose             - verbose output\n");
    printf("  -q, --quiet               - quiet output\n");
    printf("  -l, --lite                - only compares file modification time and size\n");
    //printf("  -d, --debug               - run in debug mode\n");
    printf("  -h, --help                - print usage\n");
    printf("\n");
    printf("EXPR consists of one or more FIELD=STATE conditions, separated with '@' for AND or ',' for OR.\n");
    printf("AND operators bind with higher precedence than OR.\n");
    printf("\n");
    printf("Fields: EXIST,TYPE,SIZE,UID,GID,ATIME,MTIME,CTIME,PERM,ACL,CONTENT\n");
    printf("States: DIFFER,COMMON\n");
    printf("Additional States for EXIST: ONLY_SRC,ONLY_DEST\n");
    printf("\n");
    printf("Example expressions:\n");
    printf("- Entry exists in both source and target and type differs between the two\n");
    printf("  EXIST=COMMON@TYPE=DIFFER\n");
    printf("\n");
    printf("- Entry exists only in source, or types differ, or permissions differ, or mtimes differ\n");
    printf("  EXIST=ONLY_SRC,TYPE=DIFFER,PERM=DIFFER,MTIME=DIFFER\n");
    printf("\n");
    printf("By default, dcmp checks the following expressions and prints results to stdout:\n");
    printf("  EXIST=COMMON\n");
    printf("  EXIST=DIFFER\n");
    printf("  EXIST=COMMON@TYPE=COMMON\n");
    printf("  EXIST=COMMON@TYPE=DIFFER\n");
    printf("  EXIST=COMMON@CONTENT=COMMON\n");
    printf("  EXIST=COMMON@CONTENT=DIFFER\n");
    printf("For more information see https://mpifileutils.readthedocs.io.\n");
    printf("\n");
    fflush(stdout);
}

struct dcmp_options {
    int verbose;
    int quiet;
    int lite;
    int format;                    /* output data format, 0 for text, 1 for raw */
    int base;                      /* whether to do base check */
    int debug;                     /* check result after get result */
};

struct dcmp_options options = {
    .verbose      = 0,
    .quiet        = 0,
    .lite         = 0,
    .format       = 1,
    .base         = 0,
    .debug        = 0,
};

struct mfu_cmp_options dcmp_outputs = {
    .outputs      = LIST_HEAD_INIT(dcmp_outputs.outputs),
};

struct mfu_need_compare dcmp_need_compare = {0};

/* From tail to head */
const char *dcmp_default_outputs[] = {
    "EXIST=COMMON@CONTENT=DIFFER",
    "EXIST=COMMON@CONTENT=COMMON",
    "EXIST=COMMON@TYPE=DIFFER",
    "EXIST=COMMON@TYPE=COMMON",
    "EXIST=DIFFER",
    "EXIST=COMMON",
    NULL,
};

/* use Allreduce to get the total number of bytes read if
 * data was compared */
static uint64_t get_total_bytes_read(mfu_flist src_compare_list) {

    /* get counter for flist id & byte_count */
    uint64_t idx;
    uint64_t byte_count = 0;

    /* get size of flist */
    uint64_t size = mfu_flist_size(src_compare_list);

    /* count up the number of bytes in src list
     * multiply by two in order to include number
     * of bytes read in dst list as well */
    for (idx = 0; idx < size; idx++) {
        byte_count += mfu_flist_file_get_size(src_compare_list, idx) * 2;
    }

    /* buffer for total byte count for Allreduce */
    uint64_t total_bytes_read;

    /* get total number of bytes across all processes */
    MPI_Allreduce(&byte_count, &total_bytes_read, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* return the toal number of bytes */
    return total_bytes_read;
}

/* variable to hold total bytes to be compared for computing progress and estimated time remaining */
static uint64_t compare_total_count;

static void compare_progress_fn(const uint64_t* vals, int count, int complete, int ranks, double secs)
{
    uint64_t bytes = vals[0];

    /* compute average delete rate */
    double byte_rate  = 0.0;
    if (secs > 0) {
        byte_rate  = (double)bytes / secs;
    }

    /* compute percentage of items removed */
    double percent = 0.0;
    if (compare_total_count > 0) {
        percent = 100.0 * (double)bytes / (double)compare_total_count;
    }

    /* estimate seconds remaining */
    double secs_remaining = -1.0;
    if (byte_rate > 0.0) {
        secs_remaining = (double)(compare_total_count - bytes) / byte_rate;
    }

    /* convert bytes to units */
    double agg_size_tmp;
    const char* agg_size_units;
    mfu_format_bytes(bytes, &agg_size_tmp, &agg_size_units);

    /* convert bandwidth to units */
    double agg_rate_tmp;
    const char* agg_rate_units;
    mfu_format_bw(byte_rate, &agg_rate_tmp, &agg_rate_units);

    if (complete < ranks) {
        MFU_LOG(MFU_LOG_INFO, "Compared %.3lf %s (%.0f%%) in %.3lf secs (%.3lf %s) %.0f secs left ...",
            agg_size_tmp, agg_size_units, percent, secs, agg_rate_tmp, agg_rate_units, secs_remaining);
    } else {
        MFU_LOG(MFU_LOG_INFO, "Compared %.3lf %s (%.0f%%) in %.3lf secs (%.3lf %s) done",
            agg_size_tmp, agg_size_units, percent, secs, agg_rate_tmp, agg_rate_units);
    }
}

/* given a list of source/destination files to compare, spread file
 * sections to processes to compare in parallel, fill
 * in comparison results in source and dest string maps */
static int dcmp_strmap_compare_data(
    mfu_flist src_compare_list,
    strmap* src_map,
    mfu_flist dst_compare_list,
    strmap* dst_map,
    size_t strlen_prefix,
    mfu_copy_opts_t* copy_opts,
    mfu_file_t* mfu_src_file,
    mfu_file_t* mfu_dst_file)
{
    /* assume we'll succeed */
    int rc = 0;

    /* let user know what we're doing */
    if (mfu_debug_level >= MFU_LOG_VERBOSE && mfu_rank == 0) {
         MFU_LOG(MFU_LOG_INFO, "Comparing file contents");
    }

    /* first, count number of bytes in our part of the source list,
     * and double it to count for bytes to read in destination */
    uint64_t idx;
    uint64_t size = mfu_flist_size(src_compare_list);
    uint64_t bytes = 0;
    for (idx = 0; idx < size; idx++) {
        /* count regular files and symlinks */
        mfu_filetype type = mfu_flist_file_get_type(src_compare_list, idx);
        if (type == MFU_TYPE_FILE) {
            bytes += mfu_flist_file_get_size(src_compare_list, idx);
        }
    }
    bytes *= 2;

    /* get total for print percent progress while creating */
    compare_total_count = 0;
    MPI_Allreduce(&bytes, &compare_total_count, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* get chunk size for copying files */
    uint64_t chunk_size = copy_opts->chunk_size;

    /* get the linked list of file chunks for the src and dest */
    mfu_file_chunk* src_head = mfu_file_chunk_list_alloc(src_compare_list, chunk_size);
    mfu_file_chunk* dst_head = mfu_file_chunk_list_alloc(dst_compare_list, chunk_size);

    /* get a count of how many items are the chunk list */
    uint64_t list_count = mfu_file_chunk_list_size(src_head);

    /* allocate a flag for each element in chunk list,
     * will store 0 to mean data of this chunk is the same 1 if different
     * to be used as input to logical OR to determine state of entire file */
    int* vals = (int*) MFU_MALLOC(list_count * sizeof(int));

    /* start progress messages when comparing data */
    mfu_progress* prg = mfu_progress_start(mfu_progress_timeout, 2, MPI_COMM_WORLD, compare_progress_fn);

    /* compare bytes for each file section and set flag based on what we find */
    uint64_t i = 0;
    const mfu_file_chunk* src_p = src_head;
    const mfu_file_chunk* dst_p = dst_head;
    uint64_t bytes_read    = 0;
    uint64_t bytes_written = 0;
    for (i = 0; i < list_count; i++) {
        /* get offset into file that we should compare (bytes) */
        off_t offset = (off_t)src_p->offset;

        /* get length of section that we should compare (bytes) */
        off_t length = (off_t)src_p->length;

        /* get size of file that we should compare (bytes) */
        off_t filesize = (off_t)src_p->file_size;

        /* compare the contents of the files */
        int overwrite = 0;
        int compare_rc = mfu_compare_contents(src_p->name, dst_p->name, offset, length, filesize,
                overwrite, copy_opts, &bytes_read, &bytes_written, prg, mfu_src_file, mfu_dst_file);
        if (compare_rc == -1) {
            /* we hit an error while reading */
            rc = -1;
            MFU_LOG(MFU_LOG_ERR,
              "Failed to open, lseek, or read %s and/or %s. Assuming contents are different.",
                 src_p->name, dst_p->name);

            /* consider files to be different,
             * they could be the same, but we'll draw attention to them this way */
            compare_rc = 1;
        }

        /* record results of comparison */
        vals[i] = compare_rc;

        /* update pointers for src and dest in linked list */
        src_p = src_p->next;
        dst_p = dst_p->next;
    }

    /* finalize progress messages */
    uint64_t count_bytes[2];
    count_bytes[0] = bytes_read;
    count_bytes[1] = bytes_written;
    mfu_progress_complete(count_bytes, &prg);

    /* allocate a flag for each item in our file list */
    int* results = (int*) MFU_MALLOC(size * sizeof(int));

    /* execute logical OR over chunks for each file */
    mfu_file_chunk_list_lor(src_compare_list, src_head, vals, results);

    /* unpack contents of recv buffer & store results in strmap */
    for (i = 0; i < size; i++) {
        /* lookup name of file based on id to send to strmap updata call */
        const char* name = mfu_flist_file_get_name(src_compare_list, i);

        /* ignore prefix portion of path to use as key */
        name += strlen_prefix;

        /* get comparison results for this item */
        int flag = results[i];

        /* set flag in strmap to record status of file */
        if (flag != 0) {
            /* update to say contents of the files were found to be different */
            dcmp_strmap_item_update(src_map, name, DCMPF_CONTENT, DCMPS_DIFFER);
            dcmp_strmap_item_update(dst_map, name, DCMPF_CONTENT, DCMPS_DIFFER);

        } else {
            /* update to say contents of the files were found to be the same */
            dcmp_strmap_item_update(src_map, name, DCMPF_CONTENT, DCMPS_COMMON);
            dcmp_strmap_item_update(dst_map, name, DCMPF_CONTENT, DCMPS_COMMON);
        }
    }

    /* free memory */
    mfu_free(&results);
    mfu_free(&vals);
    mfu_file_chunk_list_free(&src_head);
    mfu_file_chunk_list_free(&dst_head);

    /* determine whether any process hit an error,
     * input is either 0 or -1, so MIN will return -1 if any */
    int all_rc;
    MPI_Allreduce(&rc, &all_rc, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
    rc = all_rc;

    return rc;
}

static void time_strmap_compare(mfu_flist src_list, double start_compare,
                                double end_compare, time_t *time_started,
                                time_t *time_ended, uint64_t total_bytes_read) {

    /* if the verbose option is set print the timing data
        report compare count, time, and rate */
    if (mfu_debug_level >= MFU_LOG_VERBOSE && mfu_rank == 0) {
       /* find out how many files were compared */
       uint64_t all_count = mfu_flist_global_size(src_list);

       /* get the amount of time the compare function took */
       double time_diff = end_compare - start_compare;

       /* calculate byte and file rate */
       double file_rate = 0.0;
       double byte_rate = 0.0;
       if (time_diff > 0.0) {
           file_rate = ((double)all_count) / time_diff;
           byte_rate = ((double)total_bytes_read) / time_diff;
       }

       /* convert uint64 to strings for printing to user */
       char starttime_str[256];
       char endtime_str[256];

       struct tm* localstart = localtime(time_started);
       struct tm cp_localstart = *localstart;
       struct tm* localend = localtime(time_ended);
       struct tm cp_localend = *localend;

       strftime(starttime_str, 256, "%b-%d-%Y, %H:%M:%S", &cp_localstart);
       strftime(endtime_str, 256, "%b-%d-%Y, %H:%M:%S", &cp_localend);

       /* convert size to units */
       double size_tmp;
       const char* size_units;
       mfu_format_bytes(total_bytes_read, &size_tmp, &size_units);

       /* convert bandwidth to units */
       double total_bytes_tmp;
       const char* rate_units;
       mfu_format_bw(byte_rate, &total_bytes_tmp, &rate_units);

       MFU_LOG(MFU_LOG_INFO, "Started   : %s", starttime_str);
       MFU_LOG(MFU_LOG_INFO, "Completed : %s", endtime_str);
       MFU_LOG(MFU_LOG_INFO, "Seconds   : %.3lf", time_diff);
       MFU_LOG(MFU_LOG_INFO, "Items     : %" PRId64, all_count);
       MFU_LOG(MFU_LOG_INFO, "Item Rate : %lu items in %f seconds (%f items/sec)",
            all_count, time_diff, file_rate);
       MFU_LOG(MFU_LOG_INFO, "Bytes read: %.3lf %s (%" PRId64 " bytes)",
            size_tmp, size_units, total_bytes_read);
       MFU_LOG(MFU_LOG_INFO, "Byte Rate : %.3lf %s (%.3" PRId64 " bytes in %.3lf seconds)",
            total_bytes_tmp, rate_units, total_bytes_read, time_diff);
    }
}

int main(int argc, char **argv)
{
    int rc = 0;

    /* initialize MPI and mfu libraries */
    MPI_Init(&argc, &argv);
    mfu_init();

    /* get our rank and number of ranks */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* pointer to mfu_file src and dest objects */
    mfu_file_t* mfu_src_file = mfu_file_new();
    mfu_file_t* mfu_dst_file = mfu_file_new();

    /* allocate structure to define walk options */
    mfu_walk_opts_t* walk_opts = mfu_walk_opts_new();

    /* allocate structure to define copy (comparision) options */
    mfu_copy_opts_t* copy_opts = mfu_copy_opts_new();

    /* TODO: allow user to specify file lists as input files */

    /* TODO: three levels of comparison:
     *   1) file names only
     *   2) stat info + items in #1
     *   3) file contents + items in #2 */

    /* walk by default because there is no input file option */
    int walk = 1;

    /* By default, show info log messages. */
    /* we back off a level on CIRCLE verbosity since its INFO is verbose */
    CIRCLE_loglevel CIRCLE_debug = CIRCLE_LOG_WARN;
    mfu_debug_level = MFU_LOG_VERBOSE;

#ifdef DAOS_SUPPORT
    /* DAOS vars */ 
    daos_args_t* daos_args = daos_args_new();    
#endif

    int option_index = 0;
    static struct option long_options[] = {
        {"output",        1, 0, 'o'},
        {"text",          0, 0, 't'},
        {"base",          0, 0, 'b'},
        {"bufsize",       1, 0, 'B'},
        {"chunksize",     1, 0, 'k'},
        {"daos-prefix",   1, 0, 'X'},
        {"daos-api",      1, 0, 'x'},
        {"direct",        0, 0, 's'},
        {"progress",      1, 0, 'R'},
        {"verbose",       0, 0, 'v'},
        {"quiet",         0, 0, 'q'},
        {"lite",          0, 0, 'l'},
        {"debug",         0, 0, 'd'},
        {"help",          0, 0, 'h'},
        {0, 0, 0, 0}
    };
    int ret = 0;
    int i;

    /* read in command line options */
    int usage = 0;
    int help  = 0;
    unsigned long long bytes = 0;
    while (1) {
        int c = getopt_long(
            argc, argv, "o:tbsvqldh",
            long_options, &option_index
        );

        if (c == -1) {
            break;
        }

        switch (c) {
        case 'o':
            ret = dcmp_option_output_parse(optarg, 0, &dcmp_outputs.outputs, &dcmp_need_compare);
            if (ret) {
                usage = 1;
            }
            break;
        case 't':
            options.format = 0;
            break;
        case 'b':
            options.base++;
            break;
        case 'B':
            if (mfu_abtoull(optarg, &bytes) != MFU_SUCCESS || bytes == 0) {
                if (rank == 0) {
                    MFU_LOG(MFU_LOG_ERR,
                            "Failed to parse block size: '%s'", optarg);
                }
                usage = 1;
            } else {
                copy_opts->buf_size = (size_t)bytes;
            }
            break;
        case 'k':
            if (mfu_abtoull(optarg, &bytes) != MFU_SUCCESS || bytes == 0) {
                if (rank == 0) {
                    MFU_LOG(MFU_LOG_ERR,
                            "Failed to parse chunk size: '%s'", optarg);
                }
                usage = 1;
            } else {
                copy_opts->chunk_size = bytes;
            }
            break;
        case 's':
            copy_opts->direct = true;
            if(rank == 0) {
                MFU_LOG(MFU_LOG_INFO, "Using O_DIRECT");
            }
            break;
        case 'R':
            mfu_progress_timeout = atoi(optarg);
            break;
        case 'v':
            options.verbose++;
            mfu_debug_level = MFU_LOG_VERBOSE;
            break;
        case 'q':
            options.quiet++;
            mfu_debug_level = MFU_LOG_NONE;
            /* since process won't be printed in quiet anyway,
             * disable the algorithm to save some overhead */
            mfu_progress_timeout = 0;
            break;
        case 'l':
            options.lite++;
            break;
        case 'd':
            options.debug++;
            break;
#ifdef DAOS_SUPPORT
        case 'X':
            daos_args->dfs_prefix = MFU_STRDUP(optarg);
            break;
        case 'x':
            if (daos_parse_api_str(optarg, &daos_args->api) != 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to parse --daos-api");
                usage = 1;
            }
            break;
#endif
        case 'h':
        case '?':
            usage = 1;
            help  = 1;
            break;
        default:
            usage = 1;
            break;
        }
    }

    /* check that we got a valid progress value */
    if (mfu_progress_timeout < 0) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Seconds in --progress must be non-negative: %d invalid", mfu_progress_timeout);
        }
        usage = 1;
    }

    /* Generate default output */
    if (options.base || list_empty(&dcmp_outputs.outputs)) {
        /*
         * If -o option is not given,
         * we want to add default output,
         * in case there is no output at all.
         */
        for (i = 0; ; i++) {
            if (dcmp_default_outputs[i] == NULL) {
                break;
            }
            ret = dcmp_option_output_parse(dcmp_default_outputs[i], 1,
	        &dcmp_outputs.outputs, &dcmp_need_compare);
            assert(ret == 0);
        }
    }

    /* we should have two arguments left, source and dest paths */
    int numargs = argc - optind;

    /* if help flag was thrown, don't bother checking usage */
    if (numargs != 2 && !help) {
        MFU_LOG(MFU_LOG_ERR,
            "You must specify a source and destination path.");
        usage = 1;
    }

    /* print usage and exit if necessary */
    if (usage) {
        if (rank == 0) {
            print_usage();
        }
        dcmp_option_fini(&dcmp_outputs.outputs);
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }

    /* allocate space for each path */
    mfu_param_path* paths = (mfu_param_path*) MFU_MALLOC((size_t)numargs * sizeof(mfu_param_path));

    /* pointer to path arguments */
    char** argpaths = (&argv[optind]);

#ifdef DAOS_SUPPORT
    /* For error handling */
    bool daos_do_cleanup = false;
    bool daos_do_exit = false;
    
    /* Set up DAOS arguments, containers, dfs, etc. */
    int daos_rc = daos_setup(rank, argpaths, numargs, daos_args, mfu_src_file, mfu_dst_file);
    if (daos_rc != 0) {
        daos_do_exit = true;
    }

    /* Not yet supported */
    if (mfu_src_file->type == DAOS || mfu_dst_file->type == DAOS) {
        MFU_LOG(MFU_LOG_ERR, "dcmp only supports DAOS POSIX containers with the DFS API.");
        daos_do_cleanup = true;
        daos_do_exit = true;
    }

    if (daos_do_cleanup) {
        daos_cleanup(daos_args, mfu_src_file, mfu_dst_file);
    }
    if (daos_do_exit) {
        dcmp_option_fini();
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }
#endif

    /* process each path */
    mfu_param_path_set_all(numargs, (const char**)argpaths, paths, mfu_src_file, true);

    /* advance to next set of options */
    optind += numargs;

    /* first item is source and second is dest */
    const mfu_param_path* srcpath  = &paths[0];
    const mfu_param_path* destpath = &paths[1];

    /* create an empty file list */
    mfu_flist flist1 = mfu_flist_new();
    mfu_flist flist2 = mfu_flist_new();

    if (rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Walking source path");
    }
    mfu_flist_walk_param_paths(1,  srcpath, walk_opts, flist1, mfu_src_file);

    if (rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Walking destination path");
    }
    mfu_flist_walk_param_paths(1, destpath, walk_opts, flist2, mfu_dst_file);

    /* store src and dest path strings */
    const char* path1 = srcpath->path;
    const char* path2 = destpath->path;

    /* map files to ranks based on portion following prefix directory */
    mfu_flist flist3 = mfu_flist_remap(flist1, (mfu_flist_map_fn)dcmp_map_fn, (const void*)path1);
    mfu_flist flist4 = mfu_flist_remap(flist2, (mfu_flist_map_fn)dcmp_map_fn, (const void*)path2);

    /* map each file name to its index and its comparison state */
    strmap* map1 = dcmp_strmap_creat(flist3, path1);
    strmap* map2 = dcmp_strmap_creat(flist4, path2);

    /* compare files in map1 with those in map2 */
    int tmp_rc = dcmp_strmap_compare(flist3, map1, flist4, map2, strlen(path1), copy_opts, srcpath, destpath,
                                     mfu_src_file, mfu_dst_file, options.lite, &dcmp_need_compare);
    if (tmp_rc < 0) {
        /* hit a read error on at least one file */
        rc = 1;
    }

    /* check the results are valid */
    if (options.debug) {
        dcmp_strmap_check(map1, map2, &dcmp_need_compare);
    }

    /* write data to cache files and print summary */
    dcmp_outputs_write(flist3, map1, flist4, map2, options.format,
        &dcmp_outputs.outputs);

    /* free maps of file names to comparison state info */
    strmap_delete(&map1);
    strmap_delete(&map2);

    /* free file lists */
    mfu_flist_free(&flist1);
    mfu_flist_free(&flist2);
    mfu_flist_free(&flist3);
    mfu_flist_free(&flist4);

    /* free all param paths */
    mfu_param_path_free_all(numargs, paths);

    /* free memory allocated to hold params */
    mfu_free(&paths);

    dcmp_option_fini(&dcmp_outputs.outputs);

    /* free the copy options structure */
    mfu_copy_opts_delete(&copy_opts);

    /* free the walk options */
    mfu_walk_opts_delete(&walk_opts);

#ifdef DAOS_SUPPORT
    /* Cleanup DAOS-related variables, etc. */
    daos_cleanup(daos_args, mfu_src_file, mfu_dst_file);
#endif

    /* delete file objects */
    mfu_file_delete(&mfu_src_file);
    mfu_file_delete(&mfu_dst_file);

    /* shut down */
    mfu_finalize();
    MPI_Finalize();

    return rc;
}
