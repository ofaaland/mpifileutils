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

#include "mfu_output.h"
#include "mfu.h"
#include "strmap.h"
#include "list.h"

/* for daos */
#ifdef DAOS_SUPPORT
#include "mfu_daos.h"
#endif

uint64_t dcmp_field_depend[] = {
    [DCMPF_EXIST]   = DCMPF_EXIST_DEPEND,
    [DCMPF_TYPE]    = DCMPF_TYPE_DEPEND,
    [DCMPF_SIZE]    = DCMPF_SIZE_DEPEND,
    [DCMPF_UID]     = DCMPF_UID_DEPEND,
    [DCMPF_GID]     = DCMPF_GID_DEPEND,
    [DCMPF_ATIME]   = DCMPF_ATIME_DEPEND,
    [DCMPF_MTIME]   = DCMPF_MTIME_DEPEND,
    [DCMPF_CTIME]   = DCMPF_CTIME_DEPEND,
    [DCMPF_PERM]    = DCMPF_PERM_DEPEND,
    [DCMPF_ACL]     = DCMPF_ACL_DEPEND,
    [DCMPF_CONTENT] = DCMPF_CONTENT_DEPEND,
};

static const char* dcmp_field_to_string(dcmp_field field, int simple)
{
    assert(field < DCMPF_MAX);
    switch (field) {
    case DCMPF_EXIST:
        if (simple) {
            return "EXIST";
        } else {
            return "existence";
        }
        break;
    case DCMPF_TYPE:
        if (simple) {
            return "TYPE";
        } else {
            return "type";
        }
        break;
    case DCMPF_SIZE:
        if (simple) {
            return "SIZE";
        } else {
            return "size";
        }
        break;
    case DCMPF_UID:
        if (simple) {
            return "UID";
        } else {
            return "user ID";
        }
        break;
    case DCMPF_GID:
        if (simple) {
            return "GID";
        } else {
            return "group ID";
        }
        break;
    case DCMPF_ATIME:
        if (simple) {
            return "ATIME";
        } else {
            return "access time";
        }
        break;
    case DCMPF_MTIME:
        if (simple) {
            return "MTIME";
        } else {
            return "modification time";
        }
        break;
    case DCMPF_CTIME:
        if (simple) {
            return "CTIME";
        } else {
            return "change time";
        }
        break;
    case DCMPF_PERM:
        if (simple) {
            return "PERM";
        } else {
            return "permission";
        }
        break;
    case DCMPF_ACL:
        if (simple) {
            return "ACL";
        } else {
            return "Access Control Lists";
        }
        break;
    case DCMPF_CONTENT:
        if (simple) {
            return "CONTENT";
        } else {
            return "content";
        }
        break;
    case DCMPF_MAX:
    default:
        return NULL;
        break;
    }
    return NULL;
}

static int dcmp_field_from_string(const char* string, dcmp_field *field)
{
    dcmp_field i;
    for (i = 0; i < DCMPF_MAX; i ++) {
        if (strcmp(dcmp_field_to_string(i, 1), string) == 0) {
            *field = i;
            return 0;
        }
    }
    return -ENOENT;
}

static const char* dcmp_state_to_string(dcmp_state state, int simple)
{
    switch (state) {
    case DCMPS_INIT:
        if (simple) {
            return "INIT";
        } else {
            return "initial";
        }
        break;
    case DCMPS_COMMON:
        if (simple) {
            return "COMMON";
        } else {
            return "the same";
        }
        break;
    case DCMPS_DIFFER:
        if (simple) {
            return "DIFFER";
        } else {
            return "different";
        }
        break;
    case DCMPS_ONLY_SRC:
        if (simple) {
            return "ONLY_SRC";
        } else {
            return "exist only in source directory";
        }
        break;
    case DCMPS_ONLY_DEST:
        if (simple) {
            return "ONLY_DEST";
        } else {
            return "exist only in destination directory";
        }
        break;
    case DCMPS_MAX:
    default:
        return NULL;
        break;
    }
    return NULL;
}

static int dcmp_state_from_string(const char* string, dcmp_state *state)
{
    dcmp_state i;
    for (i = DCMPS_INIT; i < DCMPS_MAX; i ++) {
        if (strcmp(dcmp_state_to_string(i, 1), string) == 0) {
            *state = i;
            return 0;
        }
    }
    return -ENOENT;
}

/* given a filename as the key, encode an index followed
 * by the init state */
static void dcmp_strmap_item_init(
    strmap* map,
    const char *key,
    uint64_t item_index)
{
    /* Should be long enough for 64 bit number and DCMPF_MAX */
    char val[21 + DCMPF_MAX];
    int i;

    /* encode the index */
    int len = snprintf(val, sizeof(val), "%llu",
                       (unsigned long long) item_index);

    /* encode the state (state characters and trailing NUL) */
    assert((size_t)len + DCMPF_MAX + 1 <= (sizeof(val)));
    size_t position = strlen(val);
    for (i = 0; i < DCMPF_MAX; i++) {
        val[position] = DCMPS_INIT;
        position++;
    }
    val[position] = '\0';

    /* add item to map */
    strmap_set(map, key, val);
}

void dcmp_strmap_item_update(
    strmap* map,
    const char *key,
    dcmp_field field,
    dcmp_state state)
{
    /* Should be long enough for 64 bit number and DCMPF_MAX */
    char new_val[21 + DCMPF_MAX];

    /* lookup item from map */
    const char* val = strmap_get(map, key);

    /* copy existing index over */
    assert(field < DCMPF_MAX);
    assert(strlen(val) + 1 <= sizeof(new_val));
    strcpy(new_val, val);

    /* set new state value */
    size_t position = strlen(new_val) - DCMPF_MAX;
    new_val[position + field] = state;

    /* reinsert item in map */
    strmap_set(map, key, new_val);
}

static int dcmp_strmap_item_index(
    strmap* map,
    const char *key,
    uint64_t *item_index)
{
    /* Should be long enough for 64 bit number and DCMPF_MAX */
    char new_val[21 + DCMPF_MAX];

    /* lookup item from map */
    const char* val = strmap_get(map, key);
    if (val == NULL) {
        return -1;
    }

    /* extract index */
    assert(strlen(val) + 1 <= sizeof(new_val));
    strcpy(new_val, val);
    new_val[strlen(new_val) - DCMPF_MAX] = '\0';
    *item_index = strtoull(new_val, NULL, 0);

    return 0;
}

static int dcmp_strmap_item_state(
    strmap* map,
    const char *key,
    dcmp_field field,
    dcmp_state *state)
{
    /* lookup item from map */
    const char* val = strmap_get(map, key);
    if (val == NULL) {
        return -1;
    }

    /* extract state */
    assert(strlen(val) > DCMPF_MAX);
    assert(field < DCMPF_MAX);
    size_t position = strlen(val) - DCMPF_MAX;
    *state = val[position + field];

    return 0;
}

/* map each file name to its index in the file list and initialize
 * its state for comparison operation */
strmap* dcmp_strmap_creat(mfu_flist list, const char* prefix)
{
    /* create a new string map to map a file name to a string
     * encoding its index and state */
    strmap* map = strmap_new();

    /* determine length of prefix string */
    size_t prefix_len = strlen(prefix);

    /* iterate over each item in the file list */
    uint64_t i = 0;
    uint64_t count = mfu_flist_size(list);
    while (i < count) {
        /* get full path of file name */
        const char* name = mfu_flist_file_get_name(list, i);

        /* ignore prefix portion of path */
        name += prefix_len;

        /* create entry for this file */
        dcmp_strmap_item_init(map, name, i);

        /* go to next item in list */
        i++;
    }

    return map;
}

static void dcmp_compare_acl(
    const char *key,
    mfu_flist src_list,
    uint64_t src_index,
    mfu_flist dst_list,
    uint64_t dst_index,
    strmap* src_map,
    strmap* dst_map,
    int *diff)
{
    void *src_val, *dst_val;
    ssize_t src_size, dst_size;
    bool is_same = true;

#if DCOPY_USE_XATTRS
    src_val = mfu_flist_file_get_acl(src_list, src_index, &src_size,
                                     "system.posix_acl_access");
    dst_val = mfu_flist_file_get_acl(dst_list, dst_index, &dst_size,
                                     "system.posix_acl_access");

    if (src_size == dst_size) {
        if (src_size > 0) {
            if (memcmp(src_val, dst_val, src_size)) {
                is_same = false;
                goto out;
            }
        }
    } else {
        is_same = false;
        goto out;
    }

    mfu_filetype type = mfu_flist_file_get_type(src_list, src_index);
    if (type == MFU_TYPE_DIR) {
        mfu_free(&src_val);
        mfu_free(&dst_val);

        src_val = mfu_flist_file_get_acl(src_list, src_index, &src_size,
                                         "system.posix_acl_default");
        dst_val = mfu_flist_file_get_acl(dst_list, dst_index, &dst_size,
                                         "system.posix_acl_default");

        if (src_size == dst_size) {
            if (src_size > 0) {
                if (memcmp(src_val, dst_val, src_size)) {
                    is_same = false;
                    goto out;
                }
            }
        } else {
            is_same = false;
            goto out;
        }
    }

out:
    mfu_free(&src_val);
    mfu_free(&dst_val);

#endif
    if (is_same) {
        dcmp_strmap_item_update(src_map, key, DCMPF_ACL, DCMPS_COMMON);
        dcmp_strmap_item_update(dst_map, key, DCMPF_ACL, DCMPS_COMMON);
    } else {
        dcmp_strmap_item_update(src_map, key, DCMPF_ACL, DCMPS_DIFFER);
        dcmp_strmap_item_update(dst_map, key, DCMPF_ACL, DCMPS_DIFFER);
        (*diff)++;
    }
}

static int dcmp_option_need_compare(struct mfu_need_compare *compare_flags,
    dcmp_field field)
{
    return compare_flags->need_compare[field];
}

/* Return -1 when error, return 0 when equal, return > 0 when diff */
static int dcmp_compare_metadata(
    mfu_flist src_list,
    strmap* src_map,
    uint64_t src_index,
    mfu_flist dst_list,
    strmap* dst_map,
    uint64_t dst_index,
    const char* key,
    struct mfu_need_compare *compare_flags)
{
    int diff = 0;

    if (dcmp_option_need_compare(compare_flags, DCMPF_SIZE)) {
        mfu_filetype type = mfu_flist_file_get_type(src_list, src_index);
        if (type != MFU_TYPE_DIR) {
            uint64_t src = mfu_flist_file_get_size(src_list, src_index);
            uint64_t dst = mfu_flist_file_get_size(dst_list, dst_index);
            if (src != dst) {
                /* file size is different */
                dcmp_strmap_item_update(src_map, key, DCMPF_SIZE, DCMPS_DIFFER);
                dcmp_strmap_item_update(dst_map, key, DCMPF_SIZE, DCMPS_DIFFER);
                diff++;
             } else {
                dcmp_strmap_item_update(src_map, key, DCMPF_SIZE, DCMPS_COMMON);
                dcmp_strmap_item_update(dst_map, key, DCMPF_SIZE, DCMPS_COMMON);
             }
        } else {
            dcmp_strmap_item_update(src_map, key, DCMPF_SIZE, DCMPS_COMMON);
            dcmp_strmap_item_update(dst_map, key, DCMPF_SIZE, DCMPS_COMMON);
        }
    }
    if (dcmp_option_need_compare(compare_flags, DCMPF_GID)) {
        uint64_t src = mfu_flist_file_get_gid(src_list, src_index);
        uint64_t dst = mfu_flist_file_get_gid(dst_list, dst_index);
        if (src != dst) {
            /* file gid is different */
            dcmp_strmap_item_update(src_map, key, DCMPF_GID, DCMPS_DIFFER);
            dcmp_strmap_item_update(dst_map, key, DCMPF_GID, DCMPS_DIFFER);
             diff++;
        } else {
            dcmp_strmap_item_update(src_map, key, DCMPF_GID, DCMPS_COMMON);
            dcmp_strmap_item_update(dst_map, key, DCMPF_GID, DCMPS_COMMON);
        }
    }
    if (dcmp_option_need_compare(compare_flags, DCMPF_UID)) {
        uint64_t src = mfu_flist_file_get_uid(src_list, src_index);
        uint64_t dst = mfu_flist_file_get_uid(dst_list, dst_index);
        if (src != dst) {
            /* file uid is different */
            dcmp_strmap_item_update(src_map, key, DCMPF_UID, DCMPS_DIFFER);
            dcmp_strmap_item_update(dst_map, key, DCMPF_UID, DCMPS_DIFFER);
            diff++;
        } else {
            dcmp_strmap_item_update(src_map, key, DCMPF_UID, DCMPS_COMMON);
            dcmp_strmap_item_update(dst_map, key, DCMPF_UID, DCMPS_COMMON);
        }
    }
    if (dcmp_option_need_compare(compare_flags, DCMPF_ATIME)) {
        uint64_t src_atime      = mfu_flist_file_get_atime(src_list, src_index);
        uint64_t src_atime_nsec = mfu_flist_file_get_atime_nsec(src_list, src_index);
        uint64_t dst_atime      = mfu_flist_file_get_atime(dst_list, dst_index);
        uint64_t dst_atime_nsec = mfu_flist_file_get_atime_nsec(dst_list, dst_index);
        if ((src_atime != dst_atime) || (src_atime_nsec != dst_atime_nsec)) {
            /* file atime is different */
            dcmp_strmap_item_update(src_map, key, DCMPF_ATIME, DCMPS_DIFFER);
            dcmp_strmap_item_update(dst_map, key, DCMPF_ATIME, DCMPS_DIFFER);
            diff++;
        } else {
            dcmp_strmap_item_update(src_map, key, DCMPF_ATIME, DCMPS_COMMON);
            dcmp_strmap_item_update(dst_map, key, DCMPF_ATIME, DCMPS_COMMON);
        }
    }
    if (dcmp_option_need_compare(compare_flags, DCMPF_MTIME)) {
        uint64_t src_mtime      = mfu_flist_file_get_mtime(src_list, src_index);
        uint64_t src_mtime_nsec = mfu_flist_file_get_mtime_nsec(src_list, src_index);
        uint64_t dst_mtime      = mfu_flist_file_get_mtime(dst_list, dst_index);
        uint64_t dst_mtime_nsec = mfu_flist_file_get_mtime_nsec(dst_list, dst_index);
        if ((src_mtime != dst_mtime) || (src_mtime_nsec != dst_mtime_nsec)) {
            /* file mtime is different */
            dcmp_strmap_item_update(src_map, key, DCMPF_MTIME, DCMPS_DIFFER);
            dcmp_strmap_item_update(dst_map, key, DCMPF_MTIME, DCMPS_DIFFER);
            diff++;
        } else {
            dcmp_strmap_item_update(src_map, key, DCMPF_MTIME, DCMPS_COMMON);
            dcmp_strmap_item_update(dst_map, key, DCMPF_MTIME, DCMPS_COMMON);
        }
    }
    if (dcmp_option_need_compare(compare_flags, DCMPF_CTIME)) {
        uint64_t src_ctime      = mfu_flist_file_get_ctime(src_list, src_index);
        uint64_t src_ctime_nsec = mfu_flist_file_get_ctime_nsec(src_list, src_index);
        uint64_t dst_ctime      = mfu_flist_file_get_ctime(dst_list, dst_index);
        uint64_t dst_ctime_nsec = mfu_flist_file_get_ctime_nsec(dst_list, dst_index);
        if ((src_ctime != dst_ctime) || (src_ctime_nsec != dst_ctime_nsec)) {
            /* file ctime is different */
            dcmp_strmap_item_update(src_map, key, DCMPF_CTIME, DCMPS_DIFFER);
            dcmp_strmap_item_update(dst_map, key, DCMPF_CTIME, DCMPS_DIFFER);
            diff++;
        } else {
            dcmp_strmap_item_update(src_map, key, DCMPF_CTIME, DCMPS_COMMON);
            dcmp_strmap_item_update(dst_map, key, DCMPF_CTIME, DCMPS_COMMON);
        }
    }
    if (dcmp_option_need_compare(compare_flags, DCMPF_PERM)) {
        uint64_t src = mfu_flist_file_get_perm(src_list, src_index);
        uint64_t dst = mfu_flist_file_get_perm(dst_list, dst_index);
        if (src != dst) {
            /* file perm is different */
            dcmp_strmap_item_update(src_map, key, DCMPF_PERM, DCMPS_DIFFER);
            dcmp_strmap_item_update(dst_map, key, DCMPF_PERM, DCMPS_DIFFER);
            diff++;
        } else {
            dcmp_strmap_item_update(src_map, key, DCMPF_PERM, DCMPS_COMMON);
            dcmp_strmap_item_update(dst_map, key, DCMPF_PERM, DCMPS_COMMON);
        }
    }
    if (dcmp_option_need_compare(compare_flags, DCMPF_ACL)) {
        dcmp_compare_acl(key, src_list,src_index,
                         dst_list, dst_index,
                         src_map, dst_map, &diff);
    }

    return diff;
}

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

/* compare entries from src into dst */
int dcmp_strmap_compare(
    mfu_flist src_list,
    strmap* src_map,
    mfu_flist dst_list,
    strmap* dst_map,
    size_t strlen_prefix,
    mfu_copy_opts_t* copy_opts,
    const mfu_param_path* src_path,
    const mfu_param_path* dest_path,
    mfu_file_t* mfu_src_file,
    mfu_file_t* mfu_dst_file,
    int lite_comparison,
    struct mfu_need_compare *compare_flags,
    dcmp_compare_t comparer)
{
    /* assume we'll succeed */
    int rc = 0;
    int tmp_rc;

    /* wait for all tasks and start timer */
    MPI_Barrier(MPI_COMM_WORLD);

    /* let user know what we're doing */
    if (mfu_debug_level >= MFU_LOG_VERBOSE && mfu_rank == 0) {
         MFU_LOG(MFU_LOG_INFO, "Comparing items");
    }

    time_t   time_started;
    time_t   time_ended;

    double start_compare = MPI_Wtime();
    time(&time_started);

    /* create compare_lists */
    mfu_flist src_compare_list = mfu_flist_subset(src_list);
    mfu_flist dst_compare_list = mfu_flist_subset(dst_list);

    /* get mtime seconds and nsecs to check modification times of src & dst */
    uint64_t src_mtime;
    uint64_t src_mtime_nsec;
    uint64_t dst_mtime;
    uint64_t dst_mtime_nsec;

    /* iterate over each item in source map */
    const strmap_node* node;
    strmap_foreach(src_map, node) {

        /* get file name */
        const char* key = strmap_node_key(node);

        /* get index of source file */
        uint64_t src_index;
        tmp_rc = dcmp_strmap_item_index(src_map, key, &src_index);
        assert(tmp_rc == 0);

        /* get index of destination file */
        uint64_t dst_index;
        tmp_rc = dcmp_strmap_item_index(dst_map, key, &dst_index);

        /* get mtime seconds and nsecs to check modification times of src & dst */
        src_mtime      = mfu_flist_file_get_mtime(src_list, src_index);
        src_mtime_nsec = mfu_flist_file_get_mtime_nsec(src_list, src_index);
        dst_mtime      = mfu_flist_file_get_mtime(dst_list, dst_index);
        dst_mtime_nsec = mfu_flist_file_get_mtime_nsec(dst_list, dst_index);

        if (tmp_rc) {
            dcmp_strmap_item_update(src_map, key, DCMPF_EXIST, DCMPS_ONLY_SRC);

            /* skip uncommon files, all other states are DCMPS_INIT */
            continue;
        }

        dcmp_strmap_item_update(src_map, key, DCMPF_EXIST, DCMPS_COMMON);
        dcmp_strmap_item_update(dst_map, key, DCMPF_EXIST, DCMPS_COMMON);

        /* get modes of files */
        mode_t src_mode = (mode_t) mfu_flist_file_get_mode(src_list,
            src_index);
        mode_t dst_mode = (mode_t) mfu_flist_file_get_mode(dst_list,
            dst_index);

        tmp_rc = dcmp_compare_metadata(src_list, src_map, src_index,
             dst_list, dst_map, dst_index,
             key, compare_flags);

        assert(tmp_rc >= 0);

        if (!dcmp_option_need_compare(compare_flags, DCMPF_TYPE)) {
            /*
             * Skip if no need to compare type.
             * All the following comparison depends on type.
             */
            continue;
        }

        /* check whether files are of the same type */
        if ((src_mode & S_IFMT) != (dst_mode & S_IFMT)) {
            /* file type is different, no need to go any futher */
            dcmp_strmap_item_update(src_map, key, DCMPF_TYPE, DCMPS_DIFFER);
            dcmp_strmap_item_update(dst_map, key, DCMPF_TYPE, DCMPS_DIFFER);

            if (!dcmp_option_need_compare(compare_flags, DCMPF_CONTENT)) {
                continue;
            }

            /* take them as differ content */
            dcmp_strmap_item_update(src_map, key, DCMPF_CONTENT, DCMPS_DIFFER);
            dcmp_strmap_item_update(dst_map, key, DCMPF_CONTENT, DCMPS_DIFFER);
            continue;
        }

        dcmp_strmap_item_update(src_map, key, DCMPF_TYPE, DCMPS_COMMON);
        dcmp_strmap_item_update(dst_map, key, DCMPF_TYPE, DCMPS_COMMON);

        if (!dcmp_option_need_compare(compare_flags, DCMPF_CONTENT)) {
            /* Skip if no need to compare content. */
            continue;
        }

        /* for now, we can only compare content of regular files */
        /* TODO: add support for symlinks */
        if (! S_ISREG(dst_mode)) {
            /* not regular file, take them as common content */
            dcmp_strmap_item_update(src_map, key, DCMPF_CONTENT, DCMPS_COMMON);
            dcmp_strmap_item_update(dst_map, key, DCMPF_CONTENT, DCMPS_COMMON);
            continue;
        }

        dcmp_state state;
        tmp_rc = dcmp_strmap_item_state(src_map, key, DCMPF_SIZE, &state);
        assert(tmp_rc == 0);
        if (state == DCMPS_DIFFER) {
            /* file size is different, their contents should be different */
            dcmp_strmap_item_update(src_map, key, DCMPF_CONTENT, DCMPS_DIFFER);
            dcmp_strmap_item_update(dst_map, key, DCMPF_CONTENT, DCMPS_DIFFER);
            continue;
        }

        if (lite_comparison) {
            if ((src_mtime != dst_mtime) || (src_mtime_nsec != dst_mtime_nsec)) {
                /* modification times are different, assume content is different.
                 * I don't think we can assume contents are different if the
                 * lite option is not on. Because files can have different
                 * modification times, but still have the same content. */
                dcmp_strmap_item_update(src_map, key, DCMPF_CONTENT, DCMPS_DIFFER);
                dcmp_strmap_item_update(dst_map, key, DCMPF_CONTENT, DCMPS_DIFFER);
            } else {
                dcmp_strmap_item_update(src_map, key, DCMPF_CONTENT, DCMPS_COMMON);
                dcmp_strmap_item_update(dst_map, key, DCMPF_CONTENT, DCMPS_COMMON);
            }
            continue;
        }

        /* If we get to this point, we need to open files and compare
         * file contents.  We'll first identify all such files so that
         * we can do this comparison in parallel more effectively.  For
         * now copy these files to the list of files we need to compare. */
        mfu_flist_file_copy(src_list, src_index, src_compare_list);
        mfu_flist_file_copy(dst_list, dst_index, dst_compare_list);
    }

    /* summarize lists of files for which we need to compare data contents */
    mfu_flist_summarize(src_compare_list);
    mfu_flist_summarize(dst_compare_list);

    uint64_t cmp_global_size = 0;
    if (!lite_comparison) {
        /* compare the contents of the files if we have anything in the compare list */
        cmp_global_size = mfu_flist_global_size(src_compare_list);
        if (cmp_global_size > 0) {
            tmp_rc = comparer(src_compare_list, src_map, dst_compare_list,
                    dst_map, strlen_prefix, copy_opts, mfu_src_file, mfu_dst_file);
            if (tmp_rc < 0) {
                /* got a read error, signal that back to caller */
                rc = -1;
            }
        }
    }

    /* wait for all procs to finish before stopping timer */
    MPI_Barrier(MPI_COMM_WORLD);
    double end_compare = MPI_Wtime();
    time(&time_ended);

    /* initalize total_bytes_read to zero */
    uint64_t total_bytes_read = 0;

    /* get total bytes read (if any) */
    if (cmp_global_size > 0) {
        total_bytes_read = get_total_bytes_read(src_compare_list);
    }

    time_strmap_compare(src_list, start_compare, end_compare, &time_started,
                        &time_ended, total_bytes_read);

    /* free the compare flists */
    mfu_flist_free(&dst_compare_list);
    mfu_flist_free(&src_compare_list);

    return rc;
}

/* loop on the src map to check the results */
static void dcmp_strmap_check_src(strmap* src_map,
                                  strmap* dst_map,
    struct mfu_need_compare *compare_flags)
{
    assert(dcmp_option_need_compare(compare_flags, DCMPF_EXIST));
    /* iterate over each item in source map */
    const strmap_node* node;
    strmap_foreach(src_map, node) {
        /* get file name */
        const char* key = strmap_node_key(node);
        int only_src = 0;

        /* get index of source file */
        uint64_t src_index;
        int ret = dcmp_strmap_item_index(src_map, key, &src_index);
        assert(ret == 0);

        /* get index of destination file */
        uint64_t dst_index;
        ret = dcmp_strmap_item_index(dst_map, key, &dst_index);
        if (ret) {
            only_src = 1;
        }

        /* First check exist state */
        dcmp_state src_exist_state;
        ret = dcmp_strmap_item_state(src_map, key, DCMPF_EXIST,
            &src_exist_state);
        assert(ret == 0);

        dcmp_state dst_exist_state;
        ret = dcmp_strmap_item_state(dst_map, key, DCMPF_EXIST,
            &dst_exist_state);
        if (only_src) {
            assert(ret);
        } else {
            assert(ret == 0);
        }

        if (only_src) {
            /* This file never checked for dest */
            assert(src_exist_state == DCMPS_ONLY_SRC);
        } else {
            assert(src_exist_state == dst_exist_state);
            assert(dst_exist_state == DCMPS_COMMON);
        }

        dcmp_field field;
        for (field = 0; field < DCMPF_MAX; field++) {
            if (field == DCMPF_EXIST) {
                continue;
            }

            /* get state of src and dest */
            dcmp_state src_state;
            ret = dcmp_strmap_item_state(src_map, key, field, &src_state);
            assert(ret == 0);

            dcmp_state dst_state;
            ret = dcmp_strmap_item_state(dst_map, key, field, &dst_state);
            if (only_src) {
                assert(ret);
            } else {
                assert(ret == 0);
            }

            if (only_src) {
                /* all states are not checked */
                assert(src_state == DCMPS_INIT);
            } else {
                /* all stats of source and dest are the same */
                assert(src_state == dst_state);
                /* all states are either common, differ or skipped */
                if (dcmp_option_need_compare(compare_flags, field)) {
                    assert(src_state == DCMPS_COMMON || src_state == DCMPS_DIFFER);
                } else {
                    // XXXX
                    if (src_state != DCMPS_INIT) {
                        MFU_LOG(MFU_LOG_ERR, "XXX %s wrong state %s\n",
                                dcmp_field_to_string(field, 1), dcmp_state_to_string(src_state, 1));
                    }
                    assert(src_state == DCMPS_INIT);
                }
            }
        }
    }
}

/* loop on the dest map to check the results */
static void dcmp_strmap_check_dst(strmap* src_map,
    strmap* dst_map,
    struct mfu_need_compare *compare_flags)
{
    assert(dcmp_option_need_compare(compare_flags, DCMPF_EXIST));

    /* iterate over each item in dest map */
    const strmap_node* node;
    strmap_foreach(dst_map, node) {
        /* get file name */
        const char* key = strmap_node_key(node);
        int only_dest = 0;

        /* get index of destination file */
        uint64_t dst_index;
        int ret = dcmp_strmap_item_index(dst_map, key, &dst_index);
        assert(ret == 0);

        /* get index of source file */
        uint64_t src_index;
        ret = dcmp_strmap_item_index(src_map, key, &src_index);
        if (ret) {
            /* This file only exist in dest */
            only_dest = 1;
        }

        /* First check exist state */
        dcmp_state src_exist_state;
        ret = dcmp_strmap_item_state(src_map, key, DCMPF_EXIST,
            &src_exist_state);
        if (only_dest) {
            assert(ret);
        } else {
            assert(ret == 0);
        }

        dcmp_state dst_exist_state;
        ret = dcmp_strmap_item_state(dst_map, key, DCMPF_EXIST,
            &dst_exist_state);
        assert(ret == 0);

        if (only_dest) {
            /* This file never checked for dest */
            assert(dst_exist_state == DCMPS_INIT);
        } else {
            assert(src_exist_state == dst_exist_state);
            assert(dst_exist_state == DCMPS_COMMON ||
                dst_exist_state == DCMPS_ONLY_SRC);
        }

        dcmp_field field;
        for (field = 0; field < DCMPF_MAX; field++) {
            if (field == DCMPF_EXIST) {
                continue;
            }

            /* get state of src and dest */
            dcmp_state src_state;
            ret = dcmp_strmap_item_state(src_map, key, field,
                &src_state);
            if (only_dest) {
                assert(ret);
            } else {
                assert(ret == 0);
            }

            dcmp_state dst_state;
            ret = dcmp_strmap_item_state(dst_map, key, field,
                &dst_state);
            assert(ret == 0);

            if (only_dest) {
                /* all states are not checked */
                assert(dst_state == DCMPS_INIT);
            } else {
                assert(src_state == dst_state);
                /* all states are either common, differ or skipped */
                assert(src_state == DCMPS_COMMON ||
                    src_state == DCMPS_DIFFER ||
                    src_state == DCMPS_INIT);
            }

            if (only_dest || dst_exist_state == DCMPS_ONLY_SRC) {
                /* This file never checked for dest */
                assert(dst_state == DCMPS_INIT);
            } else {
                /* all stats of source and dest are the same */
                assert(src_state == dst_state);
                /* all states are either common, differ or skipped */
                if (dcmp_option_need_compare(compare_flags, field)) {
                    assert(src_state == DCMPS_COMMON ||
                    src_state == DCMPS_DIFFER);
                } else {
                    assert(src_state == DCMPS_INIT);
                }
            }
        }
    }
}

/* check the result maps are valid */
void dcmp_strmap_check(
    strmap* src_map,
    strmap* dst_map,
    struct mfu_need_compare *compare_flags)
{
    dcmp_strmap_check_src(src_map, dst_map, compare_flags);
    dcmp_strmap_check_dst(src_map, dst_map, compare_flags);
}

static struct dcmp_expression* dcmp_expression_alloc(void)
{
    struct dcmp_expression *expression;

    expression = (struct dcmp_expression*)
        MFU_MALLOC(sizeof(struct dcmp_expression));
    INIT_LIST_HEAD(&expression->linkage);

    return expression;
}

static void dcmp_expression_free(struct dcmp_expression *expression)
{
    assert(list_empty(&expression->linkage));
    mfu_free(&expression);
}

static void dcmp_expression_print(
    struct dcmp_expression *expression,
    int simple)
{
    if (simple) {
        printf("(%s = %s)", dcmp_field_to_string(expression->field, 1),
            dcmp_state_to_string(expression->state, 1));
    } else {
        /* Special output for DCMPF_EXIST */
        if (expression->field == DCMPF_EXIST) {
            assert(expression->state == DCMPS_ONLY_SRC ||
                   expression->state == DCMPS_ONLY_DEST ||
                   expression->state == DCMPS_DIFFER ||
                   expression->state == DCMPS_COMMON);
            switch (expression->state) {
            case DCMPS_ONLY_SRC:
                printf("exist only in source directory");
                break;
            case DCMPS_ONLY_DEST:
                printf("exist only in destination directory");
                break;
            case DCMPS_COMMON:
                printf("exist in both directories");
                break;
            case DCMPS_DIFFER:
                printf("exist only in one directory");
                break;
            /* To avoid compiler warnings be exhaustive
             * and include all possible expression states */
            case DCMPS_INIT: //fall through
            case  DCMPS_MAX: //fall through
            default:
                assert(0);
            }
        } else {
            assert(expression->state == DCMPS_DIFFER ||
                   expression->state == DCMPS_COMMON);
            printf("have %s %s", dcmp_state_to_string(expression->state, 0),
                   dcmp_field_to_string(expression->field, 0));
            if (expression->state == DCMPS_DIFFER) {
                /* Make sure plurality is valid */
                printf("s");
            }
        }
    }
}

static int dcmp_expression_match(
    struct dcmp_expression *expression,
    strmap* map,
    const char* key)
{
    int ret;
    dcmp_state state;
    dcmp_state exist_state;

    ret = dcmp_strmap_item_state(map, key, DCMPF_EXIST, &exist_state);
    assert(ret == 0);
    if (exist_state == DCMPS_ONLY_SRC) {
        /*
         * Map is source and file only exist in source.
         * All fields are invalid execpt DCMPF_EXIST.
         */
        if (expression->field == DCMPF_EXIST &&
            (expression->state == DCMPS_ONLY_SRC ||
             expression->state == DCMPS_DIFFER)) {
            return 1;
        }
        return 0;
    } else if (exist_state == DCMPS_INIT) {
        /*
         * Map is dest and file only exist in dest.
         * All fields are invalid execpt DCMPF_EXIST.
         * DCMPS_INIT sate of DCMPF_EXIST in dest is
         * considered as DCMPS_ONLY_DEST.
         */
        if (expression->field == DCMPF_EXIST &&
            (expression->state == DCMPS_ONLY_DEST ||
             expression->state == DCMPS_DIFFER)) {
            return 1;
        }
        return 0;
    } else {
        assert(exist_state == DCMPS_COMMON);
        if (expression->field == DCMPF_EXIST) {
            if (expression->state == DCMPS_COMMON) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    assert(exist_state == DCMPS_COMMON);
    assert(expression->field != DCMPF_EXIST);

    ret = dcmp_strmap_item_state(map, key, expression->field, &state);
    assert(ret == 0);

     /* All fields should have been compared. */
    assert(state == DCMPS_COMMON || state == DCMPS_DIFFER);
    if (expression->state == state) {
        return 1;
    }

    return 0;
}

static struct dcmp_conjunction* dcmp_conjunction_alloc(void)
{
    struct dcmp_conjunction *conjunction;

    conjunction = (struct dcmp_conjunction*)
        MFU_MALLOC(sizeof(struct dcmp_conjunction));
    INIT_LIST_HEAD(&conjunction->linkage);
    INIT_LIST_HEAD(&conjunction->expressions);
    conjunction->src_matched_list = mfu_flist_new();
    conjunction->dst_matched_list = mfu_flist_new();

    return conjunction;
}

static void dcmp_conjunction_add_expression(
    struct dcmp_conjunction* conjunction,
    struct dcmp_expression* expression)
{
    assert(list_empty(&expression->linkage));
    list_add_tail(&expression->linkage, &conjunction->expressions);
}

static void dcmp_conjunction_free(struct dcmp_conjunction *conjunction)
{
    struct dcmp_expression* expression;
    struct dcmp_expression* n;

    assert(list_empty(&conjunction->linkage));
    list_for_each_entry_safe(expression,
                             n,
                             &conjunction->expressions,
                             linkage) {
        list_del_init(&expression->linkage);
        dcmp_expression_free(expression);
    }
    assert(list_empty(&conjunction->expressions));
    mfu_flist_free(&conjunction->src_matched_list);
    mfu_flist_free(&conjunction->dst_matched_list);
    mfu_free(&conjunction);
}

static void dcmp_conjunction_print(
    struct dcmp_conjunction *conjunction,
    int simple)
{
    struct dcmp_expression* expression;

    if (simple) {
        printf("(");
    }
    list_for_each_entry(expression,
                        &conjunction->expressions,
                        linkage) {
        dcmp_expression_print(expression, simple);
        if (expression->linkage.next != &conjunction->expressions) {
            if (simple) {
                printf("&&");
            } else {
                printf(" and ");
            }
        }
    }
    if (simple) {
        printf(")");
    }
}

/* if matched return 1, else return 0 */
static int dcmp_conjunction_match(
    struct dcmp_conjunction *conjunction,
    strmap* map,
    const char* key)
{
    struct dcmp_expression* expression;
    int matched;

    list_for_each_entry(expression,
                        &conjunction->expressions,
                        linkage) {
        matched = dcmp_expression_match(expression, map, key);
        if (!matched) {
            return 0;
        }
    }
    return 1;
}

static struct dcmp_disjunction* dcmp_disjunction_alloc(void)
{
    struct dcmp_disjunction *disjunction;

    disjunction = (struct dcmp_disjunction*)
        MFU_MALLOC(sizeof(struct dcmp_disjunction));
    INIT_LIST_HEAD(&disjunction->linkage);
    INIT_LIST_HEAD(&disjunction->conjunctions);
    disjunction->count = 0;

    return disjunction;
}

static void dcmp_disjunction_add_conjunction(
    struct dcmp_disjunction* disjunction,
    struct dcmp_conjunction* conjunction)
{
    assert(list_empty(&conjunction->linkage));
    list_add_tail(&conjunction->linkage, &disjunction->conjunctions);
    disjunction->count++;
}

static void dcmp_disjunction_free(struct dcmp_disjunction* disjunction)
{
    struct dcmp_conjunction *conjunction;
    struct dcmp_conjunction *n;

    assert(list_empty(&disjunction->linkage));
    list_for_each_entry_safe(conjunction,
                             n,
                             &disjunction->conjunctions,
                             linkage) {
        list_del_init(&conjunction->linkage);
        dcmp_conjunction_free(conjunction);
    }
    assert(list_empty(&disjunction->conjunctions));
    mfu_free(&disjunction);
}

static void dcmp_disjunction_print(
    struct dcmp_disjunction* disjunction,
    int simple,
    int indent)
{
    struct dcmp_conjunction *conjunction;

    list_for_each_entry(conjunction,
                        &disjunction->conjunctions,
                        linkage) {
        dcmp_conjunction_print(conjunction, simple);

        int size_src_matched = mfu_flist_global_size(conjunction->src_matched_list);
        int size_dst_matched = mfu_flist_global_size(conjunction->dst_matched_list);

        /* if src and dst don't match src and dest numbers need to
         * be reported separately */
        if (size_src_matched == size_dst_matched) {
            printf(": %lu (Src: %lu Dest: %lu)", size_src_matched,
                   size_src_matched, size_dst_matched);
        } else {
            printf(": N/A (Src: %lu Dest: %lu)", size_src_matched,
                   size_dst_matched);
        }

        if (conjunction->linkage.next != &disjunction->conjunctions) {

            if (simple) {
                printf("||");
            } else {
                printf(", or\n");
                int i;
                for (i = 0; i < indent; i++) {
                    printf(" ");
                }
            }
        }
    }
}

/* if matched return 1, else return 0 */
static int dcmp_disjunction_match(
    struct dcmp_disjunction* disjunction,
    strmap* map,
    const char* key,
    int is_src)
{
    struct dcmp_conjunction *conjunction;
    int matched;

    list_for_each_entry(conjunction,
                        &disjunction->conjunctions,
                        linkage) {
        matched = dcmp_conjunction_match(conjunction, map, key);
        if (matched) {
            if (is_src)
                mfu_flist_increase(&conjunction->src_matched_list);
            else
                mfu_flist_increase(&conjunction->dst_matched_list);
            return 1;
        }
    }
    return 0;
}

static struct dcmp_output* dcmp_output_alloc(void)
{
    struct dcmp_output* output;

    output = (struct dcmp_output*) MFU_MALLOC(sizeof(struct dcmp_output));
    output->file_name = NULL;
    INIT_LIST_HEAD(&output->linkage);
    output->disjunction = NULL;

    return output;
}

static void dcmp_output_init_disjunction(
    struct dcmp_output* output,
    struct dcmp_disjunction* disjunction)
{
    assert(output->disjunction == NULL);
    output->disjunction = disjunction;
}

static void dcmp_output_free(struct dcmp_output* output)
{
    assert(list_empty(&output->linkage));
    if (output->disjunction != NULL) {
        dcmp_disjunction_free(output->disjunction);
        output->disjunction = NULL;
    }
    if (output->file_name != NULL) {
        mfu_free(&output->file_name);
    }
    mfu_free(&output);
}

void dcmp_option_fini(struct list_head *outputs)
{
    struct dcmp_output* output;
    struct dcmp_output* n;

    list_for_each_entry_safe(output,
                             n,
                             outputs,
                             linkage) {
        list_del_init(&output->linkage);
        dcmp_output_free(output);
    }
    assert(list_empty(outputs));
}

static void dcmp_option_add_output(struct dcmp_output *output, int add_at_head, struct list_head *outputs)
{
    assert(list_empty(&output->linkage));
    if (add_at_head) {
        list_add(&output->linkage, outputs);
    } else {
        list_add_tail(&output->linkage, outputs);
    }
}

static void dcmp_option_add_comparison(dcmp_field field,
    struct mfu_need_compare *compare_flags)
{
    uint64_t depend = dcmp_field_depend[field];
    uint64_t i;
    for (i = 0; i < DCMPF_MAX; i++) {
        if ((depend & ((uint64_t)1 << i)) != (uint64_t)0) {
            compare_flags->need_compare[i] = 1;
        }
    }
}

static int dcmp_output_flist_match(
    struct dcmp_output *output,
    strmap* map,
    mfu_flist flist,
    mfu_flist new_flist,
    mfu_flist *matched_flist,
    int is_src)
{
    const strmap_node* node;
    struct dcmp_conjunction *conjunction;

    /* iterate over each item in map */
    strmap_foreach(map, node) {
        /* get file name */
        const char* key = strmap_node_key(node);

        /* get index of file */
        uint64_t idx;
        int ret = dcmp_strmap_item_index(map, key, &idx);
        assert(ret == 0);

        if (dcmp_disjunction_match(output->disjunction, map, key, is_src)) {
            mfu_flist_increase(matched_flist);
            mfu_flist_file_copy(flist, idx, new_flist);
        }
    }

    list_for_each_entry(conjunction,
                        &output->disjunction->conjunctions,
                        linkage) {
        if (is_src) {
            mfu_flist_summarize(conjunction->src_matched_list);
        } else {
            mfu_flist_summarize(conjunction->dst_matched_list);
        }
    }

    return 0;
}

#define DCMP_OUTPUT_PREFIX "Number of items that "

static int dcmp_output_write(
    struct dcmp_output *output,
    mfu_flist src_flist,
    strmap* src_map,
    mfu_flist dst_flist,
    strmap* dst_map,
    int output_cache)
{
    int ret = 0;
    mfu_flist new_flist = mfu_flist_subset(src_flist);

    /* find matched file in source map */
    mfu_flist src_matched = mfu_flist_new();
    ret = dcmp_output_flist_match(output, src_map, src_flist,
                                  new_flist, &src_matched, 1);
    assert(ret == 0);

    /* find matched file in dest map */
    mfu_flist dst_matched = mfu_flist_new();
    ret = dcmp_output_flist_match(output, dst_map, dst_flist,
                                  new_flist, &dst_matched, 0);
    assert(ret == 0);

    mfu_flist_summarize(new_flist);
    mfu_flist_summarize(src_matched);
    mfu_flist_summarize(dst_matched);
    if (output->file_name != NULL) {
        if (output_cache) {
            mfu_flist_write_cache(output->file_name, new_flist);
        } else {
            mfu_flist_write_text(output->file_name, new_flist);
        }
    }

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    if (rank == 0) {
        printf(DCMP_OUTPUT_PREFIX);
        dcmp_disjunction_print(output->disjunction, 0,
                               strlen(DCMP_OUTPUT_PREFIX));

        if (output->disjunction->count > 1)
            printf(", total number: %lu/%lu",
                   mfu_flist_global_size(src_matched),
                   mfu_flist_global_size(dst_matched));

        if (output->file_name != NULL) {
            printf(", dumped to \"%s\"",
                   output->file_name);
        }
        printf("\n");
    }
    mfu_flist_free(&new_flist);
    mfu_flist_free(&src_matched);
    mfu_flist_free(&dst_matched);

    return 0;
}

int dcmp_outputs_write(
    mfu_flist src_list,
    strmap* src_map,
    mfu_flist dst_list,
    strmap* dst_map,
    int output_cache,
    struct list_head *outputs)
{
    struct dcmp_output* output;
    int ret = 0;

    list_for_each_entry(output,
                        outputs,
                        linkage) {
        ret = dcmp_output_write(output, src_list, src_map, dst_list, dst_map, output_cache);
        if (ret) {
            fprintf(stderr,
                "failed to output to file \"%s\"\n",
                output->file_name);
            break;
        }
    }
    return ret;
}

#define DCMP_PATH_DELIMITER        ":"
#define DCMP_DISJUNCTION_DELIMITER ","
#define DCMP_CONJUNCTION_DELIMITER "@"
#define DCMP_EXPRESSION_DELIMITER  "="

static int dcmp_expression_parse(
    struct dcmp_conjunction* conjunction,
    const char* expression_string,
    struct mfu_need_compare *compare_flags)
{
    char* tmp = MFU_STRDUP(expression_string);
    char* field_string;
    char* state_string;
    int ret = 0;
    struct dcmp_expression* expression;

    expression = dcmp_expression_alloc();

    state_string = tmp;
    field_string = strsep(&state_string, DCMP_EXPRESSION_DELIMITER);
    if (!*field_string || state_string == NULL || !*state_string) {
        fprintf(stderr,
            "expression %s illegal, field \"%s\", state \"%s\"\n",
            expression_string, field_string, state_string);
        ret = -EINVAL;
        goto out;
    }

    ret = dcmp_field_from_string(field_string, &expression->field);
    if (ret) {
        fprintf(stderr,
            "field \"%s\" illegal\n",
            field_string);
        ret = -EINVAL;
        goto out;
    }

    ret = dcmp_state_from_string(state_string, &expression->state);
    if (ret || expression->state == DCMPS_INIT) {
        fprintf(stderr,
            "state \"%s\" illegal\n",
            state_string);
        ret = -EINVAL;
        goto out;
    }

    if ((expression->state == DCMPS_ONLY_SRC ||
         expression->state == DCMPS_ONLY_DEST) &&
        (expression->field != DCMPF_EXIST)) {
        fprintf(stderr,
            "ONLY_SRC or ONLY_DEST is only valid for EXIST\n");
        ret = -EINVAL;
        goto out;
    }

    dcmp_conjunction_add_expression(conjunction, expression);

    /* Add comparison we need for this expression */
    dcmp_option_add_comparison(expression->field, compare_flags);
out:
    if (ret) {
        dcmp_expression_free(expression);
    }
    mfu_free(&tmp);
    return ret;
}

static int dcmp_conjunction_parse(
    struct dcmp_disjunction* disjunction,
    const char* conjunction_string,
    struct mfu_need_compare *compare_flags)
{
    int ret = 0;
    char* tmp = MFU_STRDUP(conjunction_string);
    char* expression;
    char* next;
    struct dcmp_conjunction* conjunction;

    conjunction = dcmp_conjunction_alloc();

    next = tmp;
    while ((expression = strsep(&next, DCMP_CONJUNCTION_DELIMITER))) {
        if (!*expression) {
            /* empty */
            continue;
        }

        ret = dcmp_expression_parse(conjunction, expression, compare_flags);
        if (ret) {
            fprintf(stderr,
                "failed to parse expression \"%s\"\n", expression);
            goto out;
        }
    }

    dcmp_disjunction_add_conjunction(disjunction, conjunction);
out:
    if (ret) {
        dcmp_conjunction_free(conjunction);
    }
    mfu_free(&tmp);
    return ret;
}

static int dcmp_disjunction_parse(
    struct dcmp_output *output,
    const char *disjunction_string,
    struct mfu_need_compare *compare_flags)
{
    int ret = 0;
    char* tmp = MFU_STRDUP(disjunction_string);
    char* conjunction = NULL;
    char* next;
    struct dcmp_disjunction* disjunction;

    disjunction = dcmp_disjunction_alloc();

    next = tmp;
    while ((conjunction = strsep(&next, DCMP_DISJUNCTION_DELIMITER))) {
        if (!*conjunction) {
            /* empty */
            continue;
        }

        ret = dcmp_conjunction_parse(disjunction, conjunction, compare_flags);
        if (ret) {
            fprintf(stderr,
                "failed to parse conjunction \"%s\"\n", conjunction);
            goto out;
        }
    }

    dcmp_output_init_disjunction(output, disjunction);
out:
    if (ret) {
        dcmp_disjunction_free(disjunction);
    }
    mfu_free(&tmp);
    return ret;
}

int dcmp_option_output_parse(const char *option, int add_at_head,
    struct list_head *outputs, struct mfu_need_compare *compare_flags)
{
    char* tmp = MFU_STRDUP(option);
    char* disjunction;
    char* file_name;
    int ret = 0;
    struct dcmp_output* output;

    output = dcmp_output_alloc();

    file_name = tmp;
    disjunction = strsep(&file_name, DCMP_PATH_DELIMITER);
    if (!*disjunction) {
        fprintf(stderr,
            "output string illegal, disjunction \"%s\", file name \"%s\"\n",
            disjunction, file_name);
        ret = -EINVAL;
        goto out;
    }

    ret = dcmp_disjunction_parse(output, disjunction, compare_flags);
    if (ret) {
        goto out;
    }

    if (file_name != NULL && *file_name) {
        output->file_name = MFU_STRDUP(file_name);
    }
    dcmp_option_add_output(output, add_at_head, outputs);
out:
    if (ret) {
        dcmp_output_free(output);
    }
    mfu_free(&tmp);
    return ret;
}

