/* enable C++ codes to include this header directly */
#ifdef __cplusplus
extern "C" {
#endif

#ifndef MFU_OUTPUT_H
#define MFU_OUTPUT_H

#include "mfu.h"
#include "strmap.h"
#include "list.h"

typedef enum _dcmp_state {
    /* initial state */
    DCMPS_INIT = 'A',

    /* have common data/metadata */
    DCMPS_COMMON,

    /* have common data/metadata, not valid for DCMPF_EXIST */
    DCMPS_DIFFER,

     /*
      * This file only exist in src directory.
      * Only valid for DCMPF_EXIST.
      */
    DCMPS_ONLY_SRC,

     /*
      * This file only exist in dest directory.
      * Only valid for DCMPF_EXIST.
      * Not used yet,
      * because we don't want to waste a loop in dcmp_strmap_compare()
      */
    DCMPS_ONLY_DEST,

    DCMPS_MAX,
} dcmp_state;

typedef enum _dcmp_field {
    DCMPF_EXIST = 0, /* both have this file */
    DCMPF_TYPE,      /* both are the same type */
    DCMPF_SIZE,      /* both are regular file and have same size */
    DCMPF_UID,       /* both have the same UID */
    DCMPF_GID,       /* both have the same GID */
    DCMPF_ATIME,     /* both have the same atime */
    DCMPF_MTIME,     /* both have the same mtime */
    DCMPF_CTIME,     /* both have the same ctime */
    DCMPF_PERM,      /* both have the same permission */
    DCMPF_ACL,       /* both have the same ACLs */
    DCMPF_CONTENT,   /* both have the same data */
    DCMPF_MAX,
} dcmp_field;

#define DCMPF_EXIST_DEPEND   (1 << DCMPF_EXIST)
#define DCMPF_TYPE_DEPEND    (DCMPF_EXIST_DEPEND | (1 << DCMPF_TYPE))
#define DCMPF_SIZE_DEPEND    (DCMPF_TYPE_DEPEND | (1 << DCMPF_SIZE))
#define DCMPF_UID_DEPEND     (DCMPF_EXIST_DEPEND | (1 << DCMPF_UID))
#define DCMPF_GID_DEPEND     (DCMPF_EXIST_DEPEND | (1 << DCMPF_GID))
#define DCMPF_ATIME_DEPEND   (DCMPF_EXIST_DEPEND | (1 << DCMPF_ATIME))
#define DCMPF_MTIME_DEPEND   (DCMPF_EXIST_DEPEND | (1 << DCMPF_MTIME))
#define DCMPF_CTIME_DEPEND   (DCMPF_EXIST_DEPEND | (1 << DCMPF_CTIME))
#define DCMPF_PERM_DEPEND    (DCMPF_EXIST_DEPEND | (1 << DCMPF_PERM))
#define DCMPF_ACL_DEPEND     (DCMPF_EXIST_DEPEND | (1 << DCMPF_ACL))
#define DCMPF_CONTENT_DEPEND (DCMPF_SIZE_DEPEND | (1 << DCMPF_CONTENT))

struct dcmp_expression {
    dcmp_field field;              /* the concerned field */
    dcmp_state state;              /* expected state of the field */
    struct list_head linkage;      /* linkage to struct dcmp_conjunction */
};

struct dcmp_conjunction {
    struct list_head linkage;      /* linkage to struct dcmp_disjunction */
    struct list_head expressions;  /* list of logical conjunction */
    mfu_flist src_matched_list;    /* matched src items in this conjunction */
    mfu_flist dst_matched_list;    /* matched dst items in this conjunction */
};

struct dcmp_disjunction {
    struct list_head linkage;      /* linkage to struct dcmp_output */
    struct list_head conjunctions; /* list of logical conjunction */
    unsigned count;                /* logical conjunctions count */
};

struct dcmp_output {
    char* file_name;               /* output file name */
    struct list_head linkage;      /* linkage to struct mfu_cmp_options */
    struct dcmp_disjunction *disjunction; /* logical disjunction rules */
};

struct mfu_cmp_options {
    struct list_head outputs;      /* list of outputs */
};

struct mfu_need_compare {
    int need_compare[DCMPF_MAX];   /* fields that need to be compared  */
};

typedef int (*dcmp_compare_t) (
    mfu_flist src_compare_list,
    strmap* src_map,
    mfu_flist dst_compare_list,
    strmap* dst_map,
    size_t strlen_prefix,
    mfu_copy_opts_t* copy_opts,
    mfu_file_t* mfu_src_file,
    mfu_file_t* mfu_dst_file);

void dcmp_strmap_item_update(
    strmap* map,
    const char *key,
    dcmp_field field,
    dcmp_state state);

int dcmp_option_output_parse(const char *option, int add_at_head,
    struct list_head *outputs, struct mfu_need_compare *compare_flags);

void dcmp_option_fini(struct list_head *outputs);

strmap* dcmp_strmap_creat(mfu_flist list, const char* prefix);

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
    dcmp_compare_t comparer);

int dcmp_outputs_write(
    mfu_flist src_list,
    strmap* src_map,
    mfu_flist dst_list,
    strmap* dst_map,
    int output_cache,
    struct list_head *outputs);

void dcmp_strmap_check(
    strmap* src_map,
    strmap* dst_map,
    struct mfu_need_compare *compare_flags);


#endif /* MFU_OUTPUT_H */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif
