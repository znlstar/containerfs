/*
 * metanode.h - define metanode message protocol
 */

#ifndef METANODE_H
#define METANODE_H

#include "cfs_pub.h"

#define CFS_REQUEST   (0)
#define CFS_RESPONSE  (1)

/* metanode message types */
enum meta_msg_type {
    GET_META_LEADER     = 0x0001,
    META_CREATE_DIR,
    META_STAT,
    META_GET_INODE_INFO,
    META_LIST,
    META_DELETE_DIR,
    META_RENAME,
    META_CREATE_FILE,
    META_DELETE_FILE,
    META_GET_FILE_CHUNK,
    META_ALLOCATE_CHUNK,
    META_SYNC_CHUNK,
    META_MSG_BUTT,
}

/* metanode message header */
struct meta_msg_header {
    __le16  type;    /* message type: meta_msg_type */
    __le16  direction;  /* message direction: CFS_REQUEST or CFS_RESPONSE */
    char    data[0];
} __attribute__ ((packed));


/* get the leader of  metanodes for volume-id*/
struct get_meta_leader_req {
    char    volid[CFS_UUID_LEN+1];
} __attribute__ ((packed));

struct get_meta_leader_ack {
    __le32  ret;/* Note that all 'ret' is int32 */
    __le32  leader_len;
    char    leader[0];
} __attribute__ ((packed));

/* create dir */
struct create_dir_req {
    char    volid[CFS_UUID_LEN+1];
    __le64  pinode;
    __le32  name_len;
    char    name[0];
} __attribute__ ((packed));

struct create_dir_ack {
    __le32  ret;
    __le64  inode;
} __attribute__ ((packed));

/* file stat */
typedef struct create_dir_req stat_req_t;

struct stat_ack {
    __le32  ret;
    __u8    inode_type;/* bool */
    __le64  inode;
} __attribute__ ((packed));

/* get inode info */
typedef struct create_dir_req get_inode_info_req_t;

struct chunk_info {
    __le64  chunk_id;
    __le32  chunk_size;
    __le64  block_group_id;
} __attribute__ ((packed));

struct inode_info {
    __le64  modifi_time;
    __le64  access_time;
    __le32  link;
    __le64  file_size;
    __le32  chunk_num;
    struct chunk_info chun_info[0];/* chunks info of this inode */
} __attribute__ ((packed));

struct get_inode_info_ack {
    __le32  ret;
    __le64  inode;
    /* inode_info must be the last field due to we dont know number of chunk_info[] */
    struct inode_info inode_info;
} __attribute__ ((packed));

/* file list */
struct list_req {
    char    volid[CFS_UUID_LEN+1];
    __le64  pinode;
} __attribute__ ((packed));

struct dirent_info {
    __u8    inode_type;/* bool */
    __le64  inode;
    __le32  name_len;
    char    name[0];
} __attribute__ ((packed));

struct list_ack {
    __le32  ret;
    __le32  dirent_num;
    struct dirent_info dirents[0];
} __attribute__ ((packed));

/* delete dir */
typedef struct create_dir_req delete_dir_req_t;

struct delete_dir_ack {
    __le32  ret;
} __attribute__ ((packed));

/* rename dir */
struct rename_dir_req {
    char    volid[CFS_UUID_LEN+1];
    __le64  old_pinode;
    __le64  new_pinode;
    __le32  old_name_len;
    __le32  new_name_len;
    char    names[0]; /* include old name and new name */
} __attribute__ ((packed));

typedef delete_dir_ack rename_dir_ack_t;

/* create file */
typedef struct create_dir_req create_file_req_t;
typedef struct create_dir_ack create_file_ack_t;

/* delete file */
typedef struct create_dir_req delete_file_req_t;
typedef struct delete_dir_ack delete_file_ack_t;

/* get file chunks */
typedef struct create_dir_req get_file_chunks_req_t;

struct block_info {
    __le64  block_id;
    char    ip[CFS_IP_LEN_MAX+1];
    __le32  port;
    __le32  status;
    __le64  block_group_id;
    char    volid[CFS_UUID_LEN+1];
    __le32  path_len;
    char    path[0];
} __attribute__ ((packed));

struct block_group {
    __le32  block_num;
    struct block_info blocks[0];
} __attribute__ ((packed));

struct chunk_bg_info {
    __le64 chunk_id;
    __le32 chunk_size;
    struct block_group block_group;
} __attribute__ ((packed));

struct get_file_chunks_ack {
    __le32  ret;
    __le64  inode;
    __le32  chunk_num;
    struct chunk_bg_info chunk_bg_infos[0];
} __attribute__ ((packed));

/* alloc chunk */
struct allocate_chunk_req {
    char    volid[CFS_UUID_LEN+1];
} __attribute__ ((packed));

struct allocate_chunk_ack {
    __le32  ret;
    struct chunk_bg_info chunk_bg_info;
} __attribute__ ((packed));

/* sync chunk */
struct sync_chunk_req {
    char    volid[CFS_UUID_LEN+1];
    __le64  parent_inode_id;
    struct chunk_info chunk_info;
    __le64  size;
    __le32  name_len;
    char    name[0];
} __attribute__ ((packed));

struct sync_chunk_ack {
    __le32  ret;
} __attribute__ ((packed));

#endif

