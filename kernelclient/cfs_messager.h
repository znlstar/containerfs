/*
 * cfs_messager.h - CFS message interface between kernel client and CFS Metadata/Data Nodes
 */

#ifndef CFS_MESSAGER_H
#define CFS_MESSAGER_H


#define CFS_MSG_PROTOCOL_METANODE   1 /* server/client */
#define CFS_MSG_PROTOCOL_DATANODE   2 /* server/client */

/* CFS message header */
struct cfs_msg_header {
    __le16 version;   /* version of CFS messager protocol */
    __le16 proto;      /* message protocol */
    __le64 seq;       /* message seq# for this session */
    __le64 tid;       /* transaction id */
    __le32 data_len;  /* bytes of message data */
    __le32 crc;       /* crc of cfs_msg_header */
} __attribute__ ((packed));

/* CFS message : data[0] point to message data */
struct cfs_msg {
    struct  cfs_msg_header header;
    char    data[0];
} __attribute__ ((packed));


#endif
