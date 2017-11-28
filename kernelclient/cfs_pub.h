/*
 * cfs_pub.h - CFS Public definition
 */

#ifndef CFS_PUB_H
#define CFS_PUB_H

#define CFS_UUID_LEN    (32)
#define CFS_IP_LEN_MAX  (16) /*255.255.255.255*/


#define dprintk(fmt, ...)             \
    do {                                \
        pr_debug("CFS: %s: " fmt, __func__, ##__VA_ARGS__);  \
    } while (0)

#define iprintk(fmt, ...)             \
    do {                                \
        pr_err("CFS: %s: " fmt, __func__, ##__VA_ARGS__);  \
    } while (0)

#define eprintk(fmt, ...)             \
    do {                                \
        pr_err("CFS: %s: " fmt, __func__, ##__VA_ARGS__);  \
    } while (0)

#endif
