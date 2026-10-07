#ifndef SOPERATOR_JOB_MOUNT_NAMESPACE_H
#define SOPERATOR_JOB_MOUNT_NAMESPACE_H

#include <errno.h>
#include <limits.h>
#include <linux/magic.h>
#include <stdbool.h>
#include <stdio.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <sys/vfs.h>

/* Slurm job namespaces differ from the container mount namespace kept by PID 1.
 * Call before creating a private mount namespace for the jail. */
static inline bool soperator_in_job_mount_namespace(void)
{
    struct stat current_namespace;
    struct stat service_namespace;

    if (stat("/proc/self/ns/mnt", &current_namespace) != 0 ||
        stat("/proc/1/ns/mnt", &service_namespace) != 0) {
        return false;
    }
    return current_namespace.st_dev != service_namespace.st_dev ||
        current_namespace.st_ino != service_namespace.st_ino;
}

/* Call only after copying the job mount namespace and disabling propagation
 * back to it. Slurmd uses the canonical paths; jailed tasks need aliases of
 * the same filesystems before pivot_root hides the outer root. */
static inline int soperator_bind_job_tmpfs(const char *jail_path, const char **failed_source)
{
    static const char *const sources[] = {"/mnt/memory", "/dev/shm", "/tmp"};

    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        char target[PATH_MAX];
        struct statfs filesystem;
        int written;

        *failed_source = sources[i];
        if (statfs(sources[i], &filesystem) != 0) {
            return -1;
        }
        if (filesystem.f_type != TMPFS_MAGIC) {
            errno = EINVAL;
            return -1;
        }
        written = snprintf(target, sizeof(target), "%s%s", jail_path, sources[i]);
        if (written < 0 || (size_t)written >= sizeof(target)) {
            errno = ENAMETOOLONG;
            return -1;
        }
        if (mount(sources[i], target, NULL, MS_BIND, NULL) != 0) {
            return -1;
        }
    }

    *failed_source = NULL;
    return 0;
}

#endif
