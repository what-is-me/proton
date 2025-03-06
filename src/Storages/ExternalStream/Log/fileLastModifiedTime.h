#pragma once

#include <Common/Exception.h>

#include <cerrno>
#include <filesystem>
#include <unistd.h>
#include <sys/stat.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_FSTAT;
}

/// Last modified time in milliseconds
inline int64_t lastModifiedTime(const std::filesystem::path & file)
{
    struct stat stat;
    if (::stat(file.c_str(), &stat) < 0)
        throwFromErrnoWithPath("Failed to get last modified time of file", file.string(), ErrorCodes::CANNOT_FSTAT, errno);

#if defined(OS_DARWIN)
    return stat.st_mtimespec.tv_sec * 1000 + stat.st_mtimespec.tv_nsec / 1000000;
#else
    return stat.st_mtim.tv_sec * 1000 + stat.st_mtim.tv_nsec / 1000000;
#endif
}

inline int64_t lastModifiedTime(int fd, const std::string & filename)
{
    struct stat stat;
    if (::fstat(fd, &stat) < 0)
        throwFromErrnoWithPath("Failed to get last modified time of file", filename, ErrorCodes::CANNOT_FSTAT, errno);

#if defined(OS_DARWIN)
    return stat.st_mtimespec.tv_sec * 1000 + stat.st_mtimespec.tv_nsec / 1000000;
#else
    return stat.st_mtim.tv_sec * 1000 + stat.st_mtim.tv_nsec / 1000000;
#endif
}
}
