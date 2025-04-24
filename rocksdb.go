/*
Package jurocksdb
不同的平台需要安装对应的库，参考这个官方文档：https://github.com/facebook/rocksdb/blob/main/INSTALL.md
Windows 平台使用 msys2 mingw64 编译，代码中的库是针对这个编译器。

这个项目只在 Ubuntu 22.04.1 和 Windows 下编译，其它平台需要自行编译/下载/安装对应的 rocksdb 以及依赖库。
-L 指定库目录，如果没有 -L 则从环境变量加载库目录，windows下集成了需要的静态库，Linux下只集成了 librocksdb 的静态库，其它库从环境加载，需要提前安装。

Windows 平台对应的 rocksdb 信息：
Base Package: mingw-w64-rocksdb
Description: Embedded key-value store for fast storage (mingw-w64)
Homepage: https://rocksdb.org/
Repository: https://github.com/facebook/rocksdb/
Documentation: https://rocksdb.org/docs/
License(s): Apache-2.0 OR GPL-2.0-or-later
Version: 9.10.0-1
Installation: pacman -S mingw-w64-x86_64-rocksdb
Build Date:2025-01-09 20:13:21

Linux平台使用的 librocks.a 是从使用最新的 v10.0.1(03/05/2025) 编译的
*/
package jurocksdb

import "C"
