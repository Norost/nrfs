# Nora's Reliable FileSystem

## Features

* Maximum volume size of `2^64 - 1` blocks
  * With a maximum block size of `2^24`, maximum volume size is `2^88 - 2^24`
    bytes
* Maximum object size of `2^64 - 1` bytes
* Maximum object count of `2^59`
* Error detection
* Error correction (with mirrors only!)
* Compression
* Encryption
* Transactional updates
* Mirroring
* Sparse objects
* Up to 2^32 entries per directory, indexed using a hashmap with DoS resistance.
* File names up to 255 bytes long.
* Arbitrary key-value pairs.
  * Keys up to 255 bytes long.
  * Arbitrary-length values.
  * At most 64KiB of attribute data per item.
* Embedding small files inside directories.

## How to use

### Compile

[You need a Rust toolchain](https://rustup.rs/)

After installation, run:

```
cargo +nightly b --release
```

To enable parallelism, enable the `parallelism` feature:

```
cargo +nightly b --release --features parallelism
```

### Creating a filesystem

To create a NRFS filesystem, use the `tool` binary:

```
fallocate -l 16M /tmp/nrfs.img
# Without files
./target/release/tool make /tmp/nrfs.img
# With files copied
./target/release/tool make /tmp/nrfs.img -d /directory/to/copy
```

To check if the filesystem was properly created, use `tool dump /tmp/nrfs`:

```
$ ./target/release/tool dump /tmp/nrfs 
rw-r--r-- 1000 1000  2022-08-26T19:32:38.940       513  f build.rs
rw-r--r-- 1000 1000  2022-09-19T20:24:05.331      1146  f Cargo.toml
rwxr-xr-x 1000 1000  2022-09-11T17:27:56.256        12  d src
rw-r--r-- 1000 1000  2022-09-03T05:12:44.828      2630    f main.rs
rwxr-xr-x 1000 1000  2022-09-02T19:20:37.660         5    d memory
rw-r--r-- 1000 1000  2022-09-02T19:20:37.660       394      f virtual.rs
rw-r--r-- 1000 1000  2022-07-08T12:07:42.376       684      f mod.rs
...
```

### Mounting a filesystem

If you use a UNIX-based system (e.g. Linux, macOS, FreeBSD) you can use the FUSE driver.

#### FUSE

Mount with:

```
mkdir /tmp/nrfs
./target/release/fuse /tmp/nrfs.img /tmp/nrfs
```

Unmount with:

```
fusermount -u /tmp/mnt
```

## Crates:

* [`nros`](nros): Object store implementation
* [`nrfs`](nrfs): Filesystem implementation
* [`tool`](tool): Filesystem tool (make, dump ...)
* [`fuse`](fuse): FUSE driver
