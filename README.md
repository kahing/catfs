Catfs is a caching filesystem written in Rust.

[![Build Status](https://travis-ci.org/kahing/catfs.svg?branch=master)](https://travis-ci.org/kahing/catfs)

# Overview

Catfs allows you to have cached access to another (possibily remote)
filesystem. Currently it only provides a data cache.

Catfs is ALPHA software. Don't use this if you value your data.

# Installation

Building catfs requires [Cargo](http://doc.crates.io/).

```ShellSession
:~/catfs$ cargo build
$ # debug binary now in ./target/debug/catf
:~/catfs$ cargo install
$ # optimized binary now in $HOME/.cargo/bin/catfs
```

# Usage

Catfs requires extended attributes (xattr) to be enabled on the
filesystem where files are cached to. Typically this means you need to
have `user_xattr` mount option turned on.

```ShellSession
$ catfs <from> <to> <mountpoint>
```

Catfs will expose files in `<from>` under `<mountpoint>`, and cache
them to `<to>` as they are accessed.

# License

Copyright (C) 2017 Ka-Hing Cheung

Licensed under the Apache License, Version 2.0

# Current Status

Catfs is ALPHA software. Don't use this if you value your data.

Paging in/writeback are done in background threads. All other requests
are serviced on the same thread, so many operations could block each
other.

Data is always written back to the original filesystem on `flush()`,
so effectively it's a write-through cache.

Data is never evicted from cache even when local filesystem is full.

## TODO

* move all operations to background threads
* mechanism to control cache size and eviction

# References

* catfs is designed to work with [goofys](https://github.com/kahing/goofys/)
