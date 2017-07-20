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

All requests are serviced on the same thread. So paging in one large
file would block everything else.

Data is always written back to the original filesystem on `flush()`,
so effectively it's a write-through cache.

Data is never evicted from cache even when local filesystem is full.

## TODO

* move caching to background threads
* mechanism to control cache size and eviction

# References

* catfs is designed to work with [goofys](https://github.com/kahing/goofys/)
