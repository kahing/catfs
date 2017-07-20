CatFS is a caching filesystem written in Rust.

[![Build Status](https://travis-ci.org/kahing/catfs.svg?branch=master)](https://travis-ci.org/kahing/catfs)

# Overview

Catfs allows you to have cached access to another (possibily remote)
filesystem. Currently it only provides a data cache.

Catfs is ALPHA software. Don't use this if you value your data.

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
