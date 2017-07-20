CatFS is a caching filesystem written in Rust.

[![Build Status](https://travis-ci.org/kahing/catfs.svg?branch=master)](https://travis-ci.org/kahing/catfs)

# Overview

Catfs allows you to have cached access to another
filesystem. Currently it only provides a data cache.

Catfs is not currently crash-safe.

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

Catfs is not currently crash-safe. Don't use this if you value your data.
