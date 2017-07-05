struct Inode {
    id: u64,
    name: String,
    path: String,

    refcnt: u64,
}

impl Inode {
    fn New(id: u64, name: String, path: String) -> Inode {
        return Inode {
            id: id,
            name: name,
            path: path,
        };
    }
}
