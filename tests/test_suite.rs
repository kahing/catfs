// copied from https://users.rust-lang.org/t/why-does-rust-test-framework-lack-fixtures-and-mocking/5622/21
macro_rules! unit_tests {
    ($( fn $name:ident($fixt:ident : &$ftype:ty) $body:block )*) => (
        $(
            #[test]
            fn $name() {
                match <$ftype as Fixture>::setup() {
                    Ok($fixt) => {
                        $body
                        if let Err(e) = $fixt.teardown() {
                            panic!("teardown failed: {}", e);
                        }
                    },
                    Err(e) => panic!("setup failed: {}", e),
                }
            }
        )*
    )
}
