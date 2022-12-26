#![no_main]
use libfuzzer_sys::fuzz_target;

use nrfs::test::fuzz::v1::Test;

fuzz_target!(|test: Test| {
    test.run();
});
