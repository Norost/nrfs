#![no_main]
use libfuzzer_sys::fuzz_target;

use nros::test::fuzz::v1::Test;

fuzz_target!(|test: Test| {
    test.run();
});
