// @generated - do not modify. Modify build.rs instead.
#![allow(clippy::match_single_binding)]
pub mod funrun {
    include!(concat!(env!("OUT_DIR"), "/funrun.rs"));
}

include!(concat!(env!("OUT_DIR"), "/_extras.rs"));
use std::sync::LazyLock;

use prost_reflect::DescriptorPool;

const FILE_DESCRIPTOR_BYTES: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/descriptors.bin"));
pub static DESCRIPTOR_POOL: LazyLock<DescriptorPool> =
    LazyLock::new(|| DescriptorPool::decode(FILE_DESCRIPTOR_BYTES).unwrap());
