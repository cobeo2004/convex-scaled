mod common;

use std::collections::BTreeMap;

use ::common::{
    index::IndexKeyBytes,
    interval::Interval,
    query::{
        CursorPosition,
        Order,
    },
};
use common::*;
use database::TableCountSnapshot;
use errors::ErrorMetadataAnyhowExt;
use funrun_worker::host_client::{
    connect_host,
    EagerTableCounts,
    RemoteActionCallbacks,
    RemoteIndexReader,
};
use indexing::index_reader::IndexReader;
use keybroker::Identity;
use udf::ActionCallbacks;

#[tokio::test]
async fn remote_index_reader_reads_through_host() {
    let host = start_host_with_fakes().await;
    let client = connect_host(&format!("http://{}", host.addr), host.token.clone()).unwrap();
    let reader = RemoteIndexReader::new(client, sample_ts());
    let page = reader
        .index_page(
            sample_index_ref(),
            sample_tablet(),
            &Interval::all(),
            Order::Asc,
            10,
        )
        .await
        .unwrap();
    assert_eq!(page.entries.len(), 3);
    assert_eq!(reader.timestamp(), sample_ts());
}

async fn read_keys_with_one_entry_per_rpc(
    order: Order,
    max_results: usize,
) -> (Vec<u8>, CursorPosition) {
    // One byte: the host sends one entry per response.
    let host = start_host_with_index_page_max_bytes(Some(1)).await;
    let client = connect_host(&format!("http://{}", host.addr), host.token.clone()).unwrap();
    let page = RemoteIndexReader::new(client, sample_ts())
        .index_page(
            sample_index_ref(),
            sample_tablet(),
            &Interval::all(),
            order,
            max_results,
        )
        .await
        .unwrap();
    let keys = page.entries.iter().map(|e| e.key.0[0]).collect();
    (keys, page.cursor)
}

#[tokio::test]
async fn remote_index_reader_refills_pages_cut_by_the_host_byte_budget() {
    assert_eq!(
        read_keys_with_one_entry_per_rpc(Order::Asc, 10).await,
        (vec![1, 2, 3], CursorPosition::End)
    );
    assert_eq!(
        read_keys_with_one_entry_per_rpc(Order::Desc, 2).await,
        (vec![3, 2], CursorPosition::After(IndexKeyBytes(vec![2])))
    );
}

#[tokio::test]
async fn remote_callbacks_execute_mutation() {
    let host = start_host_with_fakes().await;
    let client = connect_host(&format!("http://{}", host.addr), host.token.clone()).unwrap();
    let cb = RemoteActionCallbacks::new(client);
    let res = cb
        .execute_mutation(
            Identity::system(),
            sample_path(),
            sample_args(),
            sample_context(),
        )
        .await
        .unwrap();
    assert!(res.result.is_ok());
}

#[tokio::test]
async fn remote_callback_error_metadata_survives() {
    let host = start_host_with_fakes().await;
    let client = connect_host(&format!("http://{}", host.addr), host.token.clone()).unwrap();
    let cb = RemoteActionCallbacks::new(client);
    let err = cb
        .execute_query(
            Identity::system(),
            sample_path(),
            sample_args(),
            sample_context(),
        )
        .await
        .unwrap_err();
    assert!(err.is_bad_request());
    assert_eq!(err.short_msg(), "FakeBadRequest");
}

#[tokio::test]
async fn eager_table_counts() {
    let t = sample_tablet();
    let counts = EagerTableCounts(Some(BTreeMap::from([(t, 7)])));
    assert_eq!(counts.count(t).await.unwrap(), Some(7));
    let other = value::TabletId(value::InternalId::from([9u8; 16]));
    assert_eq!(counts.count(other).await.unwrap(), Some(0));
    assert_eq!(EagerTableCounts(None).count(t).await.unwrap(), None);
}
