use metrics::{
    log_counter,
    register_convex_counter,
};

register_convex_counter!(
    FUNRUN_INDEX_PAGE_RPCS_TOTAL,
    "Number of IndexPage RPCs a funrun worker sent to the conductor",
);
pub fn log_index_page_rpc() {
    log_counter(&FUNRUN_INDEX_PAGE_RPCS_TOTAL, 1);
}
