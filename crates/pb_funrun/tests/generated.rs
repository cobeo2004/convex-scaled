use pb_funrun::funrun::{
    execute_down,
    function_host_server::FunctionHostServer,
    funrun_server::FunrunServer,
    ExecuteDown,
    Started,
};
use prost::Message;

#[test]
fn execute_down_round_trips() {
    let msg = ExecuteDown {
        inner: Some(execute_down::Inner::Started(Started {})),
    };
    let bytes = msg.encode_to_vec();
    assert_eq!(ExecuteDown::decode(bytes.as_slice()).unwrap(), msg);
    // Type-level check that both services were generated.
    let _ = std::any::type_name::<FunrunServer<()>>();
    let _ = std::any::type_name::<FunctionHostServer<()>>();
}
