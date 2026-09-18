use common::document::PendingDocumentUpdate;
use function_runner::FunctionWrites;

pub fn writes_to_proto(w: &FunctionWrites) -> anyhow::Result<pb_funrun::funrun::Writes> {
    Ok(pb_funrun::funrun::Writes {
        updates: w
            .updates
            .iter()
            .cloned()
            .map(pb::common::PendingDocumentUpdate::try_from)
            .collect::<anyhow::Result<_>>()?,
    })
}

pub fn writes_from_proto(p: pb_funrun::funrun::Writes) -> anyhow::Result<FunctionWrites> {
    Ok(FunctionWrites {
        updates: p
            .updates
            .into_iter()
            .map(PendingDocumentUpdate::try_from)
            .collect::<anyhow::Result<_>>()?,
    })
}
