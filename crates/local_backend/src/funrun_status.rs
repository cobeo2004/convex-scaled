//! Funrun health visibility: `GET /api/funrun/status` (admin-only JSON) and
//! `GET /funrun/status` (a self-contained HTML page that polls it).

use anyhow::Context;
use axum::{
    extract::State,
    response::{
        Html,
        IntoResponse,
    },
};
use common::http::{
    extract::Json,
    HttpResponseError,
};
use errors::ErrorMetadata;

use crate::{
    admin::must_be_admin,
    authentication::ExtractIdentity,
    LocalAppState,
};

pub async fn api_status(
    State(st): State<LocalAppState>,
    ExtractIdentity(identity): ExtractIdentity,
) -> Result<impl IntoResponse, HttpResponseError> {
    must_be_admin(&identity)?;
    let status = st.funrun_status.as_ref().context(ErrorMetadata::not_found(
        "FunrunDisabled",
        "FUNCTION_RUNNER is not remote",
    ))?;
    Ok(Json(status.json()))
}

pub async fn page() -> Html<&'static str> {
    Html(include_str!("funrun_status.html"))
}
