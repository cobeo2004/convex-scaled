use common::types::UdfType;
use tonic::Code;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureStage {
    /// Nothing can have run yet. See `failure_stage`.
    BeforeStart,
    /// The function may have run.
    AfterStart,
}

/// Whether a failed attempt may run again. `attempt` counts from 0.
pub fn may_retry(
    udf_type: UdfType,
    stage: FailureStage,
    attempt: usize,
    max_retries: usize,
) -> bool {
    if attempt >= max_retries {
        return false;
    }
    match stage {
        FailureStage::BeforeStart => true,
        // Nothing commits until the conductor commits, so Q/M are safe.
        // Actions may have caused side effects.
        FailureStage::AfterStart => match udf_type {
            UdfType::Query | UdfType::Mutation => true,
            UdfType::Action | UdfType::HttpAction => false,
        },
    }
}

/// How far the RunRequest got when an attempt failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Delivery {
    /// The RunRequest was never handed to the call.
    NotSent,
    /// The worker answered `Overloaded` instead of starting.
    Refused,
    /// Sent, then the call failed before the worker's `Started` frame.
    LostBeforeStarted,
    /// Sent, then the call failed after `Started`.
    LostAfterStarted,
}

/// Where an attempt failed, for `may_retry`.
///
/// Actions count as started once the RunRequest was sent: the isolate may
/// run user code before the worker's `Started` frame arrives, so only a
/// failed call or an explicit `Overloaded` (sent instead of starting) proves
/// nothing ran.
pub fn failure_stage(udf_type: UdfType, delivery: Delivery) -> FailureStage {
    match delivery {
        Delivery::NotSent | Delivery::Refused => FailureStage::BeforeStart,
        Delivery::LostAfterStarted => FailureStage::AfterStart,
        Delivery::LostBeforeStarted => match udf_type {
            UdfType::Query | UdfType::Mutation => FailureStage::BeforeStart,
            UdfType::Action | UdfType::HttpAction => FailureStage::AfterStart,
        },
    }
}

/// Whether a gRPC error looks like a lost connection (retryable) rather than
/// the worker's deterministic answer (returned as is).
pub fn is_transport_failure(status: &tonic::Status) -> bool {
    // Statuses built by `Status::from_anyhow` from an `ErrorMetadata` carry it
    // in `details`; transport errors have none.
    if !status.details().is_empty() {
        return false;
    }
    match status.code() {
        Code::Unavailable | Code::Unknown | Code::Cancelled => true,
        Code::Ok
        | Code::InvalidArgument
        | Code::DeadlineExceeded
        | Code::NotFound
        | Code::AlreadyExists
        | Code::PermissionDenied
        | Code::ResourceExhausted
        | Code::FailedPrecondition
        | Code::Aborted
        | Code::OutOfRange
        | Code::Unimplemented
        | Code::Internal
        | Code::DataLoss
        | Code::Unauthenticated => false,
    }
}

#[cfg(test)]
mod tests {
    use common::types::UdfType;
    use errors::ErrorMetadata;
    use pb::error_metadata::ErrorMetadataStatusExt;
    use tonic::Status;

    use super::*;

    #[test]
    fn before_start_retries_everything() {
        for t in [
            UdfType::Query,
            UdfType::Mutation,
            UdfType::Action,
            UdfType::HttpAction,
        ] {
            assert!(may_retry(t, FailureStage::BeforeStart, 0, 4));
        }
    }

    #[test]
    fn after_start_only_queries_and_mutations() {
        assert!(may_retry(UdfType::Query, FailureStage::AfterStart, 0, 4));
        assert!(may_retry(UdfType::Mutation, FailureStage::AfterStart, 0, 4));
        assert!(!may_retry(UdfType::Action, FailureStage::AfterStart, 0, 4));
        assert!(!may_retry(
            UdfType::HttpAction,
            FailureStage::AfterStart,
            0,
            4
        ));
    }

    #[test]
    fn failure_before_request_sent_is_before_start_for_all() {
        for t in [
            UdfType::Query,
            UdfType::Mutation,
            UdfType::Action,
            UdfType::HttpAction,
        ] {
            assert_eq!(
                failure_stage(t, Delivery::NotSent),
                FailureStage::BeforeStart
            );
        }
    }

    #[test]
    fn overloaded_frame_is_before_start_for_all() {
        for t in [
            UdfType::Query,
            UdfType::Mutation,
            UdfType::Action,
            UdfType::HttpAction,
        ] {
            assert_eq!(
                failure_stage(t, Delivery::Refused),
                FailureStage::BeforeStart
            );
        }
    }

    #[test]
    fn actions_after_request_sent_are_after_start_even_without_started() {
        for t in [UdfType::Action, UdfType::HttpAction] {
            assert_eq!(
                failure_stage(t, Delivery::LostBeforeStarted),
                FailureStage::AfterStart
            );
            assert_eq!(
                failure_stage(t, Delivery::LostAfterStarted),
                FailureStage::AfterStart
            );
        }
    }

    #[test]
    fn queries_and_mutations_follow_started() {
        for t in [UdfType::Query, UdfType::Mutation] {
            assert_eq!(
                failure_stage(t, Delivery::LostBeforeStarted),
                FailureStage::BeforeStart
            );
            assert_eq!(
                failure_stage(t, Delivery::LostAfterStarted),
                FailureStage::AfterStart
            );
        }
    }

    #[test]
    fn only_transport_statuses_are_retryable() {
        for code in [Code::Unavailable, Code::Unknown, Code::Cancelled] {
            assert!(is_transport_failure(&Status::new(code, "lost")), "{code:?}");
        }
        for code in [
            Code::InvalidArgument,
            Code::FailedPrecondition,
            Code::Unauthenticated,
            Code::PermissionDenied,
            Code::ResourceExhausted,
            Code::Internal,
            Code::DeadlineExceeded,
        ] {
            assert!(!is_transport_failure(&Status::new(code, "no")), "{code:?}");
        }
    }

    #[test]
    fn status_with_error_metadata_is_not_retryable() {
        let error = anyhow::anyhow!(ErrorMetadata::overloaded("Busy", "busy"));
        let status = Status::from_anyhow(error);
        assert!(!is_transport_failure(&status));
        // Even with a transport-class code.
        let status = Status::with_details(Code::Unavailable, "x", status.details().to_vec().into());
        assert!(!is_transport_failure(&status));
    }

    #[test]
    fn budget_is_enforced() {
        assert!(!may_retry(UdfType::Query, FailureStage::BeforeStart, 4, 4));
    }
}
