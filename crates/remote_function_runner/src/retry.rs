use common::types::UdfType;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureStage {
    /// Connect failure or Overloaded before the worker sent `Started`.
    BeforeStart,
    /// Stream lost after `Started`.
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

#[cfg(test)]
mod tests {
    use common::types::UdfType;

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
    fn budget_is_enforced() {
        assert!(!may_retry(UdfType::Query, FailureStage::BeforeStart, 4, 4));
    }
}
