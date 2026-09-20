//! Deploy-time evaluation over `Execute`: the five `FunctionRunner` deploy
//! methods as one request enum and one return enum, carried as JSON.
use std::collections::{
    BTreeMap,
    BTreeSet,
};

use anyhow::Context;
use common::{
    auth::{
        AuthConfig,
        SerializedAuthConfig,
    },
    bootstrap_model::components::definition::{
        ComponentDefinitionMetadata,
        SerializedComponentDefinitionMetadata,
    },
    components::{
        ComponentDefinitionPath,
        ComponentName,
        Resource,
        SerializedResource,
    },
    runtime::UnixTimestamp,
    schemas::{
        json::DatabaseSchemaJson,
        DatabaseSchema,
    },
    types::{
        EnvVarName,
        EnvVarValue,
    },
};
use model::{
    config::types::ModuleConfig,
    modules::module_versions::{
        AnalyzedModule,
        ModuleSource,
        SerializedAnalyzedModule,
        SourceMap,
    },
    udf_config::types::{
        SerializedUdfConfig,
        UdfConfig,
    },
};
use pb_funrun::funrun as pb;
use serde::{
    Deserialize,
    Serialize,
};
use sync_types::CanonicalizedModulePath;
use udf::EvaluateAppDefinitionsResult;
use value::identifier::Identifier;

#[derive(Clone, Debug, PartialEq)]
pub enum DeployCall {
    Analyze {
        udf_config: UdfConfig,
        modules: BTreeMap<CanonicalizedModulePath, ModuleConfig>,
        environment_variables: BTreeMap<EnvVarName, EnvVarValue>,
    },
    AppDefinitions {
        app_definition: ModuleConfig,
        component_definitions: BTreeMap<ComponentDefinitionPath, ModuleConfig>,
        dependency_graph: BTreeSet<(ComponentDefinitionPath, ComponentDefinitionPath)>,
        user_environment_variables: BTreeMap<EnvVarName, EnvVarValue>,
        system_env_vars: BTreeMap<EnvVarName, EnvVarValue>,
    },
    ComponentInitializer {
        evaluated_definitions: BTreeMap<ComponentDefinitionPath, ComponentDefinitionMetadata>,
        path: ComponentDefinitionPath,
        definition: ModuleConfig,
        args: BTreeMap<Identifier, Resource>,
        name: ComponentName,
    },
    Schema {
        bundle: ModuleSource,
        source_map: Option<SourceMap>,
        rng_seed: [u8; 32],
        unix_timestamp: UnixTimestamp,
    },
    AuthConfig {
        bundle: ModuleSource,
        source_map: Option<SourceMap>,
        environment_variables: BTreeMap<EnvVarName, EnvVarValue>,
        explanation: String,
    },
}

pub enum DeployReturn {
    Analyze(BTreeMap<CanonicalizedModulePath, AnalyzedModule>),
    AppDefinitions(EvaluateAppDefinitionsResult),
    ComponentInitializer(BTreeMap<Identifier, Resource>),
    Schema(DatabaseSchema),
    AuthConfig(AuthConfig),
}

impl DeployReturn {
    pub fn kind_name(&self) -> &'static str {
        match self {
            DeployReturn::Analyze(_) => "analyze",
            DeployReturn::AppDefinitions(_) => "appDefinitions",
            DeployReturn::ComponentInitializer(_) => "componentInitializer",
            DeployReturn::Schema(_) => "schema",
            DeployReturn::AuthConfig(_) => "authConfig",
        }
    }
}

/// `ModuleConfig` has no serde form outside `application`, so define one.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModuleJson {
    path: String,
    source: String,
    source_map: Option<String>,
    environment: String,
}

impl TryFrom<ModuleConfig> for ModuleJson {
    type Error = anyhow::Error;

    fn try_from(m: ModuleConfig) -> anyhow::Result<Self> {
        Ok(Self {
            path: m.path.into(),
            source: m.source.to_string(),
            source_map: m.source_map,
            environment: m.environment.to_string(),
        })
    }
}

impl TryFrom<ModuleJson> for ModuleConfig {
    type Error = anyhow::Error;

    fn try_from(j: ModuleJson) -> anyhow::Result<Self> {
        Ok(Self {
            path: j.path.parse()?,
            source: ModuleSource::new(&j.source),
            source_map: j.source_map,
            environment: j.environment.parse()?,
        })
    }
}

/// Shared by `DeployCall::Analyze::modules` (keyed by
/// `CanonicalizedModulePath`)
/// and `DeployCall::AppDefinitions::component_definitions` (keyed by
/// `ComponentDefinitionPath`).
fn modules_to_json<K: Into<String>>(
    modules: BTreeMap<K, ModuleConfig>,
) -> anyhow::Result<BTreeMap<String, ModuleJson>> {
    modules
        .into_iter()
        .map(|(k, v)| anyhow::Ok((k.into(), ModuleJson::try_from(v)?)))
        .collect()
}

fn modules_from_json<K: std::str::FromStr<Err = anyhow::Error> + Ord>(
    modules: BTreeMap<String, ModuleJson>,
) -> anyhow::Result<BTreeMap<K, ModuleConfig>> {
    modules
        .into_iter()
        .map(|(k, v)| anyhow::Ok((k.parse()?, ModuleConfig::try_from(v)?)))
        .collect()
}

fn env_vars_to_json(vars: BTreeMap<EnvVarName, EnvVarValue>) -> BTreeMap<String, String> {
    vars.into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn env_vars_from_json(
    vars: BTreeMap<String, String>,
) -> anyhow::Result<BTreeMap<EnvVarName, EnvVarValue>> {
    vars.into_iter()
        .map(|(k, v)| anyhow::Ok((k.parse()?, v.parse()?)))
        .collect()
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
enum DeployRequestJson {
    #[serde(rename_all = "camelCase")]
    Analyze {
        udf_config: SerializedUdfConfig,
        modules: BTreeMap<String, ModuleJson>,
        environment_variables: BTreeMap<String, String>,
    },
    #[serde(rename_all = "camelCase")]
    AppDefinitions {
        app_definition: ModuleJson,
        component_definitions: BTreeMap<String, ModuleJson>,
        dependency_graph: Vec<(String, String)>,
        user_environment_variables: BTreeMap<String, String>,
        system_env_vars: BTreeMap<String, String>,
    },
    #[serde(rename_all = "camelCase")]
    ComponentInitializer {
        evaluated_definitions: BTreeMap<String, SerializedComponentDefinitionMetadata>,
        path: String,
        definition: ModuleJson,
        args: BTreeMap<String, SerializedResource>,
        name: String,
    },
    #[serde(rename_all = "camelCase")]
    Schema {
        bundle: String,
        source_map: Option<String>,
        rng_seed: [u8; 32],
        unix_timestamp_nanos: i64,
    },
    #[serde(rename_all = "camelCase")]
    AuthConfig {
        bundle: String,
        source_map: Option<String>,
        environment_variables: BTreeMap<String, String>,
        explanation: String,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "camelCase")]
enum DeployReturnJson {
    Analyze(BTreeMap<String, SerializedAnalyzedModule>),
    AppDefinitions(BTreeMap<String, SerializedComponentDefinitionMetadata>),
    ComponentInitializer(BTreeMap<String, SerializedResource>),
    Schema(DatabaseSchemaJson),
    AuthConfig(SerializedAuthConfig),
}

impl TryFrom<DeployCall> for pb::DeployRequest {
    type Error = anyhow::Error;

    fn try_from(call: DeployCall) -> anyhow::Result<Self> {
        let json = match call {
            DeployCall::Analyze {
                udf_config,
                modules,
                environment_variables,
            } => DeployRequestJson::Analyze {
                udf_config: udf_config.try_into()?,
                modules: modules_to_json(modules)?,
                environment_variables: env_vars_to_json(environment_variables),
            },
            DeployCall::AppDefinitions {
                app_definition,
                component_definitions,
                dependency_graph,
                user_environment_variables,
                system_env_vars,
            } => DeployRequestJson::AppDefinitions {
                app_definition: app_definition.try_into()?,
                component_definitions: modules_to_json(component_definitions)?,
                dependency_graph: dependency_graph
                    .into_iter()
                    .map(|(a, b)| (a.into(), b.into()))
                    .collect(),
                user_environment_variables: env_vars_to_json(user_environment_variables),
                system_env_vars: env_vars_to_json(system_env_vars),
            },
            DeployCall::ComponentInitializer {
                evaluated_definitions,
                path,
                definition,
                args,
                name,
            } => DeployRequestJson::ComponentInitializer {
                evaluated_definitions: evaluated_definitions
                    .into_iter()
                    .map(|(k, v)| anyhow::Ok((k.into(), v.try_into()?)))
                    .collect::<anyhow::Result<_>>()?,
                path: path.into(),
                definition: definition.try_into()?,
                args: args
                    .into_iter()
                    .map(|(k, v)| anyhow::Ok((k.into(), v.try_into()?)))
                    .collect::<anyhow::Result<_>>()?,
                name: name.into(),
            },
            DeployCall::Schema {
                bundle,
                source_map,
                rng_seed,
                unix_timestamp,
            } => DeployRequestJson::Schema {
                bundle: bundle.to_string(),
                source_map,
                rng_seed,
                unix_timestamp_nanos: unix_timestamp
                    .as_nanos()
                    .try_into()
                    .context("unix_timestamp past 2262")?,
            },
            DeployCall::AuthConfig {
                bundle,
                source_map,
                environment_variables,
                explanation,
            } => DeployRequestJson::AuthConfig {
                bundle: bundle.to_string(),
                source_map,
                environment_variables: env_vars_to_json(environment_variables),
                explanation,
            },
        };
        Ok(pb::DeployRequest {
            json: serde_json::to_vec(&json)?,
        })
    }
}

impl TryFrom<pb::DeployRequest> for DeployCall {
    type Error = anyhow::Error;

    fn try_from(request: pb::DeployRequest) -> anyhow::Result<Self> {
        let json: DeployRequestJson = serde_json::from_slice(&request.json)?;
        Ok(match json {
            DeployRequestJson::Analyze {
                udf_config,
                modules,
                environment_variables,
            } => DeployCall::Analyze {
                udf_config: udf_config.try_into()?,
                modules: modules_from_json(modules)?,
                environment_variables: env_vars_from_json(environment_variables)?,
            },
            DeployRequestJson::AppDefinitions {
                app_definition,
                component_definitions,
                dependency_graph,
                user_environment_variables,
                system_env_vars,
            } => DeployCall::AppDefinitions {
                app_definition: app_definition.try_into()?,
                component_definitions: modules_from_json(component_definitions)?,
                dependency_graph: dependency_graph
                    .into_iter()
                    .map(|(a, b)| anyhow::Ok((a.parse()?, b.parse()?)))
                    .collect::<anyhow::Result<_>>()?,
                user_environment_variables: env_vars_from_json(user_environment_variables)?,
                system_env_vars: env_vars_from_json(system_env_vars)?,
            },
            DeployRequestJson::ComponentInitializer {
                evaluated_definitions,
                path,
                definition,
                args,
                name,
            } => DeployCall::ComponentInitializer {
                evaluated_definitions: evaluated_definitions
                    .into_iter()
                    .map(|(k, v)| anyhow::Ok((k.parse()?, v.try_into()?)))
                    .collect::<anyhow::Result<_>>()?,
                path: path.parse()?,
                definition: definition.try_into()?,
                args: args
                    .into_iter()
                    .map(|(k, v)| anyhow::Ok((k.parse()?, v.try_into()?)))
                    .collect::<anyhow::Result<_>>()?,
                name: name.parse()?,
            },
            DeployRequestJson::Schema {
                bundle,
                source_map,
                rng_seed,
                unix_timestamp_nanos,
            } => {
                anyhow::ensure!(
                    unix_timestamp_nanos >= 0,
                    "unix_timestamp before the unix epoch"
                );
                DeployCall::Schema {
                    bundle: ModuleSource::new(&bundle),
                    source_map,
                    rng_seed,
                    unix_timestamp: UnixTimestamp::from_nanos(unix_timestamp_nanos as u64),
                }
            },
            DeployRequestJson::AuthConfig {
                bundle,
                source_map,
                environment_variables,
                explanation,
            } => DeployCall::AuthConfig {
                bundle: ModuleSource::new(&bundle),
                source_map,
                environment_variables: env_vars_from_json(environment_variables)?,
                explanation,
            },
        })
    }
}

impl TryFrom<DeployReturn> for Vec<u8> {
    type Error = anyhow::Error;

    fn try_from(ret: DeployReturn) -> anyhow::Result<Self> {
        let json = match ret {
            DeployReturn::Analyze(modules) => DeployReturnJson::Analyze(
                modules
                    .into_iter()
                    .map(|(k, v)| anyhow::Ok((k.into(), v.try_into()?)))
                    .collect::<anyhow::Result<_>>()?,
            ),
            DeployReturn::AppDefinitions(definitions) => DeployReturnJson::AppDefinitions(
                definitions
                    .into_iter()
                    .map(|(k, v)| anyhow::Ok((k.into(), v.try_into()?)))
                    .collect::<anyhow::Result<_>>()?,
            ),
            DeployReturn::ComponentInitializer(args) => DeployReturnJson::ComponentInitializer(
                args.into_iter()
                    .map(|(k, v)| anyhow::Ok((k.into(), v.try_into()?)))
                    .collect::<anyhow::Result<_>>()?,
            ),
            DeployReturn::Schema(schema) => DeployReturnJson::Schema(schema.try_into()?),
            DeployReturn::AuthConfig(auth_config) => {
                DeployReturnJson::AuthConfig(auth_config.try_into()?)
            },
        };
        Ok(serde_json::to_vec(&json)?)
    }
}

pub fn decode_return(bytes: &[u8]) -> anyhow::Result<DeployReturn> {
    let json: DeployReturnJson = serde_json::from_slice(bytes)?;
    Ok(match json {
        DeployReturnJson::Analyze(modules) => DeployReturn::Analyze(
            modules
                .into_iter()
                .map(|(k, v)| anyhow::Ok((k.parse()?, v.try_into()?)))
                .collect::<anyhow::Result<_>>()?,
        ),
        DeployReturnJson::AppDefinitions(definitions) => DeployReturn::AppDefinitions(
            definitions
                .into_iter()
                .map(|(k, v)| anyhow::Ok((k.parse()?, v.try_into()?)))
                .collect::<anyhow::Result<_>>()?,
        ),
        DeployReturnJson::ComponentInitializer(args) => DeployReturn::ComponentInitializer(
            args.into_iter()
                .map(|(k, v)| anyhow::Ok((k.parse()?, v.try_into()?)))
                .collect::<anyhow::Result<_>>()?,
        ),
        DeployReturnJson::Schema(schema) => DeployReturn::Schema(schema.try_into()?),
        DeployReturnJson::AuthConfig(auth_config) => {
            DeployReturn::AuthConfig(auth_config.try_into()?)
        },
    })
}

#[cfg(test)]
mod tests {
    use std::collections::{
        BTreeMap,
        BTreeSet,
    };

    use common::{
        auth::AuthConfig,
        runtime::UnixTimestamp,
        types::ModuleEnvironment,
    };
    use model::{
        config::types::ModuleConfig,
        modules::module_versions::ModuleSource,
    };

    use super::*;

    fn module(path: &str) -> ModuleConfig {
        ModuleConfig {
            path: path.parse().unwrap(),
            source: ModuleSource::new("export default 1;"),
            source_map: Some("{}".to_string()),
            environment: ModuleEnvironment::Isolate,
        }
    }

    #[test]
    fn schema_call_round_trips() -> anyhow::Result<()> {
        let call = DeployCall::Schema {
            bundle: ModuleSource::new("export default {}"),
            source_map: None,
            rng_seed: [7; 32],
            unix_timestamp: UnixTimestamp::from_nanos(1_700_000_000_000_000_000),
        };
        let back = DeployCall::try_from(pb::DeployRequest::try_from(call.clone())?)?;
        assert_eq!(back, call);
        Ok(())
    }

    #[test]
    fn app_definitions_call_round_trips() -> anyhow::Result<()> {
        let call = DeployCall::AppDefinitions {
            app_definition: module("convex.config.js"),
            component_definitions: BTreeMap::new(),
            dependency_graph: BTreeSet::new(),
            user_environment_variables: [("A".parse()?, "b".parse()?)].into(),
            system_env_vars: BTreeMap::new(),
        };
        let back = DeployCall::try_from(pb::DeployRequest::try_from(call.clone())?)?;
        assert_eq!(back, call);
        Ok(())
    }

    #[test]
    fn auth_config_return_round_trips() -> anyhow::Result<()> {
        let ret = DeployReturn::AuthConfig(AuthConfig { providers: vec![] });
        let bytes = Vec::<u8>::try_from(ret)?;
        assert!(
            matches!(decode_return(&bytes)?, DeployReturn::AuthConfig(c) if c.providers.is_empty())
        );
        Ok(())
    }
}
