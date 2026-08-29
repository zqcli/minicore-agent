use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use minicore_runtime::model::ReasoningPreference;

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalMode {
    Auto,
    #[default]
    Ask,
    ReadOnly,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProfileCompaction {
    #[default]
    Disabled,
    Model {
        trigger_tokens: u64,
        target_tokens: u64,
    },
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub model: String,
    #[serde(default)]
    pub reasoning: ReasoningPreference,
    pub system_prompt: String,
    #[serde(default)]
    pub tools: Vec<String>,
    #[serde(default = "default_tool_rounds")]
    pub max_tool_rounds: u16,
    #[serde(default)]
    pub approval: ApprovalMode,
    #[serde(default)]
    pub compaction: ProfileCompaction,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct ProfileInfo {
    pub(crate) id: String,
    pub(crate) model: String,
    pub(crate) reasoning: ReasoningPreference,
    pub(crate) tools: Vec<String>,
    pub(crate) approval: ApprovalMode,
}

fn default_tool_rounds() -> u16 {
    32
}

pub(crate) struct Profiles {
    values: BTreeMap<String, Profile>,
}

impl Profiles {
    pub(crate) fn from_values(values: BTreeMap<String, Profile>) -> Self {
        Self { values }
    }

    pub(crate) fn get(&self, id: &str) -> Option<&Profile> {
        self.values.get(id)
    }

    pub(crate) fn list(&self) -> Vec<ProfileInfo> {
        self.values
            .iter()
            .map(|(id, profile)| ProfileInfo {
                id: id.clone(),
                model: profile.model.clone(),
                reasoning: profile.reasoning,
                tools: profile.tools.clone(),
                approval: profile.approval,
            })
            .collect()
    }
}
