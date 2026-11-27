//! This technology's declaration: the transport guide's "The declaration",
//! `module/core/capability/transport/doc/adding-a-transport.md`.

use xcore::settings::{Applies, Kind, Presence, Setting, Settings};

/// Every setting a Location takes beyond its address, the JSON API's
/// endpoint, `https://storage.googleapis.com` in the cloud. The bearer
/// token is the Location's `secret`, never a setting.
pub const SETTINGS: &Settings = &Settings {
    technology: env!("CARGO_PKG_NAME"),
    settings: &[
        Setting {
            name: "bucket",
            kind: Kind::Text,
            presence: Presence::Required,
            meaning: "The bucket a Location lists and gets from or uploads to.",
            applies: Applies::Both,
        },
        Setting {
            name: "prefix",
            kind: Kind::Text,
            presence: Presence::Optional,
            meaning: "Only the objects whose names start with it are received; every \
                      object when left out.",
            applies: Applies::Receive,
        },
        transport::configured::timeout(
            Applies::Both,
            None,
            "How long an endpoint that stops answering is waited on; unbounded \
             when left out.",
        ),
    ],
};

/// Its schemes, [`transport::Configured::SCHEMES`]: its name (ADR-0011,
/// amendment 2026-11-19).
pub const SCHEMES: &[&str] = &["google-cloud-storage"];
