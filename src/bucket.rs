//! The bucket a Receive Location takes objects from, as the capability's
//! one object-store receive and claim reach it (`transport::listed`):
//! listed under the Location's prefix, each object got and deleted in the
//! generation it was listed in (`ifGenerationMatch`), and its claim,
//! `<name>.xmip-claim`, uploaded only where none lies
//! (`ifGenerationMatch=0`, ADR-0024, amendment 2026-11-20).

use std::sync::Arc;

use transport::claim::{Artifact, Claimed, ResourceClaim};
use transport::error::Result;
use transport::listed::{AsListed, Object, Store};

use crate::{Client, GcsTransport, origin};

/// One bucket under one prefix, spoken to through one client.
pub struct Bucket {
    client: Client,
    /// The bucket's own name.
    named: String,
    prefix: String,
}

impl Store for Bucket {
    fn list(&self) -> Result<Vec<Object>> {
        self.client.list(&self.named, &self.prefix)
    }

    fn get(&self, name: &str, generation: &str) -> Result<AsListed<Vec<u8>>> {
        self.client.get(&self.named, name, generation)
    }

    fn delete(&self, name: &str, generation: &str) -> Result<AsListed<()>> {
        self.client.delete(&self.named, name, generation)
    }

    fn create(&self, name: &str, bytes: &[u8]) -> Result<Option<String>> {
        self.client.put_new(&self.named, name, bytes)
    }

    fn origin(&self, name: &str) -> String {
        origin(&self.named, name)
    }
}

impl GcsTransport {
    /// The bucket this transport receives from, through a client of its
    /// own.
    ///
    /// # Errors
    /// Where the endpoint is not an HTTP URL.
    pub fn bucket(&self) -> Result<Arc<dyn Store>> {
        Ok(Arc::new(Bucket {
            client: self.client()?,
            named: self.bucket.clone(),
            prefix: self.prefix.clone(),
        }))
    }
}

/// One object claimed by its address,
/// `google-cloud-storage://bucket/name`, as a receive claims it.
impl ResourceClaim for GcsTransport {
    fn is_available(&self, artifact: &Artifact) -> Result<bool> {
        self.claims.is_available(&*self.bucket()?, artifact)
    }

    fn claim(&self, artifact: &Artifact) -> Result<Claimed> {
        self.claims.claim(&*self.bucket()?, artifact)
    }

    fn release(&self, claimed: Claimed) -> Result<()> {
        self.claims.release(&*self.bucket()?, &claimed)
    }
}
