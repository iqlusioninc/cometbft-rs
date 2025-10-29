//! Event routing for subscriptions.

use alloc::collections::{BTreeMap as HashMap, BTreeSet as HashSet};

use tracing::debug;

use crate::{client::subscription::SubscriptionTx, error::Error, event::Event, prelude::*};

pub type SubscriptionQuery = String;
pub type SubscriptionId = String;

#[cfg_attr(not(feature = "websocket-client"), allow(dead_code))]
pub type SubscriptionIdRef<'a> = &'a str;

/// Provides a mechanism for tracking [`Subscription`]s and routing [`Event`]s
/// to those subscriptions.
///
/// [`Subscription`]: struct.Subscription.html
/// [`Event`]: ./event/struct.Event.html
#[derive(Debug, Default)]
pub struct SubscriptionRouter {
    /// A map of subscription queries to collections of subscription IDs and
    /// their result channels. Used for publishing events relating to a specific
    /// query.
    subscriptions: HashMap<SubscriptionQuery, HashMap<SubscriptionId, SubscriptionTx>>,
}

impl SubscriptionRouter {
    /// Publishes the given error to all of the subscriptions to which the
    /// error is relevant, based on the given subscription id query.
    #[cfg_attr(not(feature = "websocket-client"), allow(dead_code))]
    pub fn publish_error(&mut self, id: SubscriptionIdRef<'_>, err: Error) -> PublishResult {
        if let Some(query) = self.subscription_query(id).cloned() {
            self.publish(query, Err(err))
        } else {
            PublishResult::NoSubscribers
        }
    }

    /// Get the query associated with the given subscription.
    #[cfg_attr(not(feature = "websocket-client"), allow(dead_code))]
    fn subscription_query(&self, id: SubscriptionIdRef<'_>) -> Option<&SubscriptionQuery> {
        for (query, subs) in &self.subscriptions {
            if subs.contains_key(id) {
                return Some(query);
            }
        }

        None
    }

    /// Publishes the given event to all of the subscriptions to which the
    /// event is relevant, based on the associated query.
    #[cfg_attr(not(feature = "websocket-client"), allow(dead_code))]
    pub fn publish_event(&mut self, ev: Event) -> PublishResult {
        self.publish(ev.query.clone(), Ok(ev))
    }

    /// Publishes the given event/error to all of the subscriptions to which the
    /// event/error is relevant, based on the given query.
    pub fn publish(&mut self, query: SubscriptionQuery, ev: Result<Event, Error>) -> PublishResult {
        let subs_for_query = match self.subscriptions.get_mut(&query) {
            Some(s) => s,
            None => return PublishResult::NoSubscribers,
        };

        // We assume here that any failure to publish an event is an indication
        // that the receiver end of the channel has been dropped, which allows
        // us to safely stop tracking the subscription.
        let mut disconnected = HashSet::new();
        for (id, event_tx) in subs_for_query.iter_mut() {
            if let Err(e) = event_tx.send(ev.clone()) {
                disconnected.insert(id.clone());
                debug!(
                    "Automatically disconnecting subscription with ID {} for query \"{}\" due to failure to publish to it: {}",
                    id, query, e
                );
            }
        }

        for id in disconnected {
            subs_for_query.remove(&id);
        }

        if subs_for_query.is_empty() {
            PublishResult::AllDisconnected(query)
        } else {
            PublishResult::Success
        }
    }

    /// Immediately add a new subscription to the router without waiting for
    /// confirmation.
    #[allow(dead_code)]
    pub fn add(&mut self, id: impl ToString, query: impl ToString, tx: SubscriptionTx) {
        let query = query.to_string();
        let subs_for_query = match self.subscriptions.get_mut(&query) {
            Some(s) => s,
            None => {
                self.subscriptions.insert(query.clone(), HashMap::new());
                self.subscriptions.get_mut(&query).unwrap()
            },
        };

        subs_for_query.insert(id.to_string(), tx);
    }

    /// Removes all the subscriptions relating to the given query.
    #[allow(dead_code)]
    pub fn remove_by_query(&mut self, query: impl ToString) -> usize {
        self.subscriptions
            .remove(&query.to_string())
            .map(|subs_for_query| subs_for_query.len())
            .unwrap_or(0)
    }
}

#[cfg(feature = "websocket-client")]
impl SubscriptionRouter {
    /// Returns the number of active subscriptions for the given query.
    pub fn num_subscriptions_for_query(&self, query: impl ToString) -> usize {
        self.subscriptions
            .get(&query.to_string())
            .map(|subs_for_query| subs_for_query.len())
            .unwrap_or(0)
    }
}

#[derive(Debug, Clone)]
pub enum PublishResult {
    Success,
    NoSubscribers,
    // All subscriptions for the given query have disconnected.
    #[cfg_attr(not(feature = "websocket-client"), allow(dead_code))]
    AllDisconnected(String),
}
