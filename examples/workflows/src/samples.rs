//! The sample workflows the binaries run:
//!
//! - `checkout` reserves stock, charges the account, waits for an approval, waits a while and ships each parcel. A
//!   failed charge releases the stock; a failure or a cancel after the charge refunds it and releases the stock.
//! - `onboarding` sends a welcome mail, sleeps three days (`follow_up_s`), and sends a follow-up.
//! - `batch` starts a child run of `item` for each of `count` items and adds up their results.
//! - `nine_steps` and `one_step` run nine and one no-op activity on the run's node: the step overhead is the
//!   difference.
//! - `sleeper` sleeps `secs` and ends.
//!
//! The activities are stand-ins that answer at once (`Charge` after `ms`): real ones would call other systems, keyed
//! by `ActivityCx::idempotency_key`.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::workflow::{Activity, ActivityCx, App, Ctx, Failure, Retry, days, secs};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Parcel {
    pub to: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Order {
    pub sku: String,
    pub qty: u32,
    /// `declined` fails the charge for good.
    pub account: String,
    pub parcels: Vec<Parcel>,
    #[serde(default)]
    pub ship_after_s: u64,
    /// How long the charge takes, in ms.
    #[serde(default)]
    pub charge_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Hold {
    pub id: String,
    pub cents: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Paid {
    pub charge: String,
    pub cents: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Label {
    pub to: String,
    pub tracking: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Receipt {
    pub paid: Paid,
    pub labels: Vec<Label>,
}

#[derive(Serialize, Deserialize)]
pub struct Reserve {
    pub sku: String,
    pub qty: u32,
}

impl Activity for Reserve {
    const NAME: &'static str = "reserve";
    type Output = Hold;
    async fn run(self, cx: ActivityCx) -> Result<Hold, Failure> {
        Ok(Hold { id: format!("hold-{}", cx.idempotency_key()), cents: u64::from(self.qty) * 250 })
    }
}

#[derive(Serialize, Deserialize)]
pub struct Charge {
    pub account: String,
    pub cents: u64,
    #[serde(default)]
    pub ms: u64,
}

impl Activity for Charge {
    const NAME: &'static str = "charge";
    const QUEUE: &'static str = "payments";
    type Output = Paid;
    async fn run(self, cx: ActivityCx) -> Result<Paid, Failure> {
        if self.account == "declined" {
            return Err(Failure::retry("card_declined: the bank said no"));
        }
        tokio::time::sleep(std::time::Duration::from_millis(self.ms)).await;
        Ok(Paid { charge: format!("ch-{}", cx.idempotency_key()), cents: self.cents })
    }
}

#[derive(Serialize, Deserialize)]
pub struct Refund {
    pub charge: String,
    pub cents: u64,
}

impl Activity for Refund {
    const NAME: &'static str = "refund";
    const QUEUE: &'static str = "payments";
    type Output = ();
    async fn run(self, _: ActivityCx) -> Result<(), Failure> {
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
pub struct Release {
    pub hold: String,
}

impl Activity for Release {
    const NAME: &'static str = "release";
    type Output = ();
    async fn run(self, _: ActivityCx) -> Result<(), Failure> {
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
pub struct Ship {
    pub to: String,
}

impl Activity for Ship {
    const NAME: &'static str = "ship";
    type Output = Label;
    async fn run(self, cx: ActivityCx) -> Result<Label, Failure> {
        Ok(Label { to: self.to, tracking: format!("trk-{}", cx.idempotency_key()) })
    }
}

pub async fn checkout(ctx: Ctx, order: Order) -> Result<Receipt, Failure> {
    let hold = ctx.activity(Reserve { sku: order.sku.clone(), qty: order.qty }).await?;
    let charge = Charge { account: order.account.clone(), cents: hold.cents, ms: order.charge_ms };
    let retry = Retry::exponential(5).max(secs(60)).non_retryable(&["card_declined"]);
    let paid = match ctx.activity(charge).retry(retry).await {
        Ok(paid) => paid,
        Err(e) => {
            ctx.cleanup(|c| c.activity(Release { hold: hold.id.clone() })).await?;
            return Err(e);
        }
    };
    match deliver(&ctx, &order).await {
        Ok(labels) => Ok(Receipt { paid, labels }),
        Err(e) => {
            // These run though the run was cancelled.
            let undo = ctx.cleanup(Ctx::clone);
            undo.activity(Refund { charge: paid.charge.clone(), cents: paid.cents }).await?;
            undo.activity(Release { hold: hold.id.clone() }).await?;
            Err(e)
        }
    }
}

async fn deliver(ctx: &Ctx, order: &Order) -> Result<Vec<Label>, Failure> {
    if ctx.signal::<Value>("approve").timeout(days(2)).await?.is_none() {
        return Err(Failure::fatal("no approval within 2 days"));
    }
    ctx.sleep(secs(order.ship_after_s)).await?;
    ctx.all(order.parcels.iter().map(|p| ctx.activity(Ship { to: p.to.clone() }))).await
}

#[derive(Serialize, Deserialize)]
pub struct Welcome {
    pub user: String,
    pub day: u64,
}

impl Activity for Welcome {
    const NAME: &'static str = "welcome";
    type Output = String;
    async fn run(self, _: ActivityCx) -> Result<String, Failure> {
        Ok(format!("mail of day {} sent to {}", self.day, self.user))
    }
}

#[derive(Serialize, Deserialize)]
pub struct Signup {
    pub user: String,
    #[serde(default = "three_days")]
    pub follow_up_s: u64,
}

fn three_days() -> u64 {
    3 * 86_400
}

pub async fn onboarding(ctx: Ctx, signup: Signup) -> Result<Vec<String>, Failure> {
    let first = ctx.activity(Welcome { user: signup.user.clone(), day: 0 }).await?;
    ctx.set_status(serde_json::json!({ "follow_up_s": signup.follow_up_s }));
    ctx.sleep(secs(signup.follow_up_s)).await?;
    let follow_up = ctx.activity(Welcome { user: signup.user, day: signup.follow_up_s / 86_400 }).await?;
    Ok(vec![first, follow_up])
}

#[derive(Serialize, Deserialize)]
pub struct Square {
    pub n: u64,
}

impl Activity for Square {
    const NAME: &'static str = "square";
    type Output = u64;
    async fn run(self, _: ActivityCx) -> Result<u64, Failure> {
        Ok(self.n * self.n)
    }
}

#[derive(Serialize, Deserialize)]
pub struct Batch {
    pub count: u64,
}

/// The sum of the squares of 0..count, one child run each.
pub async fn batch(ctx: Ctx, batch: Batch) -> Result<u64, Failure> {
    let children = (0..batch.count).map(|n| ctx.child::<u64>("item", n));
    Ok(ctx.all(children).await?.into_iter().sum())
}

pub async fn item(ctx: Ctx, n: u64) -> Result<u64, Failure> {
    ctx.activity(Square { n }).await
}

/// A no-op step on the run's node.
#[derive(Serialize, Deserialize)]
pub struct Noop {
    pub i: u32,
}

impl Activity for Noop {
    const NAME: &'static str = "noop";
    type Output = u32;
    async fn run(self, _: ActivityCx) -> Result<u32, Failure> {
        Ok(self.i)
    }
}

async fn steps(ctx: &Ctx, n: u32) -> Result<u32, Failure> {
    let mut sum = 0;
    for i in 0..n {
        sum += ctx.activity(Noop { i }).await?;
    }
    Ok(sum)
}

pub async fn nine_steps(ctx: Ctx, _: Value) -> Result<u32, Failure> {
    steps(&ctx, 9).await
}

pub async fn one_step(ctx: Ctx, _: Value) -> Result<u32, Failure> {
    steps(&ctx, 1).await
}

#[derive(Serialize, Deserialize)]
pub struct Sleep {
    pub secs: u64,
}

pub async fn sleeper(ctx: Ctx, sleep: Sleep) -> Result<(), Failure> {
    ctx.sleep(secs(sleep.secs)).await
}

/// Every sample workflow and activity.
pub fn app() -> App {
    App::new()
        .workflow("checkout", 1, checkout)
        .workflow("onboarding", 1, onboarding)
        .workflow("batch", 1, batch)
        .workflow("item", 1, item)
        .workflow("nine_steps", 1, nine_steps)
        .workflow("one_step", 1, one_step)
        .workflow("sleeper", 1, sleeper)
        .activity::<Reserve>()
        .activity::<Charge>()
        .activity::<Refund>()
        .activity::<Release>()
        .activity::<Ship>()
        .activity::<Welcome>()
        .activity::<Square>()
        .activity::<Noop>()
}
