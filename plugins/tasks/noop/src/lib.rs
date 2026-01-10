#![crate_type = "cdylib"]

use legion::task::NoopTaskPlugin;
use legion::task::TaskPlugin;

#[no_mangle]
pub extern "C" fn get() -> Box<dyn TaskPlugin> {
    Box::new(NoopTaskPlugin::default())
}
