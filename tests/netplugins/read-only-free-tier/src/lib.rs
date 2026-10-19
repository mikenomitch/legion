// The example plugin from docs/network-plugins.md, verbatim: deny every
// request that is not a GET from a task labeled tier=free.
use proxy_wasm::traits::*;
use proxy_wasm::types::*;

proxy_wasm::main! {{
    proxy_wasm::set_http_context(|_, _| -> Box<dyn HttpContext> { Box::new(ReadOnlyFreeTier) });
}}

struct ReadOnlyFreeTier;
impl Context for ReadOnlyFreeTier {}
impl HttpContext for ReadOnlyFreeTier {
    fn on_http_request_headers(&mut self, _: usize, _: bool) -> Action {
        let tier = self.get_property(vec!["legion", "labels", "tier"]);
        let method = self.get_http_request_header(":method");
        if tier.as_deref() == Some(b"free".as_slice()) && method.as_deref() != Some("GET") {
            self.send_http_response(403, vec![("legion-denied-reason", "the free tier is read-only")], None);
            return Action::Pause;
        }
        Action::Continue
    }
}
