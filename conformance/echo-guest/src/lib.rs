//! Echo service: returns a JSON description of the request it received.
//! Exercises message round-tripping, body materialization, headers, and the
//! host `request` path (echoing the capability denial when asked).

wit_bindgen::generate!({
    path: "../../rs2-core/wit",
    world: "service",
});

use rs2::service::host;
use rs2::service::types::{BodyData, Header};

struct Echo;

impl Guest for Echo {
    fn init(_config: String) -> Result<(), String> {
        Ok(())
    }

    fn handle(msg: Message, config: String) -> Result<Message, String> {
        // When asked, try an ungranted capability so the suite can verify
        // the denial crosses the sandbox boundary intact.
        if msg.url.contains("deny-check") {
            return match host::request("not-granted", &msg) {
                Err(host::HostError::CapabilityDenied(_)) => Ok(Message {
                    method: msg.method,
                    url: msg.url,
                    headers: vec![],
                    status: 200,
                    body: Some(BodyData {
                        media_type: "text/plain".to_string(),
                        schema_ref: None,
                        bytes: b"denied-as-expected".to_vec(),
                    }),
                }),
                _ => Err("expected capability denial".to_string()),
            };
        }
        let body_text = msg
            .body
            .as_ref()
            .map(|b| String::from_utf8_lossy(&b.bytes).to_string())
            .unwrap_or_default();
        let json = format!(
            "{{\"method\":\"{}\",\"url\":\"{}\",\"body\":\"{}\",\"config\":{}}}",
            msg.method,
            msg.url,
            body_text.replace('"', "\\\""),
            if config.is_empty() { "null".to_string() } else { config }
        );
        Ok(Message {
            method: msg.method,
            url: msg.url,
            headers: vec![Header { name: "x-engine".to_string(), value: "wasm".to_string() }],
            status: 200,
            body: Some(BodyData {
                media_type: "application/json".to_string(),
                schema_ref: None,
                bytes: json.into_bytes(),
            }),
        })
    }
}

export!(Echo);
