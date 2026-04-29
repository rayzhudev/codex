//! We do not do true JSON-RPC 2.0, as we neither send nor expect the
//! "jsonrpc": "2.0" field.

use codex_protocol::protocol::W3cTraceContext;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use ts_rs::TS;

pub const JSONRPC_VERSION: &str = "2.0";

#[derive(
    Debug, Clone, PartialEq, PartialOrd, Ord, Deserialize, Serialize, Hash, Eq, JsonSchema, TS,
)]
#[serde(untagged)]
pub enum RequestId {
    String(String),
    #[ts(type = "number")]
    Integer(i64),
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::String(value) => f.write_str(value),
            Self::Integer(value) => write!(f, "{value}"),
        }
    }
}

pub type Result = serde_json::Value;

/// Refers to any valid JSON-RPC object that can be decoded off the wire, or encoded to be sent.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(untagged)]
pub enum JSONRPCMessage {
    Request(JSONRPCRequest),
    Notification(JSONRPCNotification),
    Response(JSONRPCResponse),
    Error(JSONRPCError),
}

/// A request that expects a response.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct JSONRPCRequest {
    pub id: RequestId,
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub params: Option<serde_json::Value>,
    /// Optional W3C Trace Context for distributed tracing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub trace: Option<W3cTraceContext>,
}

/// A notification which does not expect a response.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct JSONRPCNotification {
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub params: Option<serde_json::Value>,
}

/// A successful (non-error) response to a request.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct JSONRPCResponse {
    pub id: RequestId,
    pub result: Result,
}

/// A response to a request that indicates an error occurred.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct JSONRPCError {
    pub error: JSONRPCErrorError,
    pub id: RequestId,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct JSONRPCErrorError {
    pub code: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub data: Option<serde_json::Value>,
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use serde_json::json;

    fn trace_context() -> W3cTraceContext {
        W3cTraceContext {
            traceparent: Some(
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
            ),
            tracestate: Some("vendor=value".to_string()),
        }
    }

    #[test]
    fn request_round_trips_with_integer_id_params_and_trace() {
        let request = JSONRPCRequest {
            id: RequestId::Integer(7),
            method: "thread/read".to_string(),
            params: Some(json!({ "threadId": "thread-1" })),
            trace: Some(trace_context()),
        };

        let value = serde_json::to_value(&request).expect("serialize request");
        assert_eq!(
            value,
            json!({
                "id": 7,
                "method": "thread/read",
                "params": { "threadId": "thread-1" },
                "trace": {
                    "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                    "tracestate": "vendor=value",
                },
            })
        );

        let message: JSONRPCMessage = serde_json::from_value(value).expect("deserialize message");
        assert_eq!(message, JSONRPCMessage::Request(request));
    }

    #[test]
    fn request_omits_absent_optional_fields() {
        let request = JSONRPCRequest {
            id: RequestId::String("req-1".to_string()),
            method: "thread/list".to_string(),
            params: None,
            trace: None,
        };

        let value = serde_json::to_value(&request).expect("serialize request");
        assert_eq!(
            value,
            json!({
                "id": "req-1",
                "method": "thread/list",
            })
        );

        let decoded: JSONRPCRequest = serde_json::from_value(value).expect("deserialize request");
        assert_eq!(decoded, request);
    }

    #[test]
    fn message_deserializes_notification_without_id() {
        let value = json!({
            "method": "thread/subscribe",
            "params": { "threadId": "thread-1" },
        });

        let message: JSONRPCMessage = serde_json::from_value(value).expect("deserialize message");
        assert_eq!(
            message,
            JSONRPCMessage::Notification(JSONRPCNotification {
                method: "thread/subscribe".to_string(),
                params: Some(json!({ "threadId": "thread-1" })),
            })
        );
    }

    #[test]
    fn message_deserializes_success_response() {
        let value = json!({
            "id": "req-1",
            "result": { "ok": true },
        });

        let message: JSONRPCMessage = serde_json::from_value(value).expect("deserialize message");
        assert_eq!(
            message,
            JSONRPCMessage::Response(JSONRPCResponse {
                id: RequestId::String("req-1".to_string()),
                result: json!({ "ok": true }),
            })
        );
    }

    #[test]
    fn message_deserializes_error_response_with_data() {
        let value = json!({
            "id": 9,
            "error": {
                "code": -32602,
                "message": "invalid params",
                "data": { "field": "threadId" },
            },
        });

        let message: JSONRPCMessage = serde_json::from_value(value).expect("deserialize message");
        assert_eq!(
            message,
            JSONRPCMessage::Error(JSONRPCError {
                id: RequestId::Integer(9),
                error: JSONRPCErrorError {
                    code: -32602,
                    message: "invalid params".to_string(),
                    data: Some(json!({ "field": "threadId" })),
                },
            })
        );
    }

    #[test]
    fn request_id_display_matches_wire_value() {
        assert_eq!(RequestId::String("req-1".to_string()).to_string(), "req-1");
        assert_eq!(RequestId::Integer(42).to_string(), "42");
    }

    #[test]
    fn message_rejects_request_with_response_fields() {
        let value = json!({
            "id": 7,
            "method": "thread/read",
            "params": { "threadId": "thread-1" },
            "result": { "ok": true },
        });

        serde_json::from_value::<JSONRPCMessage>(value)
            .expect_err("request with response result should be invalid");
    }

    #[test]
    fn message_rejects_request_with_error_fields() {
        let value = json!({
            "id": 7,
            "method": "thread/read",
            "params": { "threadId": "thread-1" },
            "error": {
                "code": -32603,
                "message": "internal error",
            },
        });

        serde_json::from_value::<JSONRPCMessage>(value)
            .expect_err("request with response error should be invalid");
    }
}
