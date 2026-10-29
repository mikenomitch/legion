//! `aws-asg`: Legion's first-party node-group target for AWS Auto Scaling
//! groups (docs/autoscaling-plugins.md, "The aws-asg plugin").
//!
//! A `legion-scaling/1` target plugin with `capabilities.nodes`:
//!
//! - `target/status`: `DescribeAutoScalingGroups`; the count is the group's
//!   `DesiredCapacity`, ready members are `InService` and healthy, and every
//!   instance is a member (`id` is the instance id, which a worker joins
//!   with as `legion.provider-id`).
//! - `target/scale`: writes the node group's join token to the SSM
//!   parameter the policy names (`joinTokenParameter`, a `SecureString` the
//!   instances' user data reads), then `SetDesiredCapacity`.
//! - `target/terminate`: `TerminateInstanceInAutoScalingGroup` with
//!   `ShouldDecrementDesiredCapacity`, one instance at a time; one already
//!   gone counts as done.
//!
//! Credentials never appear in the config: a web identity token file and a
//! role (Legion's second workload identity, exchanged with STS), the
//! standard environment variables, or the instance profile (see [`aws`]).

mod aws;
mod sigv4;
mod xml;

use legion_scaling_plugin::{
    serve, Error, Initialize, Kind, Member, Plugin, PluginInfo, ScaleRequest, TargetRequest,
    TargetStatus, TerminateRequest,
};
use serde_json::{json, Value};

/// The plugin: its AWS client once `initialize` has configured it.
#[derive(Default)]
struct AwsAsg {
    aws: Option<aws::Aws>,
}

/// The policy-level config (`target.nodes.config`).
struct PolicyConfig {
    asg: String,
    join_token_parameter: Option<String>,
    honor_cooldown: bool,
}

impl PolicyConfig {
    fn from(config: &Value) -> Result<Self, Error> {
        let asg = config
            .get("asg")
            .and_then(Value::as_str)
            .filter(|asg| !asg.trim().is_empty())
            .ok_or_else(|| Error::invalid_params("config.asg names the Auto Scaling group"))?
            .to_string();
        Ok(Self {
            asg,
            join_token_parameter: config
                .get("joinTokenParameter")
                .and_then(Value::as_str)
                .filter(|name| !name.trim().is_empty())
                .map(str::to_string),
            honor_cooldown: config
                .get("honorCooldown")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        })
    }
}

/// The JSON Schema of the policy-level config.
fn policy_config_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "asg": {
                "type": "string",
                "minLength": 1,
                "description": "The Auto Scaling group's name."
            },
            "joinTokenParameter": {
                "type": "string",
                "minLength": 1,
                "description": "An SSM parameter (SecureString) to write the node group's join token to before each scale-up; the instances' user data reads it and runs `legion join`."
            },
            "honorCooldown": {
                "type": "boolean",
                "description": "Pass HonorCooldown to SetDesiredCapacity (default false: the autoscaler paces itself)."
            }
        },
        "required": ["asg"],
        "additionalProperties": false
    })
}

impl AwsAsg {
    fn aws(&mut self) -> Result<&mut aws::Aws, Error> {
        self.aws
            .as_mut()
            .ok_or_else(|| Error::new("the plugin was not initialized"))
    }
}

impl Plugin for AwsAsg {
    fn initialize(&mut self, request: Initialize) -> Result<PluginInfo, Error> {
        let settings =
            aws::Settings::from_config(&request.config).map_err(Error::invalid_params)?;
        self.aws = Some(aws::Aws::new(settings).map_err(Error::new)?);
        let mut info = PluginInfo::new("aws-asg", env!("CARGO_PKG_VERSION"), &[Kind::Target])
            .with_schema(policy_config_schema());
        info.capabilities.nodes = true;
        Ok(info)
    }

    fn status(&mut self, request: TargetRequest) -> Result<TargetStatus, Error> {
        let config = PolicyConfig::from(&request.config)?;
        let group = self
            .aws()?
            .describe_group(&config.asg)
            .map_err(Error::new)?;
        Ok(TargetStatus {
            count: group.desired,
            ready: Some(group.instances.iter().filter(|i| i.ready()).count() as u32),
            members: group
                .instances
                .iter()
                .map(|instance| Member {
                    id: instance.id.clone(),
                    node: None,
                    state: Some(instance.state().to_string()),
                })
                .collect(),
        })
    }

    fn scale(&mut self, request: ScaleRequest) -> Result<(), Error> {
        let config = PolicyConfig::from(&request.config)?;
        let token = request
            .join
            .as_ref()
            .and_then(|join| join.get("token"))
            .and_then(Value::as_str)
            .filter(|token| !token.is_empty())
            .map(str::to_string);
        let aws = self.aws()?;
        match (&token, &config.join_token_parameter) {
            (Some(token), Some(parameter)) => {
                aws.put_parameter(parameter, token).map_err(Error::new)?
            }
            (Some(_), None) => eprintln!(
                "aws-asg: {}: no joinTokenParameter, so new instances get no group join token \
                 from this plugin (they need one from their launch template)",
                config.asg
            ),
            _ => {}
        }
        aws.set_desired(&config.asg, request.desired, config.honor_cooldown)
            .map_err(Error::new)
    }

    fn terminate(&mut self, request: TerminateRequest) -> Result<(), Error> {
        if request.ids.is_empty() {
            return Ok(());
        }
        // Validated even though only the ids are used: a policy whose config
        // is wrong should fail the same way on every method.
        PolicyConfig::from(&request.config)?;
        let aws = self.aws()?;
        let mut failed = Vec::new();
        for id in &request.ids {
            if let Err(err) = aws.terminate(id) {
                failed.push(err);
            }
        }
        if failed.is_empty() {
            Ok(())
        } else {
            Err(Error::new(failed.join("; ")))
        }
    }
}

fn main() -> std::io::Result<()> {
    serve(AwsAsg::default())
}

#[cfg(test)]
mod tests {
    //! The whole plugin against a mock AWS: a local HTTP server that answers
    //! the Query API from the recorded fixtures and remembers what it was
    //! asked.

    use super::*;
    use legion_scaling_plugin::handle_line;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct Seen {
        requests: Vec<(String, String, String)>,
    }

    /// Serve `count` requests; returns the endpoint and what was asked.
    fn mock_aws(count: usize) -> (String, Arc<Mutex<Seen>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Seen::default()));
        let record = seen.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming().take(count) {
                let mut stream = stream.unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let mut length = 0usize;
                let mut authorization = String::new();
                let mut target = String::new();
                loop {
                    let mut header = String::new();
                    reader.read_line(&mut header).unwrap();
                    let header = header.trim_end();
                    if header.is_empty() {
                        break;
                    }
                    let (name, value) = header.split_once(':').unwrap();
                    match name.to_ascii_lowercase().as_str() {
                        "content-length" => length = value.trim().parse().unwrap(),
                        "authorization" => authorization = value.trim().to_string(),
                        "x-amz-target" => target = value.trim().to_string(),
                        _ => {}
                    }
                }
                let mut body = vec![0u8; length];
                reader.read_exact(&mut body).unwrap();
                let body = String::from_utf8(body).unwrap();
                let (status, reply) = if target == "AmazonSSM.PutParameter" {
                    (
                        "200 OK",
                        "{\"Version\":2,\"Tier\":\"Standard\"}".to_string(),
                    )
                } else if body.contains("Action=DescribeAutoScalingGroups") {
                    (
                        "200 OK",
                        include_str!("../fixtures/describe-auto-scaling-groups.xml").to_string(),
                    )
                } else if body.contains("InstanceId=i-0dead") {
                    (
                        "400 Bad Request",
                        include_str!("../fixtures/error.xml").to_string(),
                    )
                } else {
                    (
                        "200 OK",
                        "<Response><ResponseMetadata><RequestId>x</RequestId></ResponseMetadata></Response>"
                            .to_string(),
                    )
                };
                record
                    .lock()
                    .unwrap()
                    .requests
                    .push((target.clone(), body, authorization));
                write!(
                    stream,
                    "HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{reply}",
                    reply.len()
                )
                .unwrap();
            }
        });
        (endpoint, seen)
    }

    fn call(plugin: &mut AwsAsg, id: u64, method: &str, params: Value) -> Value {
        let line = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        serde_json::from_str(&handle_line(plugin, &line.to_string())).unwrap()
    }

    #[test]
    fn status_scale_and_terminate_against_a_mock_aws_test() {
        std::env::set_var("AWS_ACCESS_KEY_ID", "AKIDEXAMPLE");
        std::env::set_var(
            "AWS_SECRET_ACCESS_KEY",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        );
        let (endpoint, seen) = mock_aws(5);
        let mut plugin = AwsAsg::default();
        let init = call(
            &mut plugin,
            1,
            "initialize",
            json!({ "protocol": "legion-scaling/1", "name": "aws-asg",
                    "config": { "region": "us-east-1", "endpoint": endpoint,
                                "ssmEndpoint": endpoint, "imds": false } }),
        );
        assert_eq!(init["result"]["capabilities"]["nodes"], true, "{init}");
        assert_eq!(init["result"]["kinds"], json!(["target"]));

        let config =
            json!({ "asg": "legion-workers", "joinTokenParameter": "/legion/workers/join" });
        let status = call(
            &mut plugin,
            2,
            "target/status",
            json!({ "policy": "workers", "config": config }),
        );
        assert_eq!(status["result"]["count"], 3, "{status}");
        assert_eq!(status["result"]["ready"], 1);
        assert_eq!(
            status["result"]["members"][0],
            json!({ "id": "i-0abc1234def567890", "state": "running" })
        );
        assert_eq!(status["result"]["members"][2]["state"], "terminating");

        let scale = call(
            &mut plugin,
            3,
            "target/scale",
            json!({ "policy": "workers", "config": config, "desired": 5, "decision": "d-1",
                    "join": { "token": "legion-join-secret", "attributes": { "legion.nodegroup": "workers" } } }),
        );
        assert_eq!(scale["result"], json!({}), "{scale}");

        let terminate = call(
            &mut plugin,
            4,
            "target/terminate",
            json!({ "policy": "workers", "config": config, "ids": ["i-0abc1234def567890", "i-0dead"] }),
        );
        assert_eq!(terminate["result"], json!({}), "{terminate}");
        // No ids: nothing to ask AWS.
        let nothing = call(
            &mut plugin,
            5,
            "target/terminate",
            json!({ "config": config, "ids": [] }),
        );
        assert_eq!(nothing["result"], json!({}));

        let seen = seen.lock().unwrap();
        let requests = &seen.requests;
        assert_eq!(requests.len(), 5);
        assert!(requests[0]
            .1
            .contains("AutoScalingGroupNames.member.1=legion-workers"));
        // The token went to SSM, before the resize, as a SecureString.
        assert_eq!(requests[1].0, "AmazonSSM.PutParameter");
        let put: Value = serde_json::from_str(&requests[1].1).unwrap();
        assert_eq!(put["Value"], "legion-join-secret");
        assert_eq!(put["Type"], "SecureString");
        assert!(requests[2].1.contains("Action=SetDesiredCapacity"));
        assert!(requests[2].1.contains("DesiredCapacity=5"));
        assert!(!requests[2].1.contains("legion-join-secret"));
        assert!(requests[3].1.contains("InstanceId=i-0abc1234def567890"));
        assert!(requests[3]
            .1
            .contains("ShouldDecrementDesiredCapacity=true"));
        assert!(requests[4].1.contains("InstanceId=i-0dead"));
        for (_, _, authorization) in requests.iter() {
            assert!(
                authorization.starts_with("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/"),
                "{authorization}"
            );
        }
        assert!(requests[0]
            .2
            .contains("/us-east-1/autoscaling/aws4_request"));
        assert!(requests[1].2.contains("/us-east-1/ssm/aws4_request"));
    }

    #[test]
    fn a_policy_config_without_a_group_is_invalid_params_test() {
        let mut plugin = AwsAsg::default();
        let status = call(&mut plugin, 1, "target/status", json!({ "config": {} }));
        assert_eq!(status["error"]["code"], -32602);
        let uninitialized = call(
            &mut plugin,
            2,
            "target/status",
            json!({ "config": { "asg": "x" } }),
        );
        assert!(uninitialized["error"]["message"]
            .as_str()
            .unwrap()
            .contains("not initialized"));
    }
}
