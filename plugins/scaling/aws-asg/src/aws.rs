//! The AWS calls the plugin makes, and where its credentials come from.
//!
//! - **Auto Scaling** (Query API, version 2011-01-01): `DescribeAutoScalingGroups`,
//!   `SetDesiredCapacity`, `TerminateInstanceInAutoScalingGroup`.
//! - **SSM** (JSON API): `PutParameter`, to hand new instances the node
//!   group's join token, when the policy names a parameter.
//! - **STS**: `AssumeRoleWithWebIdentity`, unsigned, for credentials from a
//!   workload identity token.
//!
//! Credentials, first that applies (refreshed five minutes before they
//! expire):
//! 1. the plugin config's `webIdentityTokenFile` and `roleArn`: Legion's
//!    second workload identity, exchanged with STS (no static keys);
//! 2. `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (`AWS_SESSION_TOKEN`);
//! 3. `AWS_WEB_IDENTITY_TOKEN_FILE` and `AWS_ROLE_ARN`;
//! 4. the EC2 instance profile, through IMDSv2 (unless `imds = false`).

use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};

use crate::sigv4::{self, Credentials};
use crate::xml::{self, Element};

/// The Auto Scaling API version.
const AUTOSCALING_VERSION: &str = "2011-01-01";
/// The STS API version.
const STS_VERSION: &str = "2011-06-15";
/// Refresh temporary credentials this long before they expire.
const REFRESH_BEFORE: i64 = 300;
/// One AWS call's timeout.
const CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// Where the plugin finds AWS, from its `initialize` config.
#[derive(Debug, Clone, PartialEq)]
pub struct Settings {
    /// The region (`region`, else `AWS_REGION`, else `AWS_DEFAULT_REGION`).
    pub region: String,
    /// Auto Scaling's endpoint (`endpoint`; tests point it at a mock).
    pub autoscaling_endpoint: String,
    /// SSM's endpoint (`ssmEndpoint`).
    pub ssm_endpoint: String,
    /// STS's endpoint (`stsEndpoint`).
    pub sts_endpoint: String,
    /// A web identity token file and the role to assume with it.
    pub web_identity: Option<(PathBuf, String)>,
    /// The session name STS records (`roleSessionName`).
    pub role_session_name: String,
    /// The instance metadata service (`imdsEndpoint`), or `None` (`imds = false`).
    pub imds_endpoint: Option<String>,
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.trim().is_empty())
}

impl Settings {
    /// Read the plugin config.
    pub fn from_config(config: &Value) -> Result<Self, String> {
        let text = |key: &str| -> Result<Option<String>, String> {
            match config.get(key) {
                None | Some(Value::Null) => Ok(None),
                Some(Value::String(value)) if !value.trim().is_empty() => {
                    Ok(Some(value.trim().to_string()))
                }
                Some(other) => Err(format!("config.{key} must be a non-empty string, got {other}")),
            }
        };
        let region = text("region")?
            .or_else(|| env("AWS_REGION"))
            .or_else(|| env("AWS_DEFAULT_REGION"))
            .ok_or("set config.region (or AWS_REGION in the plugin's environment)")?;
        let web_identity = match (text("webIdentityTokenFile")?, text("roleArn")?) {
            (Some(file), Some(role)) => Some((PathBuf::from(expand_env(&file)?), role)),
            (None, None) => None,
            _ => {
                return Err(
                    "config.webIdentityTokenFile and config.roleArn go together".to_string()
                )
            }
        };
        let imds_endpoint = match config.get("imds") {
            Some(Value::Bool(false)) => None,
            None | Some(Value::Null) | Some(Value::Bool(true)) => Some(
                text("imdsEndpoint")?.unwrap_or_else(|| "http://169.254.169.254".to_string()),
            ),
            Some(other) => return Err(format!("config.imds must be a boolean, got {other}")),
        };
        Ok(Self {
            autoscaling_endpoint: text("endpoint")?
                .unwrap_or_else(|| format!("https://autoscaling.{region}.amazonaws.com")),
            ssm_endpoint: text("ssmEndpoint")?
                .unwrap_or_else(|| format!("https://ssm.{region}.amazonaws.com")),
            sts_endpoint: text("stsEndpoint")?
                .unwrap_or_else(|| format!("https://sts.{region}.amazonaws.com")),
            role_session_name: text("roleSessionName")?
                .unwrap_or_else(|| "legion-aws-asg".to_string()),
            region,
            web_identity,
            imds_endpoint,
        })
    }
}

/// A path that starts with `$NAME/` or `${NAME}/` gets the variable's value
/// (the app's `$LEGION_TASK_DIR`, where its identities' token files land).
pub fn expand_env(path: &str) -> Result<String, String> {
    let Some(rest) = path.strip_prefix('$') else {
        return Ok(path.to_string());
    };
    let (name, tail) = match rest.strip_prefix('{') {
        Some(braced) => {
            let end = braced
                .find('}')
                .ok_or_else(|| format!("{path:?}: an unclosed ${{"))?;
            (&braced[..end], &braced[end + 1..])
        }
        None => {
            let end = rest
                .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .unwrap_or(rest.len());
            (&rest[..end], &rest[end..])
        }
    };
    let value = std::env::var(name)
        .map_err(|_| format!("{path:?}: ${name} is not set in the plugin's environment"))?;
    Ok(format!("{value}{tail}"))
}

/// One Auto Scaling group, as `DescribeAutoScalingGroups` describes it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Group {
    /// The group's name.
    pub name: String,
    /// `DesiredCapacity`.
    pub desired: u32,
    /// `MinSize`.
    pub min: u32,
    /// `MaxSize`.
    pub max: u32,
    /// Its instances.
    pub instances: Vec<Instance>,
}

/// One instance of a group.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Instance {
    /// `i-…`.
    pub id: String,
    /// `LifecycleState`: `Pending`, `InService`, `Terminating`, …
    pub lifecycle: String,
    /// `HealthStatus`: `Healthy` or `Unhealthy`.
    pub health: String,
}

impl Instance {
    /// The lifecycle state in the words the autoscaler reads: `running`,
    /// `pending`, `terminating`, `terminated`, `standby`, `detached`.
    pub fn state(&self) -> &'static str {
        let lifecycle = self.lifecycle.as_str();
        if lifecycle == "InService" {
            "running"
        } else if lifecycle.starts_with("Pending") || lifecycle == "Warmed:Pending" {
            "pending"
        } else if lifecycle == "Terminated" {
            "terminated"
        } else if lifecycle.starts_with("Terminating") {
            "terminating"
        } else if lifecycle.contains("Standby") || lifecycle.starts_with("EnteringStandby") {
            "standby"
        } else if lifecycle.starts_with("Detach") {
            "detached"
        } else {
            "other"
        }
    }

    /// In service and healthy.
    pub fn ready(&self) -> bool {
        self.lifecycle == "InService" && self.health != "Unhealthy"
    }
}

fn number(element: &Element, name: &str) -> Result<u32, String> {
    element
        .text_of(name)
        .and_then(|text| text.parse().ok())
        .ok_or_else(|| format!("the response has no {name}"))
}

/// Read one group out of a `DescribeAutoScalingGroups` response.
pub fn parse_group(doc: &Element, name: &str) -> Result<Group, String> {
    let groups = doc
        .find("AutoScalingGroups")
        .ok_or("the response has no AutoScalingGroups")?;
    let group = groups
        .children_named("member")
        .find(|member| member.text_of("AutoScalingGroupName") == Some(name))
        .ok_or_else(|| format!("no Auto Scaling group named {name:?} in this account and region"))?;
    let instances = group
        .child("Instances")
        .map(|instances| {
            instances
                .children_named("member")
                .map(|member| Instance {
                    id: member.text_of("InstanceId").unwrap_or_default().to_string(),
                    lifecycle: member.text_of("LifecycleState").unwrap_or_default().to_string(),
                    health: member.text_of("HealthStatus").unwrap_or_default().to_string(),
                })
                .filter(|instance| !instance.id.is_empty())
                .collect()
        })
        .unwrap_or_default();
    Ok(Group {
        name: name.to_string(),
        desired: number(group, "DesiredCapacity")?,
        min: number(group, "MinSize")?,
        max: number(group, "MaxSize")?,
        instances,
    })
}

/// The `Code: Message` of an AWS Query API error document.
pub fn parse_error(body: &str) -> Option<(String, String)> {
    let doc = xml::parse(body).ok()?;
    let error = doc.find("Error")?;
    Some((
        error.text_of("Code").unwrap_or("Unknown").to_string(),
        error.text_of("Message").unwrap_or_default().to_string(),
    ))
}

/// Temporary or static credentials out of an STS response.
pub fn parse_sts_credentials(doc: &Element) -> Result<(Credentials, Option<i64>), String> {
    let credentials = doc
        .find("Credentials")
        .ok_or("the STS response has no Credentials")?;
    let field = |name: &str| {
        credentials
            .text_of(name)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .ok_or_else(|| format!("the STS response has no {name}"))
    };
    Ok((
        Credentials {
            access_key_id: field("AccessKeyId")?,
            secret_access_key: field("SecretAccessKey")?,
            session_token: Some(field("SessionToken")?),
        },
        credentials.text_of("Expiration").and_then(sigv4::parse_iso8601),
    ))
}

/// The AWS client: settings, an HTTP client, and cached credentials.
pub struct Aws {
    settings: Settings,
    http: reqwest::blocking::Client,
    cached: Option<(Credentials, Option<i64>)>,
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or(0)
}

/// The `host[:port]` of an endpoint URL, as it is signed and sent.
fn host_of(url: &reqwest::Url) -> Result<String, String> {
    let host = url
        .host_str()
        .ok_or_else(|| format!("endpoint {url} has no host"))?;
    Ok(match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    })
}

impl Aws {
    /// A client for `settings`.
    pub fn new(settings: Settings) -> Result<Self, String> {
        let http = reqwest::blocking::Client::builder()
            .timeout(CALL_TIMEOUT)
            .build()
            .map_err(|err| format!("building the HTTP client: {err}"))?;
        Ok(Self {
            settings,
            http,
            cached: None,
        })
    }

    /// The settings.
    pub fn settings(&self) -> &Settings {
        &self.settings
    }

    /// Current credentials, from the cache or the chain.
    pub fn credentials(&mut self) -> Result<Credentials, String> {
        if let Some((credentials, expires)) = &self.cached {
            if expires.is_none_or(|at| at - REFRESH_BEFORE > now_unix()) {
                return Ok(credentials.clone());
            }
        }
        let fresh = self.resolve_credentials()?;
        self.cached = Some(fresh.clone());
        Ok(fresh.0)
    }

    fn resolve_credentials(&self) -> Result<(Credentials, Option<i64>), String> {
        if let Some((file, role)) = &self.settings.web_identity {
            return self.assume_role_with_web_identity(file, role);
        }
        if let (Some(id), Some(secret)) = (env("AWS_ACCESS_KEY_ID"), env("AWS_SECRET_ACCESS_KEY")) {
            return Ok((
                Credentials {
                    access_key_id: id,
                    secret_access_key: secret,
                    session_token: env("AWS_SESSION_TOKEN"),
                },
                None,
            ));
        }
        if let (Some(file), Some(role)) = (env("AWS_WEB_IDENTITY_TOKEN_FILE"), env("AWS_ROLE_ARN")) {
            return self.assume_role_with_web_identity(&PathBuf::from(file), &role);
        }
        if let Some(imds) = &self.settings.imds_endpoint {
            return self.instance_profile(imds);
        }
        Err("no AWS credentials: set config.webIdentityTokenFile and config.roleArn, \
             AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or run on an instance with a profile"
            .to_string())
    }

    /// STS `AssumeRoleWithWebIdentity` with the token in `file` (read
    /// fresh every time: the issuer rotates it).
    fn assume_role_with_web_identity(
        &self,
        file: &PathBuf,
        role: &str,
    ) -> Result<(Credentials, Option<i64>), String> {
        let token = std::fs::read_to_string(file)
            .map_err(|err| format!("reading the web identity token {}: {err}", file.display()))?;
        let body = sigv4::form(&[
            ("Action".into(), "AssumeRoleWithWebIdentity".into()),
            ("Version".into(), STS_VERSION.into()),
            ("RoleArn".into(), role.to_string()),
            ("RoleSessionName".into(), self.settings.role_session_name.clone()),
            ("WebIdentityToken".into(), token.trim().to_string()),
        ]);
        let response = self
            .http
            .post(&self.settings.sts_endpoint)
            .header("content-type", "application/x-www-form-urlencoded; charset=utf-8")
            .body(body)
            .send()
            .map_err(|err| format!("STS AssumeRoleWithWebIdentity: {err}"))?;
        let status = response.status();
        let text = response
            .text()
            .map_err(|err| format!("STS AssumeRoleWithWebIdentity: {err}"))?;
        if !status.is_success() {
            return Err(match parse_error(&text) {
                Some((code, message)) => {
                    format!("STS AssumeRoleWithWebIdentity as {role}: {code}: {message}")
                }
                None => format!("STS AssumeRoleWithWebIdentity as {role}: HTTP {status}"),
            });
        }
        let doc = xml::parse(&text).map_err(|err| format!("STS response: {err}"))?;
        parse_sts_credentials(&doc)
    }

    /// The instance profile's credentials, through IMDSv2.
    fn instance_profile(&self, imds: &str) -> Result<(Credentials, Option<i64>), String> {
        let fail = |err: String| format!("instance metadata ({imds}): {err}");
        let token = self
            .http
            .put(format!("{imds}/latest/api/token"))
            .header("x-aws-ec2-metadata-token-ttl-seconds", "300")
            .timeout(Duration::from_secs(2))
            .send()
            .and_then(|response| response.error_for_status())
            .and_then(|response| response.text())
            .map_err(|err| fail(err.to_string()))?;
        let get = |path: &str| {
            self.http
                .get(format!("{imds}/latest/meta-data/iam/security-credentials/{path}"))
                .header("x-aws-ec2-metadata-token", token.trim())
                .timeout(Duration::from_secs(2))
                .send()
                .and_then(|response| response.error_for_status())
                .and_then(|response| response.text())
                .map_err(|err| fail(err.to_string()))
        };
        let role = get("")?;
        let role = role
            .lines()
            .next()
            .map(str::trim)
            .filter(|role| !role.is_empty())
            .ok_or_else(|| fail("the instance has no profile".to_string()))?
            .to_string();
        let document: Value = serde_json::from_str(&get(&role)?)
            .map_err(|err| fail(format!("credentials are not JSON: {err}")))?;
        let field = |name: &str| {
            document
                .get(name)
                .and_then(Value::as_str)
                .map(str::to_string)
                .ok_or_else(|| fail(format!("credentials have no {name}")))
        };
        Ok((
            Credentials {
                access_key_id: field("AccessKeyId")?,
                secret_access_key: field("SecretAccessKey")?,
                session_token: Some(field("Token")?),
            },
            document
                .get("Expiration")
                .and_then(Value::as_str)
                .and_then(sigv4::parse_iso8601),
        ))
    }

    /// One signed POST; returns the status and the body.
    fn signed_post(
        &mut self,
        endpoint: &str,
        service: &str,
        headers: &[(String, String)],
        body: String,
    ) -> Result<(reqwest::StatusCode, String), String> {
        let credentials = self.credentials()?;
        let url = reqwest::Url::parse(endpoint)
            .map_err(|err| format!("endpoint {endpoint:?}: {err}"))?;
        let host = host_of(&url)?;
        let signed = sigv4::sign(
            &credentials,
            &self.settings.region,
            service,
            &sigv4::amz_date(now_unix()),
            &sigv4::Request {
                method: "POST",
                host: &host,
                path: url.path(),
                query: &[],
                headers,
                payload: body.as_bytes(),
            },
        );
        let mut request = self.http.post(url).header("host", host).body(body);
        for (name, value) in headers.iter().chain(signed.iter()) {
            request = request.header(name.as_str(), value.as_str());
        }
        let response = request.send().map_err(|err| format!("{service}: {err}"))?;
        let status = response.status();
        let text = response.text().map_err(|err| format!("{service}: {err}"))?;
        Ok((status, text))
    }

    /// One Auto Scaling Query API call.
    fn autoscaling(&mut self, action: &str, params: &[(&str, String)]) -> Result<Element, String> {
        let mut pairs = vec![
            ("Action".to_string(), action.to_string()),
            ("Version".to_string(), AUTOSCALING_VERSION.to_string()),
        ];
        pairs.extend(params.iter().map(|(key, value)| (key.to_string(), value.clone())));
        let endpoint = self.settings.autoscaling_endpoint.clone();
        let (status, text) = self.signed_post(
            &endpoint,
            "autoscaling",
            &[(
                "content-type".to_string(),
                "application/x-www-form-urlencoded; charset=utf-8".to_string(),
            )],
            sigv4::form(&pairs),
        )?;
        if !status.is_success() {
            return Err(match parse_error(&text) {
                Some((code, message)) => format!("{action}: {code}: {message}"),
                None => format!("{action}: HTTP {status}"),
            });
        }
        xml::parse(&text).map_err(|err| format!("{action}: the response is not XML: {err}"))
    }

    /// `DescribeAutoScalingGroups` for one group.
    pub fn describe_group(&mut self, name: &str) -> Result<Group, String> {
        let doc = self.autoscaling(
            "DescribeAutoScalingGroups",
            &[("AutoScalingGroupNames.member.1", name.to_string())],
        )?;
        parse_group(&doc, name)
    }

    /// `SetDesiredCapacity`.
    pub fn set_desired(&mut self, name: &str, desired: u32, honor_cooldown: bool) -> Result<(), String> {
        self.autoscaling(
            "SetDesiredCapacity",
            &[
                ("AutoScalingGroupName", name.to_string()),
                ("DesiredCapacity", desired.to_string()),
                ("HonorCooldown", honor_cooldown.to_string()),
            ],
        )
        .map(|_| ())
    }

    /// `TerminateInstanceInAutoScalingGroup`, lowering the desired
    /// capacity with it. An instance already gone counts as terminated.
    pub fn terminate(&mut self, instance: &str) -> Result<(), String> {
        match self.autoscaling(
            "TerminateInstanceInAutoScalingGroup",
            &[
                ("InstanceId", instance.to_string()),
                ("ShouldDecrementDesiredCapacity", "true".to_string()),
            ],
        ) {
            Ok(_) => Ok(()),
            Err(err) if is_gone(&err) => Ok(()),
            Err(err) => Err(err),
        }
    }

    /// SSM `PutParameter` as a `SecureString`, overwriting.
    pub fn put_parameter(&mut self, name: &str, value: &str) -> Result<(), String> {
        let body = json!({
            "Name": name,
            "Value": value,
            "Type": "SecureString",
            "Overwrite": true,
        })
        .to_string();
        let endpoint = self.settings.ssm_endpoint.clone();
        let (status, text) = self.signed_post(
            &endpoint,
            "ssm",
            &[
                ("content-type".to_string(), "application/x-amz-json-1.1".to_string()),
                ("x-amz-target".to_string(), "AmazonSSM.PutParameter".to_string()),
            ],
            body,
        )?;
        if status.is_success() {
            return Ok(());
        }
        let detail = serde_json::from_str::<Value>(&text)
            .ok()
            .map(|error| {
                format!(
                    "{}: {}",
                    error
                        .get("__type")
                        .and_then(Value::as_str)
                        .unwrap_or("Unknown")
                        .rsplit('#')
                        .next()
                        .unwrap_or("Unknown"),
                    error
                        .get("message")
                        .or_else(|| error.get("Message"))
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                )
            })
            .unwrap_or_else(|| format!("HTTP {status}"));
        Err(format!("PutParameter {name}: {detail}"))
    }
}

/// Whether a terminate failed because the instance is already gone.
fn is_gone(error: &str) -> bool {
    error.contains("ValidationError")
        && (error.contains("not found") || error.contains("Instance Id not found"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const DESCRIBE: &str = include_str!("../fixtures/describe-auto-scaling-groups.xml");
    const ERROR: &str = include_str!("../fixtures/error.xml");
    const STS: &str = include_str!("../fixtures/assume-role-with-web-identity.xml");

    #[test]
    fn a_group_parses_from_a_recorded_response_test() {
        let doc = xml::parse(DESCRIBE).unwrap();
        let group = parse_group(&doc, "legion-workers").unwrap();
        assert_eq!((group.desired, group.min, group.max), (3, 0, 20));
        assert_eq!(group.instances.len(), 3);
        assert_eq!(group.instances[0].id, "i-0abc1234def567890");
        assert_eq!(group.instances[0].state(), "running");
        assert!(group.instances[0].ready());
        assert_eq!(group.instances[1].state(), "pending");
        assert!(!group.instances[1].ready());
        assert_eq!(group.instances[2].state(), "terminating");
        assert!(parse_group(&doc, "other")
            .unwrap_err()
            .contains("no Auto Scaling group named"));
    }

    #[test]
    fn errors_and_sts_credentials_parse_test() {
        assert_eq!(
            parse_error(ERROR),
            Some((
                "ValidationError".to_string(),
                "Instance Id not found - No managed instance found for instance ID: i-0dead"
                    .to_string()
            ))
        );
        assert!(is_gone("TerminateInstanceInAutoScalingGroup: ValidationError: Instance Id not found - No managed instance found for instance ID: i-0dead"));
        assert!(!is_gone("SetDesiredCapacity: ValidationError: New SetDesiredCapacity value 30 is above max value 20"));
        let doc = xml::parse(STS).unwrap();
        let (credentials, expires) = parse_sts_credentials(&doc).unwrap();
        assert_eq!(credentials.access_key_id, "ASIAEXAMPLEKEYID");
        assert_eq!(credentials.session_token.as_deref(), Some("FwoGZXIvYXdzEXAMPLE"));
        assert_eq!(expires, sigv4::parse_iso8601("2026-10-28T13:00:00Z"));
    }

    #[test]
    fn settings_read_the_config_test() {
        let settings = Settings::from_config(&json!({ "region": "eu-west-1" })).unwrap();
        assert_eq!(
            settings.autoscaling_endpoint,
            "https://autoscaling.eu-west-1.amazonaws.com"
        );
        assert_eq!(settings.imds_endpoint.as_deref(), Some("http://169.254.169.254"));
        let federated = Settings::from_config(&json!({
            "region": "us-east-1", "imds": false,
            "webIdentityTokenFile": "/secrets/aws/token",
            "roleArn": "arn:aws:iam::123456789012:role/legion-autoscaler",
            "endpoint": "http://127.0.0.1:9000",
        }))
        .unwrap();
        assert_eq!(federated.imds_endpoint, None);
        assert_eq!(federated.autoscaling_endpoint, "http://127.0.0.1:9000");
        assert_eq!(
            federated.web_identity,
            Some((
                PathBuf::from("/secrets/aws/token"),
                "arn:aws:iam::123456789012:role/legion-autoscaler".to_string()
            ))
        );
        assert!(Settings::from_config(&json!({ "region": "us-east-1", "roleArn": "x" })).is_err());
        std::env::set_var("AWS_ASG_TEST_TASK_DIR", "/task");
        assert_eq!(
            expand_env("$AWS_ASG_TEST_TASK_DIR/secrets/aws.token").unwrap(),
            "/task/secrets/aws.token"
        );
        assert_eq!(
            expand_env("${AWS_ASG_TEST_TASK_DIR}/t").unwrap(),
            "/task/t"
        );
        assert_eq!(expand_env("/abs/t").unwrap(), "/abs/t");
        assert!(expand_env("$AWS_ASG_TEST_UNSET_VAR/t").is_err());
        assert!(Settings::from_config(&json!({ "region": 5 })).is_err());
    }
}
