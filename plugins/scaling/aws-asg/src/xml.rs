//! Just enough XML for the AWS Query API's responses: elements, text,
//! the five predefined entities and numeric references. Attributes,
//! comments, the declaration and CDATA markers are read past.

/// One element.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Element {
    /// Its local name (a `prefix:` is dropped).
    pub name: String,
    /// Its text, entities decoded (the concatenation of its text nodes).
    pub text: String,
    /// Its child elements, in order.
    pub children: Vec<Element>,
}

impl Element {
    /// The first child named `name`.
    pub fn child(&self, name: &str) -> Option<&Element> {
        self.children.iter().find(|child| child.name == name)
    }

    /// Every child named `name`.
    pub fn children_named<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a Element> + 'a {
        self.children.iter().filter(move |child| child.name == name)
    }

    /// The first element named `name` at any depth, this one included.
    pub fn find(&self, name: &str) -> Option<&Element> {
        if self.name == name {
            return Some(self);
        }
        self.children.iter().find_map(|child| child.find(name))
    }

    /// The trimmed text of the first child named `name`.
    pub fn text_of(&self, name: &str) -> Option<&str> {
        self.child(name).map(|child| child.text.trim())
    }
}

fn decode(raw: &str) -> Result<String, String> {
    let mut out = String::with_capacity(raw.len());
    let mut rest = raw;
    while let Some(at) = rest.find('&') {
        out.push_str(&rest[..at]);
        let end = rest[at..]
            .find(';')
            .ok_or_else(|| format!("an unterminated entity in {raw:?}"))?;
        let entity = &rest[at + 1..at + end];
        match entity {
            "amp" => out.push('&'),
            "lt" => out.push('<'),
            "gt" => out.push('>'),
            "quot" => out.push('"'),
            "apos" => out.push('\''),
            _ => {
                let code = if let Some(hex) = entity.strip_prefix("#x") {
                    u32::from_str_radix(hex, 16).ok()
                } else if let Some(dec) = entity.strip_prefix('#') {
                    dec.parse::<u32>().ok()
                } else {
                    None
                };
                out.push(
                    code.and_then(char::from_u32)
                        .ok_or_else(|| format!("an unknown entity &{entity};"))?,
                );
            }
        }
        rest = &rest[at + end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

fn local(name: &str) -> String {
    name.rsplit(':').next().unwrap_or(name).to_string()
}

/// Parse a document into its root element.
pub fn parse(input: &str) -> Result<Element, String> {
    let mut stack: Vec<Element> = Vec::new();
    let mut root: Option<Element> = None;
    let mut rest = input;
    while !rest.is_empty() {
        let Some(open) = rest.find('<') else {
            if !rest.trim().is_empty() {
                match stack.last_mut() {
                    Some(element) => element.text.push_str(&decode(rest)?),
                    None => return Err("text outside the root element".to_string()),
                }
            }
            break;
        };
        if open > 0 {
            let text = &rest[..open];
            if let Some(element) = stack.last_mut() {
                element.text.push_str(&decode(text)?);
            } else if !text.trim().is_empty() {
                return Err("text outside the root element".to_string());
            }
        }
        rest = &rest[open..];
        if let Some(after) = rest.strip_prefix("<!--") {
            let end = after.find("-->").ok_or("an unterminated comment")?;
            rest = &after[end + 3..];
            continue;
        }
        if let Some(after) = rest.strip_prefix("<![CDATA[") {
            let end = after.find("]]>").ok_or("an unterminated CDATA section")?;
            if let Some(element) = stack.last_mut() {
                element.text.push_str(&after[..end]);
            }
            rest = &after[end + 3..];
            continue;
        }
        if rest.starts_with("<?") || rest.starts_with("<!") {
            let end = rest.find('>').ok_or("an unterminated declaration")?;
            rest = &rest[end + 1..];
            continue;
        }
        let end = rest.find('>').ok_or("an unterminated tag")?;
        let tag = &rest[1..end];
        rest = &rest[end + 1..];
        if let Some(name) = tag.strip_prefix('/') {
            let element = stack
                .pop()
                .ok_or_else(|| format!("a closing </{name}> with nothing open"))?;
            if element.name != local(name.trim()) {
                return Err(format!(
                    "</{}> closes <{}>",
                    name.trim(),
                    element.name
                ));
            }
            match stack.last_mut() {
                Some(parent) => parent.children.push(element),
                None if root.is_none() => root = Some(element),
                None => return Err("two root elements".to_string()),
            }
            continue;
        }
        let self_closing = tag.ends_with('/');
        let tag = tag.trim_end_matches('/');
        let name = tag
            .split(|c: char| c.is_whitespace())
            .next()
            .filter(|name| !name.is_empty())
            .ok_or("a tag without a name")?;
        let element = Element {
            name: local(name),
            ..Default::default()
        };
        if self_closing {
            match stack.last_mut() {
                Some(parent) => parent.children.push(element),
                None if root.is_none() => root = Some(element),
                None => return Err("two root elements".to_string()),
            }
        } else {
            stack.push(element);
        }
    }
    if let Some(open) = stack.last() {
        return Err(format!("<{}> is never closed", open.name));
    }
    root.ok_or_else(|| "no root element".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_an_aws_response_test() {
        let doc = parse(
            r#"<?xml version="1.0"?>
<DescribeAutoScalingGroupsResponse xmlns="https://autoscaling.amazonaws.com/doc/2011-01-01/">
  <!-- a comment -->
  <DescribeAutoScalingGroupsResult>
    <AutoScalingGroups>
      <member><AutoScalingGroupName>a &amp; b</AutoScalingGroupName><Tags/></member>
    </AutoScalingGroups>
  </DescribeAutoScalingGroupsResult>
</DescribeAutoScalingGroupsResponse>"#,
        )
        .unwrap();
        assert_eq!(doc.name, "DescribeAutoScalingGroupsResponse");
        let member = doc.find("member").unwrap();
        assert_eq!(member.text_of("AutoScalingGroupName"), Some("a & b"));
        assert!(member.child("Tags").is_some());
    }

    #[test]
    fn refuses_broken_documents_test() {
        assert!(parse("<a><b></a>").is_err());
        assert!(parse("<a>").is_err());
        assert!(parse("").is_err());
        assert!(parse("<a>&bogus;</a>").is_err());
        assert_eq!(parse("<a>&#65;&#x42;</a>").unwrap().text, "AB");
    }
}
