use time::macros::format_description;
use crate::constants::DEFAULT_HEARTBEAT_TIMEOUT_SEC;
use crate::util::{merge_json_strings, R};

tonic::include_proto!("legion");

pub trait Printable {
    fn print_short(&self) -> ();
    fn print_short_json(&self) -> ();
    fn print_long(&self) -> ();
    fn print_long_json(&self) -> ();
    fn print_list_json(nodes: Vec<Node>) -> ();
    fn print_list(nodes: Vec<Node>) -> ();
}

impl Printable for Node {
    fn print_short(&self) {
        println!(
            "ID: {}, Status: {}, Pool: {}, Checkin: {}",
            self.id, self.status, self.pool, self.last_heartbeat
        );
    }

    fn print_list_json(nodes: Vec<Node>) {
        let json = serde_json::to_string(&nodes).unwrap();
        println!("{}", json);
    }

    fn print_list(nodes: Vec<Node>) {
        nodes.into_iter().for_each(|node| {
            node.print_short()
        });
    }

    fn print_short_json(&self) {
        let json = serde_json::to_string(&self).unwrap();
        println!("{}", json);
    }

    fn print_long(&self) {
        println!("Node ID: {}", self.id);
        let full_attrs = merge_json_strings(self.attrs.clone(), self.dynamic.clone());
        println!("Attributes: {}", full_attrs);
    }

    fn print_long_json(&self) {
        let json = serde_json::to_string(&self).unwrap();
        println!("{}", json);
    }
}

impl Node {
    pub fn last_heartbeat_too_slow(last_heartbeat: &str) -> R<bool> {
        let fmt = format_description!(
            "[year]-[month]-[day] [hour]:[minute]:[second] [offset_hour \
                 sign:mandatory]:[offset_minute]:[offset_second]"
        );

        let last_heartbeat = time::OffsetDateTime::parse(last_heartbeat, fmt)?;
        let now = time::OffsetDateTime::now_utc();
        let diff = now - last_heartbeat;
        let too_slow = diff > time::Duration::seconds(DEFAULT_HEARTBEAT_TIMEOUT_SEC);

        Ok(too_slow)
    }
}