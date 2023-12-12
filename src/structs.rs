use crate::util::merge_json_strings;
tonic::include_proto!("legion");

pub trait Printable {
    fn print_short(&self) -> ();
    fn print_long(&self) -> ();
}

impl Printable for Node {
    fn print_short(&self) {
        println!("ID: {}, Status: {}, Checkin: {}", self.id, self.status, self.last_heartbeat);
    }

    fn print_long(&self) {
        println!("Node ID: {}", self.id);
        let full_attrs = merge_json_strings(self.attrs.clone(), self.dynamic.clone());
        println!("Attributes: {}", full_attrs);
    }
}

// impl for Node {
//     last_heartbeat_too_slow(last_heartbeat: string) -> bool {
//         let last_heartbeat = self.last_heartbeat;
//         let last_heartbeat = OffsetDateTime::parse(last_heartbeat, "%+")?;
//         let now = OffsetDateTime::now_utc();
//         let diff = now - last_heartbeat;
//         let diff = diff.whole_seconds();
//         diff > 30
//     }
// }
