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
