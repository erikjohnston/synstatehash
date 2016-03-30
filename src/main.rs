extern crate blake2_rfc;
extern crate docopt;
extern crate postgres;
extern crate postgres_array;
extern crate rustc_serialize;

use blake2_rfc::blake2b::blake2b;

use docopt::Docopt;

use postgres::{Connection, SslMode};
use postgres_array::Array;

use rustc_serialize::base64;
use rustc_serialize::base64::ToBase64;

use std::io::Write;
use std::process;



const USAGE: &'static str = "
synstatehash.

Usage:
    synstatehash <connection> <rows>
";


pub const UNPADDED_BASE64 : base64::Config = base64::Config {
    char_set: base64::CharacterSet::Standard,
    newline: base64::Newline::LF,
    pad: false,
    line_length: None,
};


#[derive(Debug, RustcDecodable)]
struct Args {
    arg_connection: String,
    arg_rows: i32,
}

fn main() {
    let args: Args = Docopt::new(USAGE)
                            .and_then(|d| d.decode())
                            .unwrap_or_else(|e| e.exit());

    writeln!(&mut std::io::stderr(), "Connecting").unwrap();

    let conn = Connection::connect(&args.arg_connection[..], SslMode::None).unwrap();

    writeln!(&mut std::io::stderr(), "Connected").unwrap();

    let txn = conn.transaction().unwrap();

    // Try and make sure we do fast start...
    txn.execute("set cursor_tuple_fraction TO 0", &[]).unwrap();
    txn.execute("set enable_sort TO false", &[]).unwrap();

    let stmt = txn.prepare(
        "SELECT state_group, array_agg(event_id) FROM state_groups_state GROUP BY state_group"
    ).unwrap();

    writeln!(&mut std::io::stderr(), "Executing...").unwrap();

    for row_result in stmt.lazy_query(&txn, &[], args.arg_rows).unwrap() {
        let row = row_result.unwrap();
        let event_ids_array: Array<String> = row.get(1);

        let mut event_ids: Vec<String> = event_ids_array.into_iter().collect();
        event_ids.sort();

        let hash = blake2b(64, &[], event_ids.join(",").as_bytes());
        let pretty_hash = hash.as_bytes().to_base64(UNPADDED_BASE64);

        // jki.re database has state_group column of type varchar, everyone else has bigint.
        if let Ok(group) = row.get_opt::<_, String>(0).unwrap() {
            println!("{}: {}", group, pretty_hash);
        } else if let Ok(group) = row.get_opt::<_, i64>(0).unwrap() {
            println!("{}: {}", group, pretty_hash);
        } else {
            let col_type = row.columns()[0].type_();
            writeln!(&mut std::io::stderr(), "Can't decode type {}", col_type).unwrap();
            process::exit(1);
        }
    }
}
