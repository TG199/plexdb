use std::collections::HashMap;

struct SimpleDb {
    pub data: HashMap<String, String>,
}

impl SimpleDb {
    fn init() {
        let db = SimpleDB {
            data: HashMap::new(),
        }
    }

    fn insert(db: SimpleDb, key: String, value: String) {
        db.entry(key).or_insert(value)
    }

    fn get(db: SimpleDb, key: String) -> SimpleDb {
        db.get(&key)
    }

    fn delete(db: SimpleDb, key: String) {
        db.remove(&key);
        println!("key '{}' has been deleted", key)
    }

    fn display(db: SimpleDb) {
        for (k, v) in db.iter() {
            println!("{} {}", k, v);
    }

}

fn main() {

    let db = SimpleDb.init();

    db.insert(String::from("name"), String::from("kele")); 
}
