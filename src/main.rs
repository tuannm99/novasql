mod config;
mod database;
mod page;

fn main() {
    let config = config::Config::from_yaml_file("novasql.yaml").ok();
    let db_path = "testdb.db";
    println!("Loaded config: {:?}", config);
    let db = database::Database::new_with_config(db_path, config.as_ref())
        .expect("Failed to create database");

    let data = vec![42u8; page::PAGE_SIZE];
    db.write_page(0, &data).expect("Failed to write page");

    let page = db.get_page(0).expect("Failed to get page");
    println!("Read page 0, first 8 bytes: {:?}", &page.data[..8]);

    db.close().expect("Failed to close database");
    println!("Database closed.");
}
