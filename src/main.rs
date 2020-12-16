use std::{
    env,
    process::{Command, Output},
};

use anyhow::{Context, Result, ensure, anyhow};
use postgres::{Client, NoTls, Row};
use serde_json::Value;
use tracing::*;
use tracing_subscriber::*;
use stopwatch::{Stopwatch};

/*
Shortcomings:
no db pooling
allocation strategy - only IPv4 is added to DB and used for resource allocation
Resource states not supported: on bench, free (deleted currently)
 */

#[derive(Debug, PartialEq)]
struct ResourcePool {
    id: i32,
    name: String,
    version: i32,
}

#[derive(Debug, PartialEq)]
struct Resource {
    id: Option<i32>,
    resource_pool_id: i32,
    value: Value,
}

impl Resource {
    fn new(resource_pool_id: i32, value_str: &str) -> Result<Resource> {
        let value = serde_json::from_str(value_str)?;
        Ok(Resource {
            id: Option::None,
            resource_pool_id,
            value,
        })
    }
}

struct WasmerEnv {
    wasmer_command: Command,
    wasmer_js: String,
}

impl WasmerEnv {
    fn new() -> Result<WasmerEnv> {
        let wasmer_bin = env::var("WASMER_BIN").context("Cannot read env var WASMER_BIN")?;
        let wasmer_command = Command::new(wasmer_bin);
        let wasmer_js = env::var("WASMER_JS").context("Cannot read env var WASMER_JS")?;
        Ok(WasmerEnv {
            wasmer_command,
            wasmer_js,
        })
    }

    fn invoke_js(&mut self, script: &str) -> Result<Output> {
        self.wasmer_command
            .arg(&self.wasmer_js)
            .arg("--")
            .arg("--std")
            .arg("-e")
            .arg(script);

        self.wasmer_command
            .output()
            .context("Cannot execute quickJS")
    }

    fn invoke_and_parse(&mut self, script: &str, user_input: Value, resource_pool_properties: Value,
                        resource_pool: Value, current_resources: Vec<Value>, function_call: &str)
                        -> Result<Value> {
        let mut header = "
        console.error = function(...args) {
            std.err.puts(args.join(' '));
            std.err.puts('\\n');
        }
        const log = console.error;
        ".to_owned();

        header += &Self::add_js_var("userInput", user_input)?;
        header += &Self::add_js_var("resourcePoolProperties", resource_pool_properties)?;
        header += &Self::add_js_var("resourcePool", resource_pool)?;
        header += &Self::add_js_var("currentResources", Value::Array(current_resources))?;

        let footer = format!("\nlet result = {};\n", function_call) + "
        if (result != null) {
            if (typeof result === 'object') {
                result = JSON.stringify(result);
            }
            std.out.puts(result);
        }
        ";
        let script = header + script + &footer;
        debug!("Executing script:\n{}", script);
        let output = self.invoke_js(&script)?;
        trace!("Output {:?}", output);
        let val: Value = serde_json::from_slice(&output.stdout)
            .context(format!("Cannot deserialize '{:?}'", output.stdout))?;
        Ok(val)
    }

    fn add_js_var(name: &str, val: Value) -> Result<String> {
        let serialized = serde_json::to_string(&val)?;
        Ok(format!("const {} = {};\n", name, &serialized))
    }
}

struct DB {
    client: Client,
}

impl DB {
    pub fn new_from_env() -> Result<DB> {
        let params = std::env::var("DB_PARAMS")?;
        Self::new(&params)
    }

    pub fn new(params: &str) -> Result<DB> {
        let client = Client::connect(params, NoTls)?;
        Ok(DB { client })
    }

    // allocation strategies
    pub fn get_ipv4_script(&mut self) -> Result<String> {
        let found = self.client.query_one(
            "SELECT script FROM allocation_strategies WHERE name='ipv4'", &[])?;
        let script: &str = found.get(0);
        Ok(script.to_owned())
    }

    // resource pools
    pub fn insert_resource_pool(&mut self, name: &str) -> Result<ResourcePool> {
        let version: i32 = 0;
        let allocation_strategy: i32 = 1;//FIXME hardcoded ipv4 strategy
        let row = self.client.query_one(
            "INSERT INTO resource_pools (name, version, resource_pool_allocation_strategy) \
            VALUES ($1, $2, $3) RETURNING id as id",
            &[&name, &version, &allocation_strategy],
        )?;
        let id: i32 = row.get(0);
        Ok(ResourcePool { id, name: name.to_owned(), version })
    }

    pub fn get_resource_pool_by_id(&mut self, id: i32) -> Result<ResourcePool> {
        let found = self.client.query_one(
            "SELECT id, name, version FROM resource_pools WHERE id=$1", &[&id])?;
        Self::row_to_resource_pool(found)
    }

    pub fn get_resource_pool_by_name(&mut self, name: &str) -> Result<ResourcePool> {
        let found = self.client.query_one(
            "SELECT id, name, version FROM resource_pools WHERE name=$1", &[&name])?;
        Self::row_to_resource_pool(found)
    }

    fn row_to_resource_pool(row: Row) -> Result<ResourcePool> {
        let id: i32 = row.get(0);
        let name: String = row.get(1);
        let version: i32 = row.get(2);
        Ok(ResourcePool { id, name, version })
    }

    // resources
    pub fn insert_resources(&mut self, mut pool: ResourcePool, items: &Vec<Resource>) -> Result<()> {
        let mut transaction = self.client.transaction()?;
        ensure!(items.len() > 0, "Cannot insert zero resources");
        const PARAMS_PER_ROW: usize = 2;
        let mut params: Vec<&(dyn postgres::types::ToSql + Sync)> =
            Vec::with_capacity(PARAMS_PER_ROW * items.len());
        let mut query =
            "INSERT INTO resources (resource_pool, value) VALUES ".to_owned();
        let mut idx = 0;
        for resource in items {
            ensure!(resource.resource_pool_id == pool.id, "Wrong resource id");
            params.push(&resource.resource_pool_id);
            params.push(&resource.value);
            query += &format!("(${},${}),", PARAMS_PER_ROW * idx + 1, PARAMS_PER_ROW * idx + 2);
            idx += 1;
        }
        ensure!(query.remove(query.len() - 1) == ',', "Expected to remove a coma");
        // if IDs are needed, add " RETURNING id as id";
        let inserted = transaction.execute(query.as_str(), &params)?;
        debug!("inserted {}", inserted);
        ensure!(inserted == items.len() as u64, "Insertion of resources returned wrong number of rows");
        // update pool version
        pool.version += 1;
        let updated_pool = transaction.execute("UPDATE resource_pools SET version=$1 WHERE id=$2",
                                               &[&pool.version, &pool.id])?;
        ensure!(updated_pool == 1, "Update of resource_pools returned wrong number of rows");
        transaction.commit()?;
        Ok(())
    }
}

fn main() -> Result<()> {
    let sw = Stopwatch::start_new();

    let fmt_event = tracing_subscriber::fmt::format::Format::default().with_target(false);
    tracing_subscriber::fmt()
        .event_format(fmt_event)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    debug!("Initialized in {}ms", sw.elapsed_ms());

    let span = span!(Level::INFO, "my_span");
    let _enter = span.enter();


    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Once;

    use rand::Rng;
    use rand::distributions::{Alphanumeric};
    use serde_json::json;

    use super::*;

    static START: Once = Once::new();

    fn initialize_logging() {
        START.call_once(|| {
            let fmt_event = tracing_subscriber::fmt::format::Format::default().with_target(false);
            tracing_subscriber::fmt()
                .event_format(fmt_event)
                .with_env_filter(EnvFilter::from_default_env())
                .init();
        });
    }

    #[test]
    fn wasmer_invoke_js() -> Result<()> {
        initialize_logging();
        debug!("test");
        let mut wasmer_env = WasmerEnv::new()?;
        let output = wasmer_env.invoke_js("console.log(2+2)")?;
        trace!("{:?}", output);
        assert_eq!("4\n", String::from_utf8(output.stdout)?);
        Ok(())
    }

    #[test]
    fn wasmer_invoke_and_parse() -> Result<()> {
        initialize_logging();
        let mut wasmer_env = WasmerEnv::new()?;
        let script = "function invoke() {\
        return {mykey:1, userInput, resourcePoolProperties, resourcePool, currentResources}\
        }";
        let user_input = json!({
            "input": "input"
        });
        let resource_pool_properties = json!({
            "rpp":"rpp"
        });
        let resource_pool = json!({
            "rp":"rp"
        });
        let current_resources = json!([
            "res1", "res2"
        ]).as_array().ok_or(anyhow!("Unexpected"))?.to_owned();

        let actual = wasmer_env.invoke_and_parse(script, user_input, resource_pool_properties,
                                                 resource_pool, current_resources, "invoke()")?;
        let expected = json!({
            "mykey": 1,
            "userInput": {"input":"input"},
            "resourcePoolProperties": {"rpp":"rpp"},
            "resourcePool": {"rp":"rp"},
            "currentResources": ["res1", "res2"]
        });
        assert_eq!(expected, actual);
        Ok(())
    }

    #[test]
    fn db_get_ipv4_script() -> Result<()> {
        initialize_logging();

        let mut db = DB::new_from_env()?;
        let sw = Stopwatch::start_new();
        let script = db.get_ipv4_script()?;
        debug!("Found row in {}ms", sw.elapsed_ms());
        trace!("found script: {}", script);
        Ok(())
    }

    fn create_random_pool(db: &mut DB) -> Result<ResourcePool> {
        let random_string: String = rand::thread_rng().sample_iter(&Alphanumeric).take(10).collect();
        // check that it does not exist
        assert!(db.get_resource_pool_by_name(&random_string).is_err());
        db.insert_resource_pool(&random_string)
    }

    #[test]
    fn db_resource_pool() -> Result<()> {
        initialize_logging();

        let mut db = DB::new_from_env()?;
        let sw = Stopwatch::start_new();
        let inserted = create_random_pool(&mut db)?;
        debug!("Inserted row in {}ms", sw.elapsed_ms());
        let sw = Stopwatch::start_new();
        let by_name = db.get_resource_pool_by_name(&inserted.name)?;
        debug!("Found row in {}ms", sw.elapsed_ms());
        let sw = Stopwatch::start_new();
        assert_eq!(inserted, by_name);
        let by_id = db.get_resource_pool_by_id(inserted.id)?;
        debug!("Found row in {}ms", sw.elapsed_ms());
        assert_eq!(inserted, by_id);
        Ok(())
    }

    #[test]
    fn db_insert_resources_duplicates_should_fail() -> Result<()> {
        initialize_logging();
        let mut db = DB::new_from_env()?;
        let pool = create_random_pool(&mut db)?;
        let resource_pool_id = pool.id;

        let resources = vec!(
            Resource::new(resource_pool_id, "{\"address\":\"1.1.1.1\"}")?,
            Resource::new(resource_pool_id, "{\"address\":\"1.1.1.1\"}")?,
        );

        db.insert_resources(pool, &resources).expect_err("Should not accept duplicates");
        Ok(())
    }

    #[test]
    fn db_insert_resources() -> Result<()> {
        initialize_logging();
        const ROW_COUNT: usize = 100;
        let mut db = DB::new_from_env()?;
        let pool = create_random_pool(&mut db)?;

        let resource_pool_id = pool.id;
        let old_version = pool.version;
        let sw = Stopwatch::start_new();
        let mut resources = Vec::with_capacity(ROW_COUNT);
        for idx in 0..ROW_COUNT {
            resources.push(Resource::new(resource_pool_id,
                                         &format!("{{\"address\":\"1.1.1.{}\"}}", idx))?);
        }
        db.insert_resources(pool, &resources)?;
        debug!("inserted {} rows in {}ms", resources.len(), sw.elapsed_ms());
        // check that version is incremented
        assert_eq!(db.get_resource_pool_by_id(resource_pool_id)?.version, old_version + 1);
        Ok(())
    }
}
