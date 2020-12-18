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
use serde_json::json;

#[derive(Debug, PartialEq)]
struct ResourcePool {
    id: i32,
    name: String,
    version: i32,
    allocation_strategy_id: i32,
}

impl ResourcePool {
    pub fn as_json(&self) -> Value {
        //FIXME no documentation found, currently not used by IPv4 script
        json!({})
    }

    pub fn get_pool_properties(&self) -> Value {
        // currently hardcoded
        json!({
            "address": "10.0.0.0",
            "prefix": 8,
        })
    }
}

#[derive(Debug, PartialEq)]
struct Resource {
    id: Option<i32>,
    resource_pool_id: i32,
    value: Value,
}

impl Resource {
    fn new_from_str(resource_pool_id: i32, value_str: &str) -> Result<Resource> {
        let value = serde_json::from_str(value_str)?;
        Ok(Resource {
            id: None,
            resource_pool_id,
            value,
        })
    }

    fn new_from_value(resource_pool_id: i32, value: Value) -> Resource {
        Resource { id: None, resource_pool_id, value }
    }

    fn as_json(&self) -> Value {
        json!({"Properties": &self.value})
    }
}

struct WasmerEnv {
    wasmer_bin: String,
    wasmer_js: String,
}

impl WasmerEnv {
    fn new() -> Result<WasmerEnv> {
        let wasmer_bin = env::var("WASMER_BIN").context("Cannot read env var WASMER_BIN")?;
        let wasmer_js = env::var("WASMER_JS").context("Cannot read env var WASMER_JS")?;
        Ok(WasmerEnv {
            wasmer_bin,
            wasmer_js,
        })
    }

    fn invoke_js(&mut self, script: &str) -> Result<Output> {
        Command::new(&self.wasmer_bin)
            .arg(&self.wasmer_js)
            .arg("--")
            .arg("--std")
            .arg("-e")
            .arg(script)
            .output()
            .context("Cannot execute quickJS")
    }

    fn invoke_and_parse(&mut self, script: &str, user_input: Value, resource_pool_properties: Value,
                        resource_pool: Value, current_resources: Vec<Value>, function_call: &str)
                        -> Result<Vec<Value>> {
        let sw = Stopwatch::start_new();
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
        trace!("Executing script:\n{}", script);
        let output = self.invoke_js(&script)?;
        debug!("Output {:?}", output);
        let val: Value = serde_json::from_slice(&output.stdout)
            .context(format!("Cannot deserialize '{:?}'", output))?;
        info!("Wasmer finished in {}ms", sw.elapsed_ms());
        val.as_array()
            .ok_or(anyhow!("Script did not return an array"))
            .map(|vec| vec.to_owned())
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
    pub fn get_allocation_script(&mut self, id: i32) -> Result<String> {
        let found = self.client.query_one(
            "SELECT script FROM allocation_strategies WHERE id=$1", &[&id])?;
        let script: &str = found.get(0);
        Ok(script.to_owned())
    }

    // resource pools
    pub fn insert_resource_pool(&mut self, name: &str, allocation_strategy_id: i32) -> Result<ResourcePool> {
        let version: i32 = 0;
        let row = self.client.query_one(
            "INSERT INTO resource_pools (name, version, resource_pool_allocation_strategy) \
            VALUES ($1, $2, $3) RETURNING id as id",
            &[&name, &version, &allocation_strategy_id],
        )?;
        let id: i32 = row.get(0);
        Ok(ResourcePool { id, name: name.to_owned(), version, allocation_strategy_id })
    }

    pub fn get_resource_pool_by_id(&mut self, id: i32) -> Result<ResourcePool> {
        let found = self.client.query_one(
            "SELECT id, name, version, resource_pool_allocation_strategy FROM resource_pools WHERE id=$1", &[&id])?;
        Self::row_to_resource_pool(found)
    }

    pub fn get_resource_pool_by_name(&mut self, name: &str) -> Result<ResourcePool> {
        let found = self.client.query_one(
            "SELECT id, name, version, resource_pool_allocation_strategy FROM resource_pools WHERE name=$1", &[&name])?;
        Self::row_to_resource_pool(found)
    }

    fn row_to_resource_pool(row: Row) -> Result<ResourcePool> {
        let id: i32 = row.get(0);
        let name: String = row.get(1);
        let version: i32 = row.get(2);
        let allocation_strategy_id = row.get(3);
        Ok(ResourcePool { id, name, version, allocation_strategy_id })
    }

    // resources
    pub fn insert_resources(&mut self, mut pool: ResourcePool, items: Vec<Resource>)
                            -> Result<(ResourcePool, Vec<Resource>)> {
        let mut transaction = self.client.transaction()?;
        ensure!(items.len() > 0, "Cannot insert zero resources");
        const PARAMS_PER_ROW: usize = 2;
        let mut params: Vec<&(dyn postgres::types::ToSql + Sync)> =
            Vec::with_capacity(PARAMS_PER_ROW * items.len());
        let mut query =
            "INSERT INTO resources (resource_pool, value) VALUES ".to_owned();
        let mut idx = 0;
        for resource in &items {
            ensure!(resource.resource_pool_id == pool.id, "Wrong resource id");
            params.push(&resource.resource_pool_id);
            params.push(&resource.value);
            query += &format!("(${},${}),", PARAMS_PER_ROW * idx + 1, PARAMS_PER_ROW * idx + 2);
            idx += 1;
        }
        ensure!(query.remove(query.len() - 1) == ',', "Expected to remove a coma");

        // FIXME if IDs are needed, add " RETURNING id as id";

        let inserted_count = transaction.execute(query.as_str(), &params)?;
        trace!("Inserted {} resources", inserted_count);
        ensure!(inserted_count == items.len() as u64, "Insertion of resources returned wrong number of rows");
        // update pool version
        let expected_current_version = pool.version;
        pool.version += 1;
        let updated_count = transaction.execute(
            "UPDATE resource_pools SET version=$1 WHERE id=$2 AND version=$3",
            &[&pool.version, &pool.id, &expected_current_version])?;
        ensure!(updated_count == 1, "Update of resource_pools returned wrong number of rows");
        transaction.commit()?;
        Ok((ResourcePool {
            id: pool.id,
            name: pool.name,
            version: pool.version,
            allocation_strategy_id: pool.allocation_strategy_id,
        }, items))
    }

    pub fn get_resources(&mut self, resource_pool_id: i32) -> Result<Vec<Resource>> {
        let rows = self.client.query(
            "SELECT id, value FROM resources WHERE resource_pool=$1", &[&resource_pool_id])?;
        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let id: i32 = row.get(0);
            let value: Value = row.get(1);
            result.push(Resource { id: Some(id), resource_pool_id, value });
        }
        debug!("Found {} resources of pool {}", result.len(), resource_pool_id);
        Ok(result)
    }

    pub fn allocate_resources(&mut self, pool: ResourcePool, wasmer_env: &mut WasmerEnv,
                              user_input: Value) -> Result<(ResourcePool, Vec<Resource>)> {
        // get script
        let script = self.get_allocation_script(pool.allocation_strategy_id)?;

        let current_resources = self.get_resources(pool.id)?.iter()
            .map(|it| it.as_json())
            .collect::<Vec<Value>>();
        let resource_pool = pool.as_json();
        let resource_pool_properties = pool.get_pool_properties();
        let execution_result = wasmer_env.invoke_and_parse(
            &script, user_input, resource_pool_properties,
            resource_pool, current_resources, "invoke()")?;

        // save to DB
        let resources = execution_result.into_iter()
            .map(|value| Resource::new_from_value(pool.id, value))
            .collect::<Vec<Resource>>();
        let (pool, resources) = self.insert_resources(pool, resources)?;
        Ok((pool, resources))
    }
}

fn main() -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Once;

    use rand::Rng;
    use rand::distributions::{Alphanumeric};
    use serde_json::json;
    use std::str::FromStr;
    use std::thread;

    use super::*;

    static START: Once = Once::new();
    const IPV4_ALLOCATION_STRATEGY_ID: i32 = 1;

    fn initialize_logging() {
        START.call_once(|| {
            let fmt_event = tracing_subscriber::fmt::format::Format::default()
                .with_target(false)
                .with_thread_names(true);
            tracing_subscriber::fmt()
                .event_format(fmt_event)
                .with_env_filter(EnvFilter::from_default_env())
                .init();
        });
    }

    fn create_some_ips<T>(start_idx: T, count: T, wrap_in_properties: bool) -> Vec<Value>
        where T: num_traits::identities::One + num_traits::int::PrimInt + std::fmt::Display {

        let mut result = Vec::new();
        let mut idx: T = start_idx;
        while idx != start_idx + count {
            let value = json!({"address": &format!("10.0.0.{}", idx)});
            let value = if wrap_in_properties {
                json!({"Properties": value})
            } else {
                value
            };
            result.push(value);
            idx = idx + T::one();
        }
        result
    }

    #[test]
    fn wasmer_invoke_js() -> Result<()> {
        initialize_logging();

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
        return [{mykey:1, userInput, resourcePoolProperties, resourcePool, currentResources}]\
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
        let expected = json!([{
            "mykey": 1,
            "userInput": {"input":"input"},
            "resourcePoolProperties": {"rpp":"rpp"},
            "resourcePool": {"rp":"rp"},
            "currentResources": ["res1", "res2"]
        }]).as_array().ok_or(anyhow!("Unreachable"))?.to_owned();
        assert_eq!(expected, actual);
        Ok(())
    }

    #[test]
    fn db_get_ipv4_script() -> Result<()> {
        initialize_logging();

        let mut db = DB::new_from_env()?;
        let sw = Stopwatch::start_new();
        let script = db.get_allocation_script(IPV4_ALLOCATION_STRATEGY_ID)?;
        debug!("Found row in {}ms", sw.elapsed_ms());
        trace!("found script: {}", script);
        Ok(())
    }

    #[test]
    fn execute_ipv4_script_with_mocked_data() -> Result<()> {
        initialize_logging();

        let mut db = DB::new_from_env()?;
        let script = db.get_allocation_script(IPV4_ALLOCATION_STRATEGY_ID)?;

        let mut wasmer_env = WasmerEnv::new()?;

        let user_input = json!({
            "resourceCount": 2
        });
        let resource_pool_properties = json!({
            "address": "10.0.0.0",
            "prefix": 24,
        });
        let resource_pool = json!({});
        let current_resources = create_some_ips(1, 2, true); // 10.0.0.1, 10.0.0.2

        let actual = wasmer_env.invoke_and_parse(&script, user_input.clone(), resource_pool_properties.clone(),
                                                 resource_pool.clone(), current_resources, "invoke()")?;
        let expected = json!([
            {"address":"10.0.0.0"},
            {"address":"10.0.0.3"}
        ]).as_array().ok_or(anyhow!("Unreachable"))?.to_owned();
        assert_eq!(expected, actual);

        let current_resources = create_some_ips(0, 4, true); // 10.0.0.0 - 10.0.0.3

        let actual = wasmer_env.invoke_and_parse(&script, user_input, resource_pool_properties,
                                                 resource_pool, current_resources, "invoke()")?;
        let expected = json!([
            {"address":"10.0.0.4"},
            {"address":"10.0.0.5"}
        ]).as_array().ok_or(anyhow!("Unreachable"))?.to_owned();
        assert_eq!(expected, actual);

        Ok(())
    }

    fn create_random_pool(db: &mut DB) -> Result<ResourcePool> {
        let random_string: String = rand::thread_rng().sample_iter(&Alphanumeric).take(10).collect();
        // check that it does not exist
        assert!(db.get_resource_pool_by_name(&random_string).is_err());
        db.insert_resource_pool(&random_string, IPV4_ALLOCATION_STRATEGY_ID)
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
            Resource::new_from_value(resource_pool_id, json!({"address": "1.1.1.1"})),
            Resource::new_from_value(resource_pool_id, json!({"address": "1.1.1.1"})),
        );

        db.insert_resources(pool, resources).expect_err("Should not accept duplicates");
        Ok(())
    }

    #[test]
    fn db_resources() -> Result<()> {
        initialize_logging();

        const ROW_COUNT: usize = 100;
        let mut db = DB::new_from_env()?;
        let pool = create_random_pool(&mut db)?;

        let resource_pool_id = pool.id;
        let old_version = pool.version;
        let sw = Stopwatch::start_new();
        let mut resources = Vec::with_capacity(ROW_COUNT);
        for idx in 0..ROW_COUNT {
            resources.push(Resource::new_from_str(resource_pool_id,
                                                  &format!("{{\"address\":\"1.1.1.{}\"}}", idx))?);
        }
        let (pool, resources) = db.insert_resources(pool, resources)?;
        debug!("inserted {} rows in {}ms", resources.len(), sw.elapsed_ms());
        // check that version is incremented
        assert_eq!(pool.version, old_version + 1);
        assert_eq!(db.get_resource_pool_by_id(resource_pool_id)?.version, old_version + 1);
        // get resources
        let found_resources = db.get_resources(resource_pool_id)?;
        assert_eq!(ROW_COUNT, found_resources.len());
        // IDs are added, just compare values
        assert_eq!(resources.iter().map(|it| &it.value).collect::<Vec<&Value>>(),
                   found_resources.iter().map(|it| &it.value).collect::<Vec<&Value>>());
        Ok(())
    }

    // Get env.var value. If present, panic on parsing error.
    fn get_env_value<F: FromStr>(key: &str, default_value: F) -> F
        where <F as FromStr>::Err: std::fmt::Debug {
        env::var(key)
            .map(|c| c.parse().unwrap())
            .unwrap_or(default_value)
    }

    #[test]
    fn execute_ipv4_script_with_db() -> Result<()> {
        initialize_logging();

        let sw = Stopwatch::start_new();
        let row_count = get_env_value("ROW_COUNT", 100);
        let iterations = get_env_value("ITERATIONS", 2);

        let mut db = DB::new_from_env()?;
        let mut pool = create_random_pool(&mut db)?;
        let old_version = pool.version;
        let mut wasmer_env = WasmerEnv::new()?;
        let user_input = json!({
            "resourceCount": row_count
        });
        info!("Created pool in {}ms", sw.elapsed_ms());
        for iteration in 1..iterations + 1 {
            info!("Starting iteration {}", iteration);
            let sw = Stopwatch::start_new();
            let (pool2, _resources) = db.allocate_resources(
                pool, &mut wasmer_env, user_input.clone())?;
            pool = pool2;
            // check that version is incremented
            let expected_version = old_version + iteration;
            assert_eq!(pool.version, expected_version);
            assert_eq!(db.get_resource_pool_by_id(pool.id)?.version, expected_version);

            if env::var("VERIFY_RESOURCES").is_ok() {
                if iterations * row_count > 255 {
                    // FIXME
                    panic!("Too many IPs to be created - create_some_ips would overflow. Turn off VERIFY_RESOURCES");
                }
                // get resources, might slow down the performance
                let found_resources = db.get_resources(pool.id)?;
                let mut actual = found_resources.into_iter()
                    .map(|it| it.value.get("address").expect("address must exist")
                        .as_str().expect("address value must be a string").to_owned())
                    .collect::<Vec<String>>();
                actual.sort();
                let mut expected = create_some_ips(0, row_count * iteration, false)
                    .into_iter()
                    .map(|value| value.get("address").expect("address must exist")
                        .as_str().expect("address value must be a string").to_owned())
                    .collect::<Vec<String>>();
                expected.sort();
                assert_eq!(expected, actual);
            }
            info!("Inserted {} resources in {}ms", row_count, sw.elapsed_ms());
        }
        Ok(())
    }

    #[test]
    fn parallel_allocation() -> Result<()> {
        initialize_logging();

        let sw = Stopwatch::start_new();
        let number_of_threads = get_env_value("NUMBER_OF_THREADS", 2);
        let mut join_handles = vec![];
        for _ in 0..number_of_threads {
            join_handles.push(thread::spawn(move || {
                execute_ipv4_script_with_db().unwrap();
            }));
        }
        // join all
        join_handles.into_iter().for_each(|handle| handle.join().unwrap());
        info!("Finished executing {} threads in {}ms", number_of_threads, sw.elapsed_ms());
        Ok(())
    }
}
